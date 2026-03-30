# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Alexandre Licinio
__version__ = '0.6.0'
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
import base64, hashlib as _hl
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    
import json, html as _html
import os, time, sqlite3, cherrypy
import hashlib, secrets, functools
import threading
import random
from collections import deque
from typing import Optional
from mako.lookup import TemplateLookup
from collect.scripts.streamhub import fetch_streamhub
try:
    from .logger import LiveLogger  # cas package
except Exception:
    from logger import LiveLogger    # cas script direct

BASE_DIR = Path(__file__).resolve().parent
ROOT = BASE_DIR.parent  # project root
DB_PATH = ROOT / "mini.db"
APP_META = {"client": None, "status": None}

def _get_client_status():
    """Load client/status from environment variables (no YAML)."""
    try:
        client = (os.getenv('CLIENT_NAME') or '').strip() or None
        APP_META["client"] = client
        APP_META["status"] = status
        return client, status
    except Exception:
        return APP_META.get("client"), APP_META.get("status")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOGGER = LiveLogger(DB_PATH)

# Global poller reference for health/status
POLLER = None

lookup = TemplateLookup(
    directories=[str(BASE_DIR / "view")],
    input_encoding="utf-8",
    output_encoding="utf-8",
    encoding_errors="replace",
)

# --- Background poller for StreamHub data ---

class BackgroundPoller:
    def __init__(self, db_path: Path, interval: float = 2.0):
        self.db_path = Path(db_path)
        self.interval = float(interval)
        self._stop = threading.Event()
        self._thr = None
        self.last_cycle_at = 0
        self.last_error = None
        self.age_history = {}  # host -> deque[(ts, age_sec)]
        self.age_window_sec = 120
        self.slack_seen = {}          # host -> deque of log fingerprints
        self.slack_seen_max = 4000
        self.alert_state = {}         # host -> {owd,bitrate,drops,error,active_sessions}
        self._slack_initialized = set()

    # ── Slack helpers ─────────────────────────────────────────────────────

    def _load_slack_cfg(self):
        try:
            with connect_db() as c:
                r = c.execute('SELECT webhook_url,channel,username FROM slack_config WHERE id=1').fetchone()
            if r and r[0] and r[0].strip():
                return r[0].strip(), (r[1] or '#streampilot'), (r[2] or 'StreamPilot')
        except Exception: pass
        return None

    def _load_device_slack(self, device_id):
        _COLS = 'device_id,notify_session,notify_drops,notify_owd_threshold,'\
                'notify_bitrate_min,notify_poller_error,notify_logs,notify_connection,notify_paused,ignore_contains'
        try:
            with connect_db() as c:
                c.execute('INSERT OR IGNORE INTO device_slack(device_id) VALUES(?)', (device_id,))
                r = c.execute(f'SELECT {_COLS} FROM device_slack WHERE device_id=?', (device_id,)).fetchone()
            if r:
                return dict(zip(_COLS.split(','), r))
        except Exception: pass
        return {}

    @staticmethod
    def _slack_post(webhook, text, channel, username, color=None, title=None):
        import json as _j, urllib.request, urllib.parse
        payload = {'channel': channel, 'username': username, 'text': ''}
        if color or title:
            att = {'color': color or '#439FE0', 'text': text, 'mrkdwn_in': []}
            if title: att['title'] = title
            payload['attachments'] = [att]
        else:
            payload['text'] = text
        body = urllib.parse.urlencode({'payload': _j.dumps(payload, ensure_ascii=False)}).encode('utf-8')
        req = urllib.request.Request(webhook, data=body,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=6) as r: r.read()
        except Exception as e:
            cherrypy.log(f'[slack] post error: {e}')

    def _notify_new_logs(self, host, device_id, dev_name, log_entries, cfg, ds):
        import json as _j, hashlib as _hl, re as _re
        if ds.get('notify_paused'): return
        webhook, channel, username = cfg
        ignore_list = []
        try: ignore_list = [s.lower() for s in _j.loads(ds.get('ignore_contains') or '[]') if s.strip()]
        except Exception: pass
        dq = self.slack_seen.setdefault(host, deque(maxlen=self.slack_seen_max))
        seen_set = set(dq)
        if host not in self._slack_initialized:
            for e in log_entries:
                fp = e.get('fp') or _hl.sha1((e.get('raw') or '').encode()).hexdigest()
                dq.append(fp)
            self._slack_initialized.add(host)
            return
        _PAT_CONN  = _re.compile(r'source\s*#(\d+):\s*connection of ([^(]+)', _re.IGNORECASE)
        _PAT_DCONN = _re.compile(r'source\s*#(\d+):\s*disconnection of (\S+)', _re.IGNORECASE)
        for e in log_entries:
            fp = e.get('fp') or _hl.sha1((e.get('raw') or '').encode()).hexdigest()
            if fp in seen_set: continue
            raw_line = e.get('raw') or f"{e.get('ts','')} {e.get('message','')}"
            msg = e.get('message') or ''
            if ignore_list and any(s in raw_line.lower() for s in ignore_list):
                dq.append(fp); seen_set.add(fp); continue
            # Specific connection/disconnection notifications
            mc = _PAT_CONN.search(msg)
            md = _PAT_DCONN.search(msg)
            if mc and ds.get('notify_connection', 1) and not ds.get('notify_paused'):
                src_n, tx_name = mc.group(1), mc.group(2).strip()
                self._slack_post(webhook,
                    f':satellite_antenna: *{dev_name}* — Source #{src_n} `{tx_name}` connected',
                    channel, username, color='#439FE0')
                dq.append(fp); seen_set.add(fp); continue
            if md and ds.get('notify_connection', 1) and not ds.get('notify_paused'):
                src_n, tx_name = md.group(1), md.group(2).strip()
                self._slack_post(webhook,
                    f':zzz: *{dev_name}* — Source #{src_n} `{tx_name}` disconnected',
                    channel, username, color='#9B9B9B')
                dq.append(fp); seen_set.add(fp); continue
            # Generic log forwarding
            if not ds.get('notify_logs'): dq.append(fp); seen_set.add(fp); continue
            level = (e.get('level') or 'INFO').upper()
            color_map = {'ERROR':'danger','WARN':'warning','WARNING':'warning','INFO':'#439FE0','DEBUG':'#9B9B9B'}
            slack_line = _re.sub(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+', '', raw_line)
            self._slack_post(webhook, slack_line, channel, username,
                             color=color_map.get(level, '#439FE0'), title=dev_name)
            dq.append(fp); seen_set.add(fp)

    def _notify_alerts(self, host, device_id, dev_name, payload, cfg, ds):
        if ds.get('notify_paused'): return
        webhook, channel, username = cfg
        state = self.alert_state.setdefault(host, {
            'owd': False, 'bitrate': False, 'drops': False, 'error': False, 'active_sessions': 0
        })
        inputs = (payload or {}).get('inputs', {})
        active = [v for k, v in inputs.items()
                  if str(k).isdigit() and (v or {}).get('status') == 'on'
                  and (v or {}).get('protocol', '').lower() == 'sst']
        if ds.get('notify_session'):
            prev, cur = state['active_sessions'], len(active)
            if cur > prev:
                for v in active:
                    nm = v.get('identifier') or v.get('source_name') or 'unknown'
                    self._slack_post(webhook, f'*{dev_name}* — Session started: `{nm}`',
                                     channel, username, color='good')
            elif cur < prev:
                _prev_names = state.get('last_active_names', '')
                _stopped_msg = f':red_circle: *{dev_name}* — Session stopped ({prev} -> {cur} active)'
                if _prev_names: _stopped_msg += f': {_prev_names}'
                self._slack_post(webhook, _stopped_msg, channel, username, color='danger')
            state['active_sessions'] = cur
            state['last_active_names'] = ', '.join(
                f"`{v.get('identifier') or v.get('source_name') or 'unknown'}`"
                for v in active
            )
        if not active: return
        # Build a label listing all active transmitter identifiers
        active_names = ', '.join(
            f"`{v.get('identifier') or v.get('source_name') or 'unknown'}`"
            for v in active
        )
        max_owd, total_rb, total_dv, total_dt = 0, 0, 0, 0
        worst_input_owd = 'unknown'
        worst_input_rb  = 'unknown'
        for v in active:
            nm = v.get('identifier') or v.get('source_name') or 'unknown'
            L = v.get('links') or {}
            rb = int(L.get('total_rx_bitrate_from_links') or 0)
            total_rb += rb
            if rb < (int(ds.get('notify_bitrate_min') or 0) or 999999):
                worst_input_rb = nm
            for lk, ls in L.items():
                if str(lk).isdigit():
                    owd = (ls or {}).get('owdR')
                    if isinstance(owd, (int, float)) and int(owd) > max_owd:
                        max_owd = int(owd)
                        worst_input_owd = nm
            notif = (v.get('notifications') or {}).get('dropped') or {}
            total_dv += int(notif.get('video') or 0)
            total_dt += int(notif.get('ts') or 0)
        owd_thr = int(ds.get('notify_owd_threshold') or 0)
        if owd_thr > 0:
            bad = max_owd >= owd_thr
            if bad and not state['owd']:
                self._slack_post(webhook,
                    f':warning: *{dev_name}* — `{worst_input_owd}` OWD high: *{max_owd} ms* (thr {owd_thr} ms)',
                    channel, username, color='warning'); state['owd'] = True
            elif not bad and state['owd']:
                self._slack_post(webhook,
                    f':white_check_mark: *{dev_name}* — `{worst_input_owd}` OWD normal: {max_owd} ms',
                    channel, username, color='good'); state['owd'] = False
        rb_min = int(ds.get('notify_bitrate_min') or 0)
        if rb_min > 0:
            bad = total_rb < rb_min
            if bad and not state['bitrate']:
                self._slack_post(webhook,
                    f':warning: *{dev_name}* — `{worst_input_rb}` Bitrate low: *{total_rb} kb/s* (min {rb_min})',
                    channel, username, color='warning'); state['bitrate'] = True
            elif not bad and state['bitrate']:
                self._slack_post(webhook,
                    f':white_check_mark: *{dev_name}* — {active_names} Bitrate OK: {total_rb} kb/s',
                    channel, username, color='good'); state['bitrate'] = False
        if ds.get('notify_drops'):
            bad = total_dv > 0 or total_dt > 0
            if bad and not state['drops']:
                self._slack_post(webhook,
                    f':rotating_light: *{dev_name}* — {active_names} Drops: video={total_dv} ts={total_dt}',
                    channel, username, color='danger'); state['drops'] = True
            elif not bad and state['drops']:
                self._slack_post(webhook,
                    f':white_check_mark: *{dev_name}* — {active_names} No more drops',
                    channel, username, color='good'); state['drops'] = False

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._loop, name="StreamPilotBackgroundPoller", daemon=True)
        self._thr.start()

    def stop(self):
        try:
            self._stop.set()
            if self._thr:
                self._thr.join(timeout=2.0)
        except Exception:
            pass

    def _loop(self):
        while not self._stop.is_set():
            self.last_cycle_at = time.time()
            self.last_error = None
            loop_start = time.time()
            try:
                # Read configured devices and poll each one
                with connect_db() as conn:
                    cur = conn.execute("SELECT id,name,protocol,host,port,api_path,token FROM devices")
                    rows = cur.fetchall()

                for row in rows:
                    try:
                        did, name, protocol, host, port, api_path, token = row
                        # Normalize default ports for StreamHub API
                        p = int(port or 0)
                        if protocol == 'http' and (p in (0, 80, 443)):
                            p = 8893
                        if protocol == 'https' and (p in (0, 80, 443)):
                            p = 8896
                        base = f"{protocol}://{host}:{p}"
                        ok, payload = fetch_streamhub(base, token or None, timeout=5)
                        # Always observe, even if not ok (logger can decide)
                        try:
                            device_id = (payload.get('characteristics') or {}).get('identifier') \
                                or (payload.get('configuration') or {}).get('device', {}).get('Identifier') \
                                or str(did)
                            device_host = host
                            LOGGER.observe_payload(device_id, device_host, payload)
                            # Persist input→output name mapping
                            try:
                                _persist_output_map(host, payload)
                            except Exception as _ome:
                                cherrypy.log(f'[output_map] persist error: {_ome}')
                            # Fetch and persist StreamHub logs (plain text /logs endpoint)
                            try:
                                from collect.scripts.streamhub import fetch_logs_structured
                                ok_logs, log_entries = fetch_logs_structured(base, token or None, timeout=5)
                                if ok_logs and log_entries:
                                    _persist_logs(host, log_entries)
                                    try:
                                        _sc = self._load_slack_cfg()
                                        if _sc:
                                            _ds = self._load_device_slack(did)
                                            self._notify_new_logs(host, did, name or host, log_entries, _sc, _ds)
                                    except Exception as _sle:
                                        cherrypy.log(f'[slack] log notify: {_sle}')
                            except Exception as _log_err:
                                cherrypy.log(f"[poller] logs persist error: {_log_err}")
                            # Slack threshold alerts
                            try:
                                _sc2 = self._load_slack_cfg()
                                if _sc2 and ok:
                                    _ds2 = self._load_device_slack(did)
                                    self._notify_alerts(host, did, name or host, payload, _sc2, _ds2)
                            except Exception as _sae:
                                cherrypy.log(f'[slack] alert: {_sae}')
                        except Exception as obs_err:
                            cherrypy.log(f"[poller] observe_payload error: {obs_err}")
                            # Slack poller error
                            try:
                                _sc3 = self._load_slack_cfg()
                                if _sc3:
                                    _ds3 = self._load_device_slack(did)
                                    if _ds3.get('notify_poller_error') and not _ds3.get('notify_paused'):
                                        _st = self.alert_state.setdefault(host, {})
                                        if not _st.get('error'):
                                            self._slack_post(_sc3[0], f':x: *{name or host}* StreamHub unreachable', _sc3[1], _sc3[2], color='danger')
                                            _st['error'] = True
                            except Exception: pass
                        # Update age history for this host (based on last sample ts in DB)
                        try:
                            with connect_db() as db2:
                                last_ts = db2.execute(
                                    "SELECT MAX(ts) FROM live_sample WHERE session_id IN (SELECT id FROM live_session WHERE device_host=?)",
                                    (host,)
                                ).fetchone()[0]
                            age_sec = None
                            if last_ts:
                                from datetime import datetime
                                try:
                                    _ts_str = str(last_ts)[:19].replace('T', ' ')
                                    dt = datetime.strptime(_ts_str, '%Y-%m-%d %H:%M:%S')
                                    age_sec = max(0, int((datetime.utcnow() - dt).total_seconds()))
                                except Exception:
                                    age_sec = None
                            dq = self.age_history.get(host)
                            if dq is None:
                                dq = deque(maxlen=int(self.age_window_sec / max(self.interval, 1e-3)) + 5)
                                self.age_history[host] = dq
                            dq.append((time.time(), age_sec))
                        except Exception as _ah_err:
                            cherrypy.log(f"[poller] age_history update error: {_ah_err}")
                    except Exception as dev_err:
                        cherrypy.log(f"[poller] device poll error: {dev_err}")
                    # Jitter per device to avoid synchronized bursts under load
                    time.sleep(min(0.02, self.interval * 0.1) * random.random())

            except Exception as e:
                self.last_error = str(e)
                cherrypy.log(f"[poller] loop error: {e}")
            finally:
                # Sleep the remaining time of the interval (wall-clock), or a minimal breather if overrun
                sleep_left = self.interval - (time.time() - loop_start)
                if sleep_left > 0:
                    time.sleep(sleep_left)
                else:
                    time.sleep(0.05)


# --- One-time DB indexes (idempotent) ---
def _ensure_db_indexes(db):
    try:
        # Speeds up: MAX(ts) per host via session subquery, and session scans by time
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_sample_sid_ts ON live_sample(session_id, ts)")
    except Exception: pass
    try:
        # Speeds up: counts of active sessions per host, and lookups by host
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_session_host_ended ON live_session(device_host, ended_at)")
    except Exception: pass
    try:
        # Helpful when queries still ORDER BY id for a given session
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_sample_sid_id ON live_sample(session_id, id)")
    except Exception: pass

def _get_linked_output_names(device_host, input_index):
    """Return set of lowercase output names linked to this input,
    queried from the persisted device_output_map table.
    input_index is 1-indexed (as stored in DB and Source #N logs).
    """
    try:
        with connect_db() as c:
            rows = c.execute(
                'SELECT output_name FROM device_output_map '
                'WHERE device_host=? AND input_index=?',
                (device_host, int(input_index))
            ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _filter_logs_for_input(rows, input_index, input_identifier, device_id=None):
    """Filter StreamHub log rows for a specific session input.
    Includes:
    - Source #N / input N logs matching this input (1-indexed)
    - IP output logs whose output name is linked to this input,
      AND whose message does not explicitly reference a different input number
    - Logs mentioning the input_identifier
    Excludes:
    - Config change (global noise)
    - Source #N / input N logs belonging to other inputs
    - IP output logs that explicitly say (input N) where N != this input,
      OR that say (input AUTO) — AUTO outputs are not input-specific
    """
    import re as _re2
    _PAT_SRC = _re2.compile(
        r'(?:source\s*#|\bfor\s+input\s+|\bstopping live for input\s+|'
        r'changing video capped bitrate for input\s+)(\d+)',
        _re2.IGNORECASE
    )
    _PAT_IPOUT = _re2.compile(
        r"(?:ip output|sdi output|ndi output)[\s'\"]+([\w\-\.]+)",
        _re2.IGNORECASE
    )
    # Extracts the (input N) or (input AUTO) annotation from IP output messages
    _PAT_INPUT_TAG = _re2.compile(r'\(input\s+(\d+|AUTO)\)', _re2.IGNORECASE)
    _EXCLUDE = _re2.compile(
        r'^config change',
        _re2.IGNORECASE
    )
    # input_index in DB is 1-indexed — matches Source #N directly
    idx_str = str(int(input_index)) if input_index is not None else None
    ident_lower = (input_identifier or '').strip().lower()
    linked_outputs = _get_linked_output_names(device_id, input_index) if device_id else set()
    result = []
    for row in rows:
        msg = (row[-1] or '')
        msg_lower = msg.lower()
        # Exclude Config change
        if _EXCLUDE.search(msg):
            continue
        # Source #N or input N reference
        m_src = _PAT_SRC.search(msg)
        if m_src:
            if idx_str and m_src.group(1) == idx_str:
                result.append(row)
            # else: belongs to another input, skip
            continue
        # IP output log
        m_ip = _PAT_IPOUT.search(msg)
        if m_ip:
            out_name = m_ip.group(1).lower()
            # Normalise SDI log: "SDI output 1" captured as "1" → "sdi-1"
            if msg_lower.startswith('sdi output') and out_name.isdigit():
                out_name = f'sdi-{out_name}'
            # Check if message explicitly tags an input number or AUTO
            m_tag = _PAT_INPUT_TAG.search(msg)
            if m_tag:
                tag_val = m_tag.group(1).upper()
                if tag_val == 'AUTO':
                    # AUTO outputs follow any active input — skip entirely,
                    # they are not specific to this session's input
                    continue
                elif idx_str and tag_val != idx_str:
                    # Explicitly belongs to a different input
                    continue
                else:
                    # Tag matches this input — include regardless of linked_outputs
                    result.append(row)
                    continue
            # No explicit tag — fall back to linked_outputs mapping
            if linked_outputs and out_name in linked_outputs:
                result.append(row)
            # else: output not linked to this input, skip
            continue
        # Identifier match (e.g. transmitter name appears in message)
        if ident_lower and ident_lower in msg_lower:
            result.append(row)
    return result


def _persist_output_map(host: str, payload: dict):
    """Persist the input→output name mapping from the current payload.
    Uses INSERT OR REPLACE so historical entries are preserved for past session filtering.
    ip_outputs keys are 0-indexed in payload; input_index in DB is 1-indexed.
    Only persists outputs that are currently active (status='on').
    """
    if not payload or not host:
        return
    try:
        inputs = payload.get('inputs') or {}
        rows = []
        for key, inp in inputs.items():
            if not str(key).isdigit():
                continue
            # payload key is 0-indexed; store as 1-indexed to match Source #N
            input_idx_1 = int(key) + 1
            # IP outputs — only active ones (status='on')
            ip_outputs = (inp or {}).get('ip_outputs') or {}
            for out in ip_outputs.values():
                name = (out or {}).get('name')
                if name and (out or {}).get('status') == 'on':
                    rows.append((host, input_idx_1, str(name).lower()))
            # SDI (physical) outputs
            sdi_outputs = (inp or {}).get('sdi_outputs') or {}
            for sdi_key in sdi_outputs:
                rows.append((host, input_idx_1, f'sdi-{sdi_key}'))
            # NDI outputs
            ndi_outputs = (inp or {}).get('ndi_outputs') or {}
            for out in ndi_outputs.values():
                name = (out or {}).get('name')
                if name:
                    rows.append((host, input_idx_1, str(name).lower()))
        if rows:
            with connect_db() as c:
                c.executemany(
                    'INSERT OR REPLACE INTO device_output_map(device_host, input_index, output_name) '
                    'VALUES(?,?,?)',
                    rows
                )
    except Exception as _e:
        try:
            import cherrypy; cherrypy.log(f'[output_map] {_e}')
        except Exception:
            pass


def _init_db():
    """Run once at startup — creates tables, indexes, runs migrations."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, isolation_level=None, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")
    # Schéma minimal (tel que déjà en place chez toi)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS devices ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " name TEXT, protocol TEXT, host TEXT, port INTEGER, api_path TEXT, token TEXT)"
    )
    _ensure_db_indexes(conn)
    
    # Table logs StreamHub
    conn.execute("""
        CREATE TABLE IF NOT EXISTS streamhub_log (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            ts      TEXT,
            node    TEXT,
            level   TEXT,
            message TEXT,
            raw     TEXT,
            fp      TEXT
        )
    """)
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_shlog_fp ON streamhub_log(fp)")
    except Exception: pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shlog_host_ts ON streamhub_log(device_host, ts)")
    except Exception: pass

    # Input → Output mapping (persisted every poll cycle)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS device_output_map (
            device_host  TEXT NOT NULL,
            input_index  INTEGER NOT NULL,
            output_name  TEXT NOT NULL,
            PRIMARY KEY (device_host, output_name)
        )""")

    # SRT Gateway tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_gateway_device (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT,
            host        TEXT UNIQUE,
            port        INTEGER DEFAULT 443,
            protocol    TEXT DEFAULT 'https',
            username    TEXT DEFAULT 'haiadmin',
            password    TEXT DEFAULT 'haiadmin',
            gw_device_id TEXT,
            session_id  TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_route_sample (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT NOT NULL,
            route_name  TEXT,
            ts          TEXT,
            state       TEXT,
            total_bitrate_mbps REAL,
            src_rtt_ms  REAL,
            src_loss_pct REAL,
            src_retransmit_bps REAL,
            active_connections INTEGER
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_connection_sample (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT NOT NULL,
            ts          TEXT,
            direction   TEXT,
            address     TEXT,
            port        INTEGER,
            bitrate_mbps REAL,
            rtt_ms      REAL,
            loss_pct    REAL,
            retransmit_bps REAL,
            buffer_ms   REAL,
            state       TEXT,
            srt_version TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_session (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT NOT NULL,
            route_name  TEXT,
            started_at  TEXT,
            ended_at    TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_endpoint_location (
            address     TEXT PRIMARY KEY,
            label       TEXT,
            lat         REAL,
            lng         REAL
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_event (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT,
            route_name  TEXT,
            ts          TEXT,
            event_type  TEXT,
            detail      TEXT,
            ip_address  TEXT
        )""")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_srt_route_sample ON srt_route_sample(device_host, route_id, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_srt_conn_sample ON srt_connection_sample(device_host, route_id, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_srt_event ON srt_event(device_host, ts)")
    except Exception: pass
    try:
        _ec2 = {r[1] for r in conn.execute("PRAGMA table_info(srt_event)").fetchall()}
        if "ip_address" not in _ec2:
            conn.execute("ALTER TABLE srt_event ADD COLUMN ip_address TEXT")
    except Exception: pass

    # Slack global config
    conn.execute("""
        CREATE TABLE IF NOT EXISTS slack_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            webhook_url TEXT DEFAULT '',
            channel TEXT DEFAULT '#streampilot',
            username TEXT DEFAULT 'StreamPilot'
        )""")
    conn.execute("INSERT OR IGNORE INTO slack_config(id) VALUES(1)")

    # Per-device Slack settings
    _DI = ('["StreamHub user admin","StreamHub is disconnected from Aviwest Hub service",'
           '"StreamHub is connected to Aviwest Hub service","read ECONNRESET",'
           '"Nodejs is restarting...","502 Server Error","HTTPSConnectionPool"]')
    conn.execute(
        "CREATE TABLE IF NOT EXISTS device_slack ("
        "device_id INTEGER PRIMARY KEY,"
        "notify_session INTEGER DEFAULT 1,"
        "notify_drops INTEGER DEFAULT 1,"
        "notify_owd_threshold INTEGER DEFAULT 0,"
        "notify_bitrate_min INTEGER DEFAULT 0,"
        "notify_poller_error INTEGER DEFAULT 1,"
        "notify_logs INTEGER DEFAULT 1,"
        "notify_connection INTEGER DEFAULT 1,"
        "notify_paused INTEGER DEFAULT 0,"
        "ignore_contains TEXT DEFAULT '" + _DI + "'"
        ")"
    )
    # Migration for existing DBs
    try:
        _ec = {r[1] for r in conn.execute("PRAGMA table_info(device_slack)").fetchall()}
        if "notify_paused" not in _ec:
            conn.execute("ALTER TABLE device_slack ADD COLUMN notify_paused INTEGER DEFAULT 0")
        if "notify_connection" not in _ec:
            conn.execute("ALTER TABLE device_slack ADD COLUMN notify_connection INTEGER DEFAULT 1")
    except Exception: pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_gw_slack (
            gw_id            INTEGER PRIMARY KEY,
            notify_source    INTEGER DEFAULT 1,
            notify_client    INTEGER DEFAULT 1,
            notify_paused    INTEGER DEFAULT 0,
            thr_bitrate_min  REAL    DEFAULT 0,
            thr_loss_max     REAL    DEFAULT 0,
            thr_lost_max     INTEGER DEFAULT 0,
            thr_dropped_max  INTEGER DEFAULT 0,
            thr_retransmit_max REAL  DEFAULT 0
        )""")
    try:
        _gwc = {r[1] for r in conn.execute("PRAGMA table_info(srt_gateway_device)").fetchall()}
        if "session_id" not in _gwc:
            conn.execute("ALTER TABLE srt_gateway_device ADD COLUMN session_id TEXT")
    except Exception: pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_report_session (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT NOT NULL,
            route_name  TEXT,
            name        TEXT,
            started_at  TEXT NOT NULL,
            ended_at    TEXT NOT NULL,
            created_at  TEXT
        )""")
    conn.commit()
    conn.close()


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, isolation_level=None, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=2000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")
    # Ensure critical tables always exist (idempotent, fast after first run)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS devices ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " name TEXT, protocol TEXT, host TEXT, port INTEGER, api_path TEXT, token TEXT)"
    )
    conn.execute("""
        CREATE TABLE IF NOT EXISTS device_output_map (
            device_host  TEXT NOT NULL,
            input_index  INTEGER NOT NULL,
            output_name  TEXT NOT NULL,
            PRIMARY KEY (device_host, output_name)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS srt_report_session (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            route_id    TEXT NOT NULL,
            route_name  TEXT,
            name        TEXT,
            started_at  TEXT NOT NULL,
            ended_at    TEXT NOT NULL,
            created_at  TEXT,
            tz_offset   INTEGER DEFAULT 0
        )""")
    # Migration: add tz_offset if missing (existing DBs)
    try:
        conn.execute('ALTER TABLE srt_report_session ADD COLUMN tz_offset INTEGER DEFAULT 0')
    except Exception: pass
    try:
        conn.execute('ALTER TABLE srt_connection_sample ADD COLUMN srt_version TEXT')
    except Exception: pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS streamhub_log (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            device_host TEXT NOT NULL,
            ts      TEXT,
            node    TEXT,
            level   TEXT,
            message TEXT,
            raw     TEXT,
            fp      TEXT
        )""")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS device_slack ("
        "device_id INTEGER PRIMARY KEY,"
        "notify_session INTEGER DEFAULT 1,"
        "notify_drops INTEGER DEFAULT 1,"
        "notify_owd_threshold INTEGER DEFAULT 0,"
        "notify_bitrate_min INTEGER DEFAULT 0,"
        "notify_poller_error INTEGER DEFAULT 1,"
        "notify_logs INTEGER DEFAULT 1,"
        "notify_connection INTEGER DEFAULT 1,"
        "notify_paused INTEGER DEFAULT 0,"
        "ignore_contains TEXT DEFAULT '[]')"
    )
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_shlog_fp ON streamhub_log(fp)")
    except Exception: pass
    _ensure_db_indexes(conn)
    return conn

def _persist_logs(host: str, entries: list):
    if not entries:
        return
    with connect_db() as c:
        for e in entries:
            fp = e.get("fp")
            if not fp:
                continue
            try:
                c.execute(
                    "INSERT OR IGNORE INTO streamhub_log"
                    "(device_host, ts, node, level, message, raw, fp) VALUES(?,?,?,?,?,?,?)",
                    (host, e.get("ts"), e.get("node"), e.get("level"), e.get("message"), e.get("raw"), fp)
                )
            except Exception:
                pass

def render(tpl, **ctx):
    client, status = _get_client_status()
    ctx.setdefault('client', client)
    ctx.setdefault('status', status)
    # Provide MAX_STREAMHUB (default 1) and a device_count to all templates
    limit_env = os.getenv('MAX_STREAMHUB')
    try:
        limit = int(limit_env) if (limit_env is not None and str(limit_env).strip()) else 1
    except Exception:
        limit = 1
    if limit < 1:
        limit = 1
    ctx.setdefault('max_streamhub', limit)
    if 'devices' in ctx and 'device_count' not in ctx:
        try:
            ctx['device_count'] = len(ctx.get('devices') or [])
        except Exception:
            ctx['device_count'] = 0
    return lookup.get_template(tpl).render(**ctx)



def _gw_page(title: str, body_content: str, user=None) -> bytes:
    """Minimal HTML wrapper for SRT Gateway pages (no Mako dependency)."""
    u = (user or {}).get('username', '')
    return (
        "<!doctype html><html><head>"
        "<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css' rel='stylesheet'>"
        "<title>" + title + " — StreamPilot</title>"
        "</head><body>"
        "<nav class='navbar navbar-expand-lg bg-body-tertiary border-bottom mb-3'>"
        "<div class='container-fluid'>"
        "<a class='navbar-brand fw-bold' href='/'>StreamPilot</a>"
        "<div class='collapse navbar-collapse'>"
        "<ul class='navbar-nav me-auto'>"
        "<li class='nav-item'><a class='nav-link' href='/'>Dashboard</a></li>"
        "<li class='nav-item'><a class='nav-link' href='/devices'>Devices</a></li>"
        "<li class='nav-item'><a class='nav-link' href='/health'>Health</a></li>"
        "<li class='nav-item'><a class='nav-link' href='/settings'>Settings</a></li>"
        "<li class='nav-item'><a class='nav-link active' href='/gateway_dashboard'>SRT Gateway</a></li>"
        "</ul>"
        "<div class='ms-auto d-flex align-items-center gap-2'>"
        + ("<span class='badge text-bg-secondary'>" + u + "</span>" if u else "")
        + "<a class='btn btn-outline-secondary btn-sm' href='/logout'>Logout</a>"
        "</div></div></div></nav>"
        + body_content
        + "<script src='https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js'></script>"
        "</body></html>"
    ).encode('utf-8')


def _license_is_valid():
    return True

def _license_badge_html():
    return ''

def _license_expired_html():
    return b''

def _get_credentials():
    """Return (username, password) from env, with defaults."""
    u = (os.getenv('SP_USER') or 'admin').strip()
    p = (os.getenv('SP_PASSWORD') or 'admin').strip()
    return u, p

def current_user():
    try:
        username = cherrypy.session.get('username')
        if username:
            return {'username': username}
    except Exception:
        pass
    return None

def require_login(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        try:
            if not cherrypy.session.get('username'):
                raise cherrypy.HTTPRedirect('/login')
        except cherrypy.HTTPRedirect:
            raise
        except Exception:
            raise cherrypy.HTTPRedirect('/login')
        return fn(*args, **kwargs)
    return _wrap

def slow(th=0.25):  # log > 250 ms
    def deco(fn):
        @functools.wraps(fn)
        def wrap(*a, **k):
            t = time.perf_counter()
            r = fn(*a, **k)
            dt = time.perf_counter() - t
            if dt > th:
                cherrypy.log(f"[slow] {fn.__name__} {dt*1000:.0f}ms {getattr(cherrypy.request,'path_info','')}")
            return r
        return wrap
    return deco



# ══════════════════════════════════════════════════════════════════════════════
# SRT Gateway Poller
# ══════════════════════════════════════════════════════════════════════════════

class SRTGatewayPoller:
    """Background poller for Haivision SRT/Media Gateway devices."""

    def __init__(self, db_path, interval: int = 5):
        self.db_path  = db_path
        self.interval = interval
        self._thr     = None
        self._stop    = False
        self._sessions: dict  = {}   # host -> {route_id -> session_id}
        self._last_states: dict = {} # host -> {route_id -> state}
        self._session_ids: dict = {} # host -> sessionID cookie
        self._gw_device_ids: dict = {}  # host -> gw_device_id
        self._slack_initialized: set = set()
        self.alert_state: dict = {}
        self.last_payloads: dict = {}  # host -> {route_id -> normalized}
        self.age_history: dict = {}    # host -> deque[(ts, age_sec)]

    def start(self):
        import threading
        self._stop = False
        self._thr  = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()

    def stop(self):
        self._stop = True

    def _loop(self):
        _last_purge = 0.0
        while not self._stop:
            t0 = time.time()
            try:
                self._poll_all()
            except Exception as e:
                cherrypy.log(f'[srt_gw] loop error: {e}')
            # Purge old samples once per hour
            if t0 - _last_purge > 3600:
                try:
                    self._purge_old_samples()
                    _last_purge = t0
                except Exception as _pe:
                    cherrypy.log(f'[srt_gw] purge error: {_pe}')
            elapsed = time.time() - t0
            sleep = max(0, self.interval - elapsed)
            time.sleep(sleep)

    def _purge_old_samples(self):
        """Delete srt_route_sample and srt_connection_sample older than SRT_RETENTION_DAYS."""
        try:
            days = int(os.getenv('SRT_RETENTION_DAYS') or 30)
        except Exception:
            days = 30
        from datetime import datetime as _dtp, timedelta as _tdd
        cutoff = (_dtp.utcnow() - _tdd(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        with connect_db() as c:
            c.execute('DELETE FROM srt_route_sample WHERE ts < ?', (cutoff,))
            c.execute('DELETE FROM srt_connection_sample WHERE ts < ?', (cutoff,))
        cherrypy.log(f'[srt_gw] purged samples older than {days} days (cutoff {cutoff})')

    def _poll_all(self):
        with connect_db() as c:
            devices = c.execute(
                'SELECT id, name, host, port, protocol, username, password, gw_device_id '
                'FROM srt_gateway_device'
            ).fetchall()
        for row in devices:
            did, name, host, port, proto, user, pwd, gw_id = row
            base = f'{proto}://{host}:{port}'
            try:
                self._poll_device(did, name, host, base, user, pwd, gw_id)
            except Exception as e:
                cherrypy.log(f'[srt_gw] {host} error: {e}')

    def _get_session(self, host: str, base: str, user: str, pwd: str) -> Optional[str]:
        """Return valid sessionID.
        Priority: 1) in-memory cache, 2) persisted in DB, 3) fresh login.
        Persisting the sessionID across restarts avoids hitting the
        max-concurrent-sessions limit (HTTP 429) on the gateway.
        """
        from collect.scripts.srt_gateway import login, logout
        # 1. In-memory cache
        sid = self._session_ids.get(host)
        if sid:
            return sid
        # 2. Persisted in DB from previous run
        try:
            with connect_db() as c:
                row = c.execute(
                    "SELECT session_id FROM srt_gateway_device WHERE host=?", (host,)
                ).fetchone()
                if row and row[0]:
                    sid = row[0]
                    self._session_ids[host] = sid
                    cherrypy.log(f'[srt_gw] {host} restored session from DB')
                    return sid
        except Exception:
            pass
        # 3. Fresh login
        sid = login(base, user, pwd)
        if sid:
            self._session_ids[host] = sid
            # Persist so next restart reuses this session
            try:
                with connect_db() as c:
                    c.execute(
                        "UPDATE srt_gateway_device SET session_id=? WHERE host=?",
                        (sid, host)
                    )
            except Exception:
                pass
            cherrypy.log(f'[srt_gw] {host} logged in, session persisted')
        return sid

    def _poll_device(self, did, name, host, base, user, pwd, gw_id):
        from collect.scripts.srt_gateway import (
            login, fetch_device_id, fetch_routes, fetch_route_stats, normalize_route
        )
        sid = self._get_session(host, base, user, pwd)
        if not sid:
            # Clear any stale persisted session
            try:
                with connect_db() as c:
                    c.execute("UPDATE srt_gateway_device SET session_id=NULL WHERE host=?", (host,))
            except Exception:
                pass
            cherrypy.log(f'[srt_gw] {host} login failed')
            return

        # Resolve gateway device_id if not cached
        if not gw_id:
            ok, gw_id = fetch_device_id(base, sid)
            if ok and gw_id:
                with connect_db() as c:
                    c.execute('UPDATE srt_gateway_device SET gw_device_id=? WHERE host=?',
                              (gw_id, host))
                self._gw_device_ids[host] = gw_id
            else:
                self._session_ids.pop(host, None)
                return

        ok, routes = fetch_routes(base, sid, gw_id)
        if not ok:
            # Session likely expired — clear from memory and DB, re-login next cycle
            self._session_ids.pop(host, None)
            try:
                with connect_db() as c:
                    c.execute("UPDATE srt_gateway_device SET session_id=NULL WHERE host=?", (host,))
            except Exception:
                pass
            return

        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        normalized = {}
        for route in routes:
            route_id = route.get('id', '')
            ok_s, stats = fetch_route_stats(base, sid, gw_id, route_id)
            payload = normalize_route(route, stats if ok_s else {})
            normalized[route_id] = payload
            self._persist_sample(host, ts, payload)
            self._track_session(host, ts, payload)

        self.last_payloads[host] = normalized
        self._update_age_history(host)

        # Slack alerts
        slack_cfg = self._load_slack_cfg()
        if slack_cfg:
            self._notify_gateway(host, name, normalized, slack_cfg)

    def _persist_sample(self, host: str, ts: str, p: dict):
        src = p.get('source', {})
        with connect_db() as c:
            c.execute(
                'INSERT INTO srt_route_sample '
                '(device_host, route_id, route_name, ts, state, total_bitrate_mbps, '
                'src_rtt_ms, src_loss_pct, src_retransmit_bps, active_connections) '
                'VALUES (?,?,?,?,?,?,?,?,?,?)',
                (host, p['id'], p['name'], ts, p.get('state'),
                 p.get('total_bitrate_mbps'),
                 src.get('rtt_ms'), src.get('loss_pct'),
                 src.get('retransmit_bps'), p.get('active_connections', 0))
            )
            # Per-connection samples
            for conn in src.get('connections', []):
                c.execute(
                    'INSERT INTO srt_connection_sample '
                    '(device_host, route_id, ts, direction, address, port, '
                    'bitrate_mbps, rtt_ms, loss_pct, retransmit_bps, buffer_ms, state, srt_version) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (host, p['id'], ts, 'source',
                     conn.get('address'), conn.get('port'),
                     conn.get('bitrate_mbps'), conn.get('rtt_ms'),
                     conn.get('loss_pct'), conn.get('retransmit_bps'),
                     conn.get('buffer_ms'), conn.get('state'),
                     conn.get('srt_version', ''))
                )
            for dst in p.get('destinations', []):
                for conn in dst.get('connections', []):
                    c.execute(
                        'INSERT INTO srt_connection_sample '
                        '(device_host, route_id, ts, direction, address, port, '
                        'bitrate_mbps, rtt_ms, loss_pct, retransmit_bps, buffer_ms, state, srt_version) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (host, p['id'], ts, f"dst:{dst.get('name','')}",
                         conn.get('address'), conn.get('port'),
                         conn.get('bitrate_mbps'), conn.get('rtt_ms'),
                         conn.get('loss_pct'), None, None, conn.get('state'),
                         conn.get('srt_version', ''))
                    )

    def _update_age_history(self, host: str):
        """Record sample age (seconds since last sample) for sparkline."""
        from collections import deque
        from datetime import datetime as _dth
        try:
            with connect_db() as c:
                r = c.execute(
                    "SELECT MAX(ts) FROM srt_route_sample WHERE device_host=?", (host,)
                ).fetchone()[0]
            if r:
                dt = _dth.strptime(str(r)[:19], "%Y-%m-%d %H:%M:%S")
                age = max(0, int((_dth.utcnow() - dt).total_seconds()))
            else:
                age = None
            if host not in self.age_history:
                self.age_history[host] = deque(maxlen=120)
            self.age_history[host].append((time.time(), age))
        except Exception:
            pass

    def _persist_event(self, host, ts, route_id, route_name, event_type,
                        detail='', ip_address=None):
        try:
            with connect_db() as c:
                c.execute(
                    'INSERT INTO srt_event '
                    '(device_host, route_id, route_name, ts, event_type, detail, ip_address) '
                    'VALUES (?,?,?,?,?,?,?)',
                    (host, route_id, route_name, ts, event_type, detail, ip_address)
                )
        except Exception:
            pass

    def _track_session(self, host: str, ts: str, p: dict):
        """Detect route running/idle transitions and client count changes."""
        route_id   = p['id']
        route_name = p.get('name', route_id)
        state      = p.get('state', 'idle')
        conns      = p.get('active_connections', 0)
        prev       = self._last_states.setdefault(host, {}).get(route_id)
        prev_conns = self._last_states[host].get(route_id + '__conns')
        if prev != state:
            with connect_db() as c:
                if state == 'running':
                    c.execute(
                        'INSERT INTO srt_session (device_host, route_id, route_name, started_at) '
                        'VALUES (?,?,?,?)', (host, route_id, route_name, ts)
                    )
                elif state == 'idle' and prev == 'running':
                    c.execute(
                        'UPDATE srt_session SET ended_at=? '
                        'WHERE device_host=? AND route_id=? AND ended_at IS NULL',
                        (ts, host, route_id)
                    )
            if prev is not None:
                self._persist_event(host, ts, route_id, route_name,
                    'Route started' if state == 'running' else 'Route stopped')
            self._last_states[host][route_id] = state
        # Client connect/disconnect — track per-IP
        curr_ips = set()
        for dst in p.get('destinations', []):
            for conn in dst.get('connections', []):
                addr = conn.get('address')
                if addr:
                    curr_ips.add(addr)
        prev_ips = self._last_states[host].get(route_id + '__ips', None)
        if prev_ips is not None:
            for ip in curr_ips - prev_ips:
                self._persist_event(host, ts, route_id, route_name,
                    'Client connected', str(len(curr_ips)) + ' total', ip_address=ip)
            for ip in prev_ips - curr_ips:
                self._persist_event(host, ts, route_id, route_name,
                    'Client disconnected', str(len(curr_ips)) + ' remaining', ip_address=ip)
        elif prev_conns is not None and conns != prev_conns:
            # Fallback if IP tracking not yet available
            if conns > prev_conns:
                self._persist_event(host, ts, route_id, route_name,
                    'Client connected', str(conns) + ' total')
            else:
                self._persist_event(host, ts, route_id, route_name,
                    'Client disconnected', str(conns) + ' remaining')
        self._last_states[host][route_id + '__ips'] = curr_ips
        self._last_states[host][route_id + '__conns'] = conns

    def _load_slack_cfg(self):
        try:
            with connect_db() as c:
                r = c.execute('SELECT webhook_url, channel, username FROM slack_config WHERE id=1').fetchone()
                if r and r[0] and r[0].strip():
                    return r
        except Exception: pass
        return None

    def _load_gw_slack(self, gw_id: int) -> dict:
        try:
            with connect_db() as c:
                c.execute("INSERT OR IGNORE INTO srt_gw_slack(gw_id) VALUES(?)", (gw_id,))
                r = c.execute("SELECT notify_source,notify_client,notify_paused,"
                              "thr_bitrate_min,thr_loss_max,thr_lost_max,"
                              "thr_dropped_max,thr_retransmit_max "
                              "FROM srt_gw_slack WHERE gw_id=?", (gw_id,)).fetchone()
                if r:
                    return dict(zip(["notify_source","notify_client","notify_paused",
                                     "thr_bitrate_min","thr_loss_max","thr_lost_max",
                                     "thr_dropped_max","thr_retransmit_max"], r))
        except Exception: pass
        return {}

    def _thr_alert(self, host, ts, route_id, route_name, key, val, thr, label, unit,
                   webhook, channel, username, dev_name, worse_msg, better_msg,
                   slack_color_bad, slack_color_ok):
        """Generic threshold alert — fires on entry and recovery. Also logs to srt_event."""
        if not thr or val is None: return
        state = self.alert_state.setdefault(host, {})
        bad = val > thr
        if bad and not state.get(key):
            msg = f"{worse_msg}: *{val:.2f} {unit}* (thr {thr} {unit})"
            POLLER._slack_post(webhook, f":warning: *{dev_name}* — Route `{route_name}` {msg}",
                               channel, username, color=slack_color_bad)
            self._persist_event(host, ts, route_id, route_name,
                                f"Threshold breach: {label}", f"{val:.2f} {unit} > {thr}")
            state[key] = True
        elif not bad and state.get(key):
            msg = f"{better_msg}: {val:.2f} {unit}"
            POLLER._slack_post(webhook, f":white_check_mark: *{dev_name}* — Route `{route_name}` {msg}",
                               channel, username, color=slack_color_ok)
            self._persist_event(host, ts, route_id, route_name,
                                f"Threshold recovered: {label}", f"{val:.2f} {unit} <= {thr}")
            state[key] = False

    def _notify_gateway(self, host: str, dev_name: str, normalized: dict, cfg):
        webhook, channel, username = cfg
        # Load per-device notification config
        gw_id = None
        try:
            with connect_db() as c:
                r = c.execute("SELECT id FROM srt_gateway_device WHERE host=?", (host,)).fetchone()
                if r: gw_id = r[0]
        except Exception: pass
        gcfg = self._load_gw_slack(gw_id) if gw_id else {}
        if gcfg.get("notify_paused"): return

        state = self.alert_state.setdefault(host, {})
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())

        for route_id, p in normalized.items():
            rname  = p.get('name', route_id)
            rstate = p.get('state', 'idle')
            rkey   = f'state:{route_id}'
            prev   = state.get(rkey)

            # Route start/stop
            if prev != rstate:
                if rstate == 'running' and prev is not None:
                    POLLER._slack_post(webhook,
                        f':green_circle: *{dev_name}* — Route `{rname}` started',
                        channel, username, color='good')
                    self._persist_event(host, ts, route_id, rname, 'Route started')
                elif rstate == 'idle' and prev == 'running':
                    POLLER._slack_post(webhook,
                        f':red_circle: *{dev_name}* — Route `{rname}` stopped',
                        channel, username, color='danger')
                    self._persist_event(host, ts, route_id, rname, 'Route stopped')
                state[rkey] = rstate

            if rstate != 'running':
                continue

            src  = p.get('source', {})
            dsts = p.get('destinations', [])

            # Source connect/disconnect
            if gcfg.get('notify_source', 1):
                src_state = src.get('state', '')
                src_key   = f'src_state:{route_id}'
                prev_src  = state.get(src_key)
                if prev_src is not None and prev_src != src_state:
                    if src_state == 'connected':
                        POLLER._slack_post(webhook,
                            f':satellite_antenna: *{dev_name}* — Route `{rname}` source connected',
                            channel, username, color='good')
                        self._persist_event(host, ts, route_id, rname, 'Source connected',
                                            src.get('address', ''))
                    elif prev_src == 'connected':
                        POLLER._slack_post(webhook,
                            f':zzz: *{dev_name}* — Route `{rname}` source disconnected',
                            channel, username, color='warning')
                        self._persist_event(host, ts, route_id, rname, 'Source disconnected')
                state[src_key] = src_state

            # Client connect/disconnect — per-IP detection to include IP in Slack msg
            if gcfg.get('notify_client', 1):
                curr_ips = set()
                for _dst in dsts:
                    for _conn in _dst.get('connections', []):
                        _addr = _conn.get('address')
                        if _addr and _addr != '0.0.0.0':
                            curr_ips.add(_addr)
                cli_ips_key = f'cli_ips:{route_id}'
                prev_ips = state.get(cli_ips_key)
                if prev_ips is not None:
                    for _ip in sorted(curr_ips - prev_ips):
                        POLLER._slack_post(webhook,
                            f':busts_in_silhouette: *{dev_name}* — Route `{rname}` '
                            f'client connected `{_ip}` ({len(curr_ips)} total)',
                            channel, username, color='good')
                    for _ip in sorted(prev_ips - curr_ips):
                        POLLER._slack_post(webhook,
                            f':bust_in_silhouette: *{dev_name}* — Route `{rname}` '
                            f'client disconnected `{_ip}` ({len(curr_ips)} remaining)',
                            channel, username, color='warning')
                state[cli_ips_key] = curr_ips
                state[f'cli_count:{route_id}'] = len(curr_ips)

            # Threshold alerts
            br   = p.get('total_bitrate_mbps')
            loss = src.get('loss_pct')
            # Aggregate lost/dropped/retransmit across all destination connections
            lost = sum(len([c for c in d.get('connections', [])]) for d in dsts)  # placeholder
            # Use src connection stats for lost/dropped/retransmit when available
            src_conns = src.get('connections', [])
            if src_conns:
                sc = src_conns[0]
                lost_val      = sc.get('srtNumLostPackets')
                dropped_val   = sc.get('srtDroppedPackets')
                retransmit_val= sc.get('srtRetransmitRate')
            else:
                lost_val = dropped_val = retransmit_val = None

            thr_br  = float(gcfg.get('thr_bitrate_min') or 0)
            thr_loss= float(gcfg.get('thr_loss_max') or 0)
            thr_lost= float(gcfg.get('thr_lost_max') or 0)
            thr_drop= float(gcfg.get('thr_dropped_max') or 0)
            thr_ret = float(gcfg.get('thr_retransmit_max') or 0)

            # Bitrate min (alert when BELOW threshold)
            if thr_br and br is not None:
                br_key = f'br_low:{route_id}'
                bad = br < thr_br
                if bad and not state.get(br_key):
                    POLLER._slack_post(webhook,
                        f':chart_with_downwards_trend: *{dev_name}* — Route `{rname}` bitrate low: *{br:.2f} Mb/s* (min {thr_br})',
                        channel, username, color='warning')
                    self._persist_event(host, ts, route_id, rname, 'Threshold breach: Bitrate',
                                        f'{br:.2f} Mb/s < {thr_br}')
                    state[br_key] = True
                elif not bad and state.get(br_key):
                    POLLER._slack_post(webhook,
                        f':white_check_mark: *{dev_name}* — Route `{rname}` bitrate normal: {br:.2f} Mb/s',
                        channel, username, color='good')
                    self._persist_event(host, ts, route_id, rname, 'Threshold recovered: Bitrate',
                                        f'{br:.2f} Mb/s >= {thr_br}')
                    state[br_key] = False

            self._thr_alert(host, ts, route_id, rname, f'loss:{route_id}', loss, thr_loss,
                            'Loss', '%', webhook, channel, username, dev_name,
                            'Packet loss', 'Loss normal', 'danger', 'good')
            self._thr_alert(host, ts, route_id, rname, f'lost:{route_id}', lost_val, thr_lost,
                            'Lost packets', 'pkts', webhook, channel, username, dev_name,
                            'Lost packets', 'Lost packets normal', 'danger', 'good')
            self._thr_alert(host, ts, route_id, rname, f'drop:{route_id}', dropped_val, thr_drop,
                            'Dropped', 'pkts', webhook, channel, username, dev_name,
                            'Dropped packets', 'Dropped normal', 'danger', 'good')
            self._thr_alert(host, ts, route_id, rname, f'ret:{route_id}', retransmit_val, thr_ret,
                            'Retransmit', 'b/s', webhook, channel, username, dev_name,
                            'Retransmit rate', 'Retransmit normal', 'warning', 'good')


SRT_POLLER: Optional['SRTGatewayPoller'] = None

class App:

    @cherrypy.expose
    def login(self, msg=None):
        import html as _h
        u, _ = _get_credentials()
        msg_html = ''
        if msg == 'error':
            msg_html = '<div class="alert alert-danger mt-3">Identifiants incorrects.</div>'
        body = f'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <title>StreamPilot — Login</title>
  <style>
    body {{ background-color: #f0f4f8; display: flex; flex-direction: column; min-height: 100vh; margin: 0; }}
    .login-wrap {{ flex: 1; display: flex; align-items: center; justify-content: center; }}
    .login-card {{ width: 100%; max-width: 380px; padding: 2.5rem; background: #fff; border-radius: 12px; box-shadow: 0 4px 24px rgba(0,0,0,.10); }}
    .login-logo {{ display: block; max-width: 200px; margin: 0 auto 1.5rem; }}
  </style>
</head>
<body>
  <div class="login-wrap">
    <div class="login-card">
      <img src="/static/StreamPilot.png" alt="StreamPilot" class="login-logo">
      <form method="post" action="/login_post">
        <div class="mb-3">
          <label class="form-label fw-semibold">Username</label>
          <input class="form-control" type="text" name="username" autofocus autocomplete="username" required>
        </div>
        <div class="mb-3">
          <label class="form-label fw-semibold">Password</label>
          <input class="form-control" type="password" name="password" autocomplete="current-password" required>
        </div>
        {msg_html}
        <button class="btn btn-primary w-100 mt-2" type="submit">Sign in</button>
      </form>
    </div>
  </div>
  <footer class="text-center text-muted mt-4 mb-3 small">
    Alexandre Licinio &copy; 2026 &mdash; StreamPilot &mdash; Stream smarter. Pilot with precision. Broadcast better.
  </footer>
</body>
</html>'''
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return body.encode('utf-8')

    @cherrypy.expose
    def login_post(self, username='', password=''):
        expected_user, expected_pass = _get_credentials()
        if username.strip() == expected_user and password == expected_pass:
            cherrypy.session['username'] = username.strip()
            raise cherrypy.HTTPRedirect('/')
        raise cherrypy.HTTPRedirect('/login?msg=error')


    # ── SRT Gateway endpoints ─────────────────────────────────────────────────

    @require_login
    @cherrypy.expose
    def gateway_dashboard(self):
        import json as _j
        with connect_db() as c:
            devices = c.execute(
                "SELECT id, name, host, port, protocol FROM srt_gateway_device"
            ).fetchall()
        rows = []
        for dev in devices:
            did, name, host, port, proto = dev
            routes = list((SRT_POLLER.last_payloads.get(host) or {}).values()) if SRT_POLLER else []
            status_badge = (
                '<span class="badge text-bg-success">Online</span>' if routes
                else '<span class="badge text-bg-secondary">Connecting...</span>'
            )
            route_rows = ""
            for r in routes:
                state = r.get("state", "idle")
                sbadge = ('<span class="badge text-bg-success">running</span>'
                          if state == "running" else
                          '<span class="badge text-bg-secondary">idle</span>')
                src  = r.get("source", {})
                rtt  = src.get("rtt_ms")
                loss = src.get("loss_pct")
                br   = r.get("total_bitrate_mbps")
                route_rows += (
                    "<tr>"
                    + "<td>" + str(r.get("name","")) + "</td>"
                    + "<td>" + sbadge + "</td>"
                    + "<td>" + ("%.2f Mb/s" % br if br is not None else "—") + "</td>"
                    + "<td>" + ("%.1f ms" % rtt if rtt is not None else "—") + "</td>"
                    + "<td>" + ("%.2f%%" % loss if loss is not None else "—") + "</td>"
                    + "<td>" + str(r.get("active_connections", 0)) + "</td>"
                    + '<td><a href="/gateway_route?host=' + host + "&route_id=" + str(r.get("id","")) + '" class="btn btn-sm btn-outline-primary">Details</a></td>'
                    + "</tr>"
                )
            rows.append(
                "<div class=\"card mb-3\">"
                + "<div class=\"card-header d-flex justify-content-between align-items-center\">"
                + "<strong>" + name + " (" + host + ")</strong>" + status_badge
                + "</div>"
                + "<div class=\"card-body p-0\">"
                + "<table class=\"table table-sm mb-0\"><thead><tr>"
                + "<th>Route</th><th>State</th><th>Bitrate</th><th>RTT</th><th>Loss</th><th>Conn.</th><th></th>"
                + "</tr></thead><tbody>"
                + (route_rows or "<tr><td colspan=7 class=text-muted>No data yet</td></tr>")
                + "</tbody></table></div>"
                + "<div class=\"card-footer small text-muted\">"
                + '<a href="/gateway_add_device">+ Add device</a>'
                + "</div></div>"
            )
        body = "".join(rows) or '<p class="text-muted">No SRT Gateway configured. <a href="/gateway_add_device">Add one</a>.</p>'
        with connect_db() as c:
            gw_rows = c.execute("SELECT id,name,host,port,protocol FROM srt_gateway_device ORDER BY id ASC").fetchall()
            srt_gateways = [{"id":r[0],"name":r[1],"host":r[2],"port":r[3],"protocol":r[4]} for r in gw_rows]
        return render("gateway_dashboard.html", srt_gateways=srt_gateways,
                      page_title="SRT Gateway", user=current_user(),
                      srt_retention_days=int(os.getenv('SRT_RETENTION_DAYS') or 30))

    @require_login
    @cherrypy.expose
    def gateway_route(self, host=None, route_id=None, tab=None, msg=None):
        import json as _j
        if not host or not route_id:
            raise cherrypy.HTTPRedirect("/devices")
        payload = {}
        if SRT_POLLER:
            payload = (SRT_POLLER.last_payloads.get(host) or {}).get(route_id, {})
        with connect_db() as c:
            samples = c.execute(
                "SELECT ts, total_bitrate_mbps, src_rtt_ms, src_loss_pct "
                "FROM srt_route_sample WHERE device_host=? AND route_id=? "
                "ORDER BY id DESC LIMIT 200", (host, route_id)
            ).fetchall()
            conns = c.execute(
                "SELECT ts, direction, address, port, bitrate_mbps, rtt_ms, loss_pct "
                "FROM srt_connection_sample WHERE device_host=? AND route_id=? "
                "ORDER BY id DESC LIMIT 50", (host, route_id)
            ).fetchall()
        samples_json = _j.dumps([
            {"ts": s[0], "bitrate": s[1], "rtt": s[2], "loss": s[3]}
            for s in reversed(samples)
        ])
        conns_html = ""
        for conn in conns[:20]:
            ts, direction, addr, port, br, rtt, loss = conn
            conns_html += (
                "<tr><td>" + str(ts) + "</td><td>" + str(direction) + "</td>"
                + "<td>" + str(addr) + ":" + str(port) + "</td>"
                + "<td>" + ("%.2f" % br if br is not None else "—") + "</td>"
                + "<td>" + ("%.1f" % rtt if rtt is not None else "—") + "</td>"
                + "<td>" + ("%.2f%%" % loss if loss is not None else "—") + "</td></tr>"
            )
        src = payload.get("source", {})
        state = payload.get("state", "—")
        sbadge = "text-bg-success" if state == "running" else "text-bg-secondary"
        br_s   = ("%.2f Mb/s" % payload["total_bitrate_mbps"]) if payload.get("total_bitrate_mbps") is not None else "—"
        rtt_s  = ("%.1f ms" % src["rtt_ms"]) if src.get("rtt_ms") is not None else "—"
        loss_s = ("%.2f%%" % src["loss_pct"]) if src.get("loss_pct") is not None else "—"
        return render("gateway_route.html",
                      page_title="Route — " + str(payload.get("name", route_id)),
                      host=host, route_id=route_id,
                      user=current_user())

    @require_login
    @cherrypy.expose
    def gateway_add_device(self, name=None, host=None, port="443",
                            protocol="https", username="haiadmin", password="haiadmin"):
        if host:
            lenv = os.getenv("MAX_SRTGATEWAY")
            try: lgw = int(lenv) if lenv and str(lenv).strip() else None
            except Exception: lgw = None
            with connect_db() as c:
                if lgw is not None:
                    cnt = c.execute("SELECT COUNT(*) FROM srt_gateway_device").fetchone()[0] or 0
                    if cnt >= lgw:
                        raise cherrypy.HTTPRedirect(
                            f"/devices?msg=Max+{lgw}+SRT+Gateway(s)+can+be+configured")
                c.execute(
                    "INSERT OR REPLACE INTO srt_gateway_device "
                    "(name, host, port, protocol, username, password) "
                    "VALUES (?,?,?,?,?,?)",
                    (name or host, host, int(port or 443), protocol, username, password)
                )
            raise cherrypy.HTTPRedirect("/devices")
        raise cherrypy.HTTPRedirect("/devices")

    @require_login
    @cherrypy.expose
    def gateway_delete_device(self, id=None):
        if id:
            with connect_db() as c:
                c.execute("DELETE FROM srt_gateway_device WHERE id=?", (id,))
        raise cherrypy.HTTPRedirect("/devices")

    @require_login
    @cherrypy.expose
    def gateway_events(self, host=None, route_id=None, limit='20'):
        import json as _j
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not host:
            return b'{"ok":false}'
        try:
            lim = max(1, min(100, int(limit)))
        except Exception:
            lim = 20
        with connect_db() as c:
            if route_id:
                rows = c.execute(
                    'SELECT ts, route_name, event_type, detail, ip_address FROM srt_event '
                    'WHERE device_host=? AND route_id=? ORDER BY id DESC LIMIT ?',
                    (host, route_id, lim)
                ).fetchall()
            else:
                rows = c.execute(
                    'SELECT ts, route_name, event_type, detail, ip_address FROM srt_event '
                    'WHERE device_host=? ORDER BY id DESC LIMIT ?',
                    (host, lim)
                ).fetchall()
        events = [{"ts": r[0], "route": r[1], "type": r[2], "detail": r[3], "ip": r[4] or ""} for r in rows]
        return _j.dumps({"ok": True, "events": events}).encode("utf-8")

    @require_login
    @cherrypy.expose
    def gateway_logs(self, host=None, msg=None):
        import html as _h
        def esc(x): return _h.escape('' if x is None else str(x))
        with connect_db() as c:
            gateways = {r[0]: r[1] for r in c.execute(
                'SELECT host, name FROM srt_gateway_device').fetchall()}
            routes = c.execute(
                'SELECT rs.device_host, rs.route_id, MAX(rs.route_name),'
                ' MIN(rs.ts), MAX(rs.ts), MAX(rs.state)'
                ' FROM srt_route_sample rs'
                ' GROUP BY rs.device_host, rs.route_id'
                ' ORDER BY MAX(rs.ts) DESC'
            ).fetchall()
            evt_counts = {(r[0], r[1]): r[2] for r in c.execute(
                'SELECT device_host, route_id, COUNT(*) FROM srt_event'
                ' GROUP BY device_host, route_id').fetchall()}
        msg_html = ('<div class="alert alert-success">' + esc(msg) + '</div>') if msg else ''
        def fmt_ts(ts):
            if not ts: return '—'
            try:
                s = str(ts)[:16]; p = s[:10].split('-')
                return p[2]+'/'+p[1]+'/'+p[0]+' '+s[11:16]
            except Exception: return str(ts)
        rows_html = ''
        for dev_host, route_id, rname, first_seen, last_seen, last_state in (routes or []):
            gw_name = gateways.get(dev_host, dev_host)
            active = last_state == 'running'
            sbadge = ('<span class="badge text-bg-success">Active</span>' if active
                      else '<span class="badge text-bg-secondary">Idle</span>')
            evts = evt_counts.get((dev_host, route_id), 0)
            rows_html += (
                '<tr>'
                + '<td>' + esc(gw_name) + '</td>'
                + '<td class="fw-semibold">' + esc(rname) + '</td>'
                + '<td>' + sbadge + '</td>'
                + '<td class="small text-muted">' + fmt_ts(first_seen) + '</td>'
                + '<td class="small text-muted">' + fmt_ts(last_seen) + '</td>'
                + '<td><span class="badge text-bg-secondary">' + str(evts) + '</span></td>'
                + '<td>'
                + '<a class="btn btn-sm btn-outline-primary me-1" href="/gateway_logs_route?host='
                + esc(dev_host) + '&route_id=' + esc(route_id) + '">Events</a>'
                + '<a class="btn btn-sm btn-outline-dark me-1" href="/gateway_logs_pdf?host='
                + esc(dev_host) + '&route_id=' + esc(route_id) + '" target="_blank">PDF</a>'
                + '<form method="post" action="/gateway_logs_delete" class="d-inline"'
                + ' onsubmit="return confirm(\'Delete all logs for this route?\')">'
                + '<input type="hidden" name="host" value="' + esc(dev_host) + '">'
                + '<input type="hidden" name="route_id" value="' + esc(route_id) + '">'
                + '<button class="btn btn-sm btn-outline-danger">Delete</button>'
                + '</form></td></tr>'
            )
        # Group by day with dividers
        items = []
        current_day = None
        for dev_host, route_id, rname, first_seen, last_seen, last_state in (routes or []):
            gw_name = gateways.get(dev_host, dev_host)
            active = last_state == 'running'
            sbadge = ('<span class="badge text-bg-success">Active</span>' if active
                      else '<span class="badge text-bg-secondary">Idle</span>')
            evts = evt_counts.get((dev_host, route_id), 0)
            try:
                p = str(last_seen)[:10].split('-'); day_lbl = p[2]+'/'+p[1]+'/'+p[0]
            except Exception: day_lbl = str(last_seen)[:10]
            if day_lbl != current_day:
                items.append(
                    '<tr><td colspan="7" class="p-0">'
                    '<div class="bd-callout bd-callout-info w-100 mb-0" role="separator">'
                    + day_lbl + '</div></td></tr>'
                )
                current_day = day_lbl
            items.append(
                '<tr>'
                + '<td>' + esc(gw_name) + '</td>'
                + '<td class="fw-semibold">' + esc(rname) + '</td>'
                + '<td>' + sbadge + '</td>'
                + '<td class="small text-muted">' + fmt_ts(first_seen) + '</td>'
                + '<td class="small text-muted">' + fmt_ts(last_seen) + '</td>'
                + '<td><span class="badge text-bg-secondary">' + str(evts) + '</span></td>'
                + '<td>'
                + '<a class="btn btn-sm btn-outline-primary me-1" href="/gateway_logs_route?host='
                + esc(dev_host) + '&route_id=' + esc(route_id) + '">Events</a>'
                + '<a class="btn btn-sm btn-outline-dark me-1" href="/gateway_logs_pdf?host='
                + esc(dev_host) + '&route_id=' + esc(route_id) + '" target="_blank">PDF</a>'
                + '<form method="post" action="/gateway_logs_delete" class="d-inline"'
                + ' onsubmit="return confirm(\'Delete all logs for this route?\')">' 
                + '<input type="hidden" name="host" value="' + esc(dev_host) + '">'
                + '<input type="hidden" name="route_id" value="' + esc(route_id) + '">'
                + '<button class="btn btn-sm btn-outline-danger">Delete</button>'
                + '</form></td></tr>'
            )
        tbody = ''.join(items) if items else '<tr><td colspan="7" class="text-muted text-center p-4">No route history yet.</td></tr>'
        msg_html2 = ('<div class="alert alert-info">' + esc(msg) + '</div>') if msg else ''
        html_body = (
            '<!doctype html><html><head>'
            '<meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">'
            '<style>.bd-callout{padding:.75rem 1rem;border:1px solid var(--bs-border-color);'
            'border-left-width:.25rem;border-radius:.25rem;background-color:var(--bs-body-bg);}'
            '.bd-callout-info{border-left-color:#0d6efd;background:rgba(13,110,253,.06);}'
            '</style>'
            '<title>SRT Gateway Logs — StreamPilot</title>'
            '</head><body class="p-3">'
            '<div class="d-flex justify-content-between align-items-center mb-3">'
            '<h1 class="h4 m-0">SRT Gateway — Route Logs</h1>'
            '<a class="btn btn-outline-secondary" href="/gateway_dashboard">← SRT Gateway</a>'
            '</div>'
            + msg_html2
            + '<div class="table-responsive">'
            '<table class="table table-sm align-middle">'
            '<thead><tr>'
            '<th>Gateway</th><th>Route</th><th>Status</th>'
            '<th>First seen (UTC)</th><th>Last seen (UTC)</th><th>Events</th><th>Actions</th>'
            '</tr></thead>'
            '<tbody>' + tbody + '</tbody>'
            '</table></div>'
            '<script>(function(){'
            'var tz=new Date().getTimezoneOffset();'
            "document.querySelectorAll('a[href*=\"gateway_logs_pdf\"]').forEach(function(a){"
            "if(a.href.indexOf('tz_offset')<0) a.href+='&tz_offset='+tz;});})();</script>"
            '</body></html>'
        )
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return html_body.encode('utf-8')

    @require_login
    @cherrypy.expose
    def gateway_logs_route(self, host=None, route_id=None, page='1'):
        import html as _h
        def esc(x): return _h.escape('' if x is None else str(x))
        if not host or not route_id:
            raise cherrypy.HTTPRedirect('/gateway_logs')
        PER_PAGE = 50
        try: pgn = max(1, int(page))
        except Exception: pgn = 1
        offset = (pgn - 1) * PER_PAGE
        with connect_db() as c:
            total = c.execute(
                'SELECT COUNT(*) FROM srt_event WHERE device_host=? AND route_id=?',
                (host, route_id)).fetchone()[0]
            rows = c.execute(
                'SELECT ts, event_type, detail, ip_address FROM srt_event'
                ' WHERE device_host=? AND route_id=? ORDER BY id DESC LIMIT ? OFFSET ?',
                (host, route_id, PER_PAGE, offset)).fetchall()
            rn = c.execute(
                'SELECT route_name FROM srt_event WHERE device_host=? AND route_id=? LIMIT 1',
                (host, route_id)).fetchone()
            rname = rn[0] if rn else route_id
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        def fmt_ts(ts):
            try:
                s = str(ts); p = s[:10].split('-')
                return p[2]+'/'+p[1]+'/'+p[0], s[11:19]
            except Exception: return str(ts), ''
        rows_html = ''
        for ts, etype, detail, ip in rows:
            dt, tm = fmt_ts(ts)
            t = etype or ''
            if 'started' in t or 'connected' in t:
                badge = '<span class="badge text-bg-success">' + esc(t) + '</span>'
            elif 'stopped' in t or 'disconnected' in t:
                badge = '<span class="badge text-bg-danger">' + esc(t) + '</span>'
            else:
                badge = '<span class="badge text-bg-secondary">' + esc(t) + '</span>'
            ip_cell = ('<code class="small">' + esc(ip) + '</code>') if ip else '<span class="text-muted">—</span>'
            rows_html += ('<tr data-utc="' + esc(str(ts)[:19]) + '">'
                          + '<td class="text-muted small local-time"></td>'
                          + '<td class="text-muted small">' + dt + '</td>'
                          + '<td class="text-muted small">' + tm + '</td>'
                          + '<td>' + badge + '</td>'
                          + '<td class="small text-muted">' + esc(detail or '') + '</td>'
                          + '<td>' + ip_cell + '</td></tr>')
        if not rows_html:
            rows_html = '<tr><td colspan="6" class="text-muted text-center p-4">No events.</td></tr>'
        def plink(p2, label):
            dis = ' disabled' if p2 < 1 or p2 > total_pages else ''
            return ('<li class="page-item' + dis + '"><a class="page-link" href="/gateway_logs_route?host='
                    + esc(host) + '&route_id=' + esc(route_id) + '&page=' + str(p2) + '">' + label + '</a></li>')
        pager = ''
        if total_pages > 1:
            items = plink(pgn-1, '‹')
            for i in range(max(1, pgn-3), min(total_pages+1, pgn+4)):
                act = ' active' if i == pgn else ''
                items += ('<li class="page-item' + act + '"><a class="page-link" href="/gateway_logs_route?host='
                           + esc(host) + '&route_id=' + esc(route_id) + '&page=' + str(i) + '">' + str(i) + '</a></li>')
            items += plink(pgn+1, '›')
            pager = '<nav><ul class="pagination pagination-sm mb-0">' + items + '</ul></nav>'
        html_body2 = (
            '<!doctype html><html><head>'
            '<meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">'
            '<title>Events — ' + esc(rname) + ' — StreamPilot</title>'
            '</head><body class="p-4">'
            '<div class="d-flex justify-content-between align-items-center mb-3">'
            '<div class="d-flex align-items-center gap-2">'
            '<a href="/gateway_logs" class="btn btn-outline-secondary btn-sm">← Logs</a>'
            '<h1 class="h4 m-0">Events — ' + esc(rname) + '</h1>'
            '<span class="badge text-bg-secondary">' + str(total) + ' total</span>'
            '</div></div>'
            '<div class="mb-3">'
            '<input type="search" id="evtSearch" class="form-control" placeholder="Search events, details or IP…">'
            '</div>'
            '<div class="table-responsive">'
            '<table class="table table-sm table-hover align-middle" id="evtTable">'
            '<thead class="table-light"><tr>'
            '<th>Local time</th><th>Date (UTC)</th><th>Time (UTC)</th><th>Event</th><th>Detail</th><th>IP</th>'
            '</tr></thead>'
            '<tbody>' + rows_html + '</tbody>'
            '</table></div>'
            '<div class="d-flex justify-content-between align-items-center mt-3">'
            '<span class="text-muted small" id="evtCount">Page ' + str(pgn) + ' / ' + str(total_pages)
            + ' — ' + str(total) + ' events</span>'
            + pager
            + '</div>'
            + """
            <script>
            (function(){
              var tz = new Date().getTimezoneOffset();
              document.querySelectorAll('a[href*="gateway_logs_pdf"]').forEach(function(a){
                if(a.href.indexOf('tz_offset') < 0) a.href += '&tz_offset=' + tz;
              });
              function fillLocal(){
                document.querySelectorAll('tr[data-utc]').forEach(function(tr){
                  var utc = tr.dataset.utc; if(!utc) return;
                  var cell = tr.querySelector('.local-time'); if(!cell) return;
                  try {
                    var d = new Date(utc.replace(' ','T') + 'Z');
                    cell.textContent = d.toLocaleTimeString(undefined,
                      {hour:'2-digit', minute:'2-digit', second:'2-digit'});
                  } catch(e) { cell.textContent = '?'; }
                });
              }
              if(document.readyState === 'loading'){
                document.addEventListener('DOMContentLoaded', fillLocal);
              } else { fillLocal(); }
            """
            '  var inp=document.getElementById("evtSearch");'
            '  var tbl=document.getElementById("evtTable");'
            '  var tot=' + str(total) + ';'
            '  inp.addEventListener("input",function(){'
            '    var q=inp.value.toLowerCase();'
            '    var rows=tbl.querySelectorAll("tbody tr");'
            '    var shown=0;'
            '    rows.forEach(function(tr){'
            '      var m=!q||tr.textContent.toLowerCase().indexOf(q)>=0;'
            '      tr.style.display=m?"":"none"; if(m)shown++;'
            '    });'
            '    document.getElementById("evtCount").textContent='
            '      q?(shown+" result(s) found"):(tot+" events");'
            '  });'
            '})();</script>'
            '</body></html>'
        )
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return html_body2.encode('utf-8')

    @require_login
    @cherrypy.expose
    def gateway_logs_delete(self, host=None, route_id=None):
        if host and route_id:
            with connect_db() as c:
                for tbl in ('srt_event', 'srt_route_sample', 'srt_connection_sample', 'srt_session'):
                    c.execute('DELETE FROM ' + tbl + ' WHERE device_host=? AND route_id=?',
                              (host, route_id))
        raise cherrypy.HTTPRedirect('/gateway_logs?msg=Logs+deleted')

    @require_login
    @cherrypy.expose
    def gateway_logs_pdf(self, host=None, route_id=None, tz_offset=None):
        import html as _h, time as _t
        from datetime import datetime as _dtlg, timedelta as _tdlg
        try: _tz_min = int((tz_offset[-1] if isinstance(tz_offset, list) else tz_offset) or 0)
        except Exception: _tz_min = 0
        _tz_hours = -_tz_min / 60
        _tz_label = f"UTC{'+' if _tz_hours >= 0 else ''}{_tz_hours:g}"
        def _utc_to_local_lg(ts_str):
            try:
                d = _dtlg.strptime(str(ts_str)[:19], '%Y-%m-%d %H:%M:%S')
                return (d - _tdlg(minutes=_tz_min)).strftime('%H:%M:%S')
            except Exception: return ''
        def esc(x): return _h.escape('' if x is None else str(x))
        if not host or not route_id:
            raise cherrypy.HTTPRedirect('/gateway_logs')
        with connect_db() as c:
            rows = c.execute(
                'SELECT ts, event_type, detail, ip_address FROM srt_event'
                ' WHERE device_host=? AND route_id=? ORDER BY id ASC',
                (host, route_id)).fetchall()
            rn = c.execute(
                'SELECT route_name FROM srt_event WHERE device_host=? AND route_id=? LIMIT 1',
                (host, route_id)).fetchone()
            rname = rn[0] if rn else route_id
            gw = c.execute('SELECT name FROM srt_gateway_device WHERE host=?', (host,)).fetchone()
            gw_name = gw[0] if gw else host
        generated = _t.strftime('%d/%m/%Y %H:%M:%S UTC', _t.gmtime())
        safe = rname.replace(' ', '_').replace('/', '-')[:40]

        # ── reportlab PDF ─────────────────────────────────────────────────
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import mm
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            import io

            buf = io.BytesIO()
            doc = SimpleDocTemplate(buf, pagesize=A4,
                                    leftMargin=15*mm, rightMargin=15*mm,
                                    topMargin=15*mm, bottomMargin=15*mm)
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle('title', parent=styles['Normal'],
                                         fontSize=14, fontName='Helvetica-Bold', spaceAfter=4)
            meta_style  = ParagraphStyle('meta',  parent=styles['Normal'],
                                         fontSize=8, textColor=colors.grey, spaceAfter=10)
            cell_style  = ParagraphStyle('cell',  parent=styles['Normal'], fontSize=7.5)

            # Fetch SRT version from live payload if available
            _log_srt_ver = ''
            if SRT_POLLER:
                _lp2 = (SRT_POLLER.last_payloads.get(host) or {}).get(route_id, {})
                _src2 = _lp2.get('source') or {}
                _src2_conns = _src2.get('connections') or []
                if _src2_conns:
                    _log_srt_ver = _src2_conns[0].get('srt_version', '')
                if not _log_srt_ver:
                    _log_srt_ver = _src2.get('srt_version', '')
            _ver_str = f' &nbsp;|&nbsp; SRT {_log_srt_ver}' if _log_srt_ver else ''

            story = [
                Paragraph(f'Route Events — {rname}', title_style),
                Paragraph(
                    f'Gateway: {gw_name} ({host}) &nbsp;|&nbsp; '
                    f'Route ID: {route_id}{_ver_str} &nbsp;|&nbsp; Generated: {generated}',
                    meta_style),
            ]

            # Table header + rows
            header = [[_tz_label, 'Date (UTC)', 'Time (UTC)', 'Event', 'Detail', 'IP']]
            data_rows = []
            for ts, etype, detail, ip in rows:
                try:
                    p = str(ts)[:10].split('-')
                    dt, tm = p[2]+'/'+p[1]+'/'+p[0], str(ts)[11:19]
                except Exception:
                    dt = tm = str(ts)
                loc_tm = _utc_to_local_lg(str(ts)[:19])
                data_rows.append([
                    Paragraph(loc_tm or '', cell_style),
                    Paragraph(dt or '', cell_style),
                    Paragraph(tm or '', cell_style),
                    Paragraph(etype or '', cell_style),
                    Paragraph(detail or '', cell_style),
                    Paragraph(ip or '', cell_style),
                ])
            if not data_rows:
                data_rows = [['—', '—', '—', 'No events recorded', '', '']]

            col_widths = [20*mm, 18*mm, 18*mm, 30*mm, 58*mm, 22*mm]
            tbl = Table(header + data_rows, colWidths=col_widths, repeatRows=1)
            tbl.setStyle(TableStyle([
                ('BACKGROUND',  (0,0), (-1,0), colors.HexColor('#f0f0f0')),
                ('FONTNAME',    (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE',    (0,0), (-1,0), 8),
                ('FONTSIZE',    (0,1), (-1,-1), 7.5),
                ('GRID',        (0,0), (-1,-1), 0.3, colors.HexColor('#dddddd')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fafafa')]),
                ('VALIGN',      (0,0), (-1,-1), 'TOP'),
                ('TOPPADDING',  (0,0), (-1,-1), 3),
                ('BOTTOMPADDING', (0,0), (-1,-1), 3),
                ('LEFTPADDING', (0,0), (-1,-1), 4),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 8*mm))
            story.append(Paragraph('StreamPilot — SRT Gateway Logs',
                                   ParagraphStyle('footer', parent=styles['Normal'],
                                                  fontSize=7, textColor=colors.grey)))
            doc.build(story)
            pdf_bytes = buf.getvalue()
            cherrypy.response.headers['Content-Type'] = 'application/pdf'
            cherrypy.response.headers['Content-Disposition'] = f'attachment; filename="events_{safe}.pdf"'
            return pdf_bytes
        except ImportError:
            pass
        except Exception as _pe:
            cherrypy.log(f'[gateway_logs_pdf] reportlab error: {_pe}')

        # ── fallback: wkhtmltopdf ─────────────────────────────────────────
        import subprocess as _sp, tempfile as _tf, os as _os
        trs = ''
        for ts, etype, detail, ip in rows:
            try:
                p = str(ts)[:10].split('-'); dt = p[2]+'/'+p[1]+'/'+p[0]; tm = str(ts)[11:19]
            except Exception:
                dt = tm = str(ts)
            from datetime import datetime as _dtfb, timedelta as _tdfb
            try:
                _local_tm = (_dtfb.strptime(str(ts)[:19], '%Y-%m-%d %H:%M:%S')
                             - _tdfb(minutes=_tz_min)).strftime('%H:%M:%S')
            except Exception: _local_tm = ''
            trs += ('<tr><td>' + esc(_local_tm) + '</td>'
                    + '<td>' + esc(dt) + '</td><td>' + esc(tm) + '</td>'
                    + '<td>' + esc(etype or '') + '</td><td>' + esc(detail or '') + '</td>'
                    + '<td>' + esc(ip or '') + '</td></tr>')
        html_doc = (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><style>'
            'body{font-family:Arial,sans-serif;font-size:11px;margin:20px}'
            'h1{font-size:16px;margin-bottom:4px}.meta{color:#666;font-size:10px;margin-bottom:16px}'
            'table{width:100%;border-collapse:collapse}'
            'th{background:#f0f0f0;text-align:left;padding:5px 8px;font-size:10px;text-transform:uppercase}'
            'td{padding:4px 8px;border-bottom:1px solid #eee;vertical-align:top}'
            'tr:nth-child(even) td{background:#fafafa}'
            '.footer{margin-top:20px;color:#999;font-size:9px;text-align:center}'
            '</style></head><body>'
            '<h1>Route Events — ' + esc(rname) + '</h1>'
            '<div class="meta">Gateway: ' + esc(gw_name) + ' (' + esc(host) + ') &nbsp;|&nbsp; '
            'Route ID: ' + esc(route_id) + ' &nbsp;|&nbsp; Generated: ' + generated + '</div>'
            f'<table><thead><tr><th>{_tz_label}</th><th>Date (UTC)</th><th>Time (UTC)</th><th>Event</th><th>Detail</th><th>IP</th></tr></thead>'
            '<tbody>' + (trs or '<tr><td colspan="5">No events recorded.</td></tr>') + '</tbody></table>'
            '<div class="footer">StreamPilot — SRT Gateway Logs</div>'
            '</body></html>'
        )
        try:
            with _tf.NamedTemporaryFile(suffix='.html', delete=False, mode='w', encoding='utf-8') as f:
                f.write(html_doc); hp = f.name
            pp = hp.replace('.html', '.pdf')
            r = _sp.run(['wkhtmltopdf', '--quiet', '--page-size', 'A4', hp, pp],
                        capture_output=True, timeout=30)
            if r.returncode == 0 and _os.path.exists(pp):
                data = open(pp, 'rb').read()
                _os.unlink(hp); _os.unlink(pp)
                cherrypy.response.headers['Content-Type'] = 'application/pdf'
                cherrypy.response.headers['Content-Disposition'] = f'attachment; filename="events_{safe}.pdf"'
                return data
            _os.unlink(hp)
            if _os.path.exists(pp): _os.unlink(pp)
        except Exception:
            pass
        # Last resort: HTML download
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        cherrypy.response.headers['Content-Disposition'] = f'attachment; filename="events_{safe}.html"'
        return html_doc.encode('utf-8')

    @require_login
    @cherrypy.expose
    def gateway_data_raw(self, host=None):
        """Fetch raw data directly from the gateway API and return full JSON.
        Includes device info, all route configs, and full statistics
        (source connections with all SRT fields, destination clientsStat, etc.)
        """
        import json as _j
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not host:
            return b'{"ok":false,"error":"missing host"}'
        # Get gateway credentials from DB
        try:
            with connect_db() as c:
                row = c.execute(
                    "SELECT protocol, port, username, password, gw_device_id, session_id "
                    "FROM srt_gateway_device WHERE host=?", (host,)
                ).fetchone()
        except Exception as e:
            return _j.dumps({"ok": False, "error": str(e)}).encode("utf-8")
        if not row:
            return b'{"ok":false,"error":"device not found"}'
        proto, port, user, pwd, gw_id, sid = row
        base = f'{proto}://{host}:{port}'
        from collect.scripts.srt_gateway import login, fetch_device_id, _HTTP, _cookies
        # Reuse or obtain session
        if not sid:
            sid = login(base, user, pwd)
            if not sid:
                return b'{"ok":false,"error":"login failed"}'
        result = {"ok": True, "host": host, "base_url": base}
        try:
            # 1. Device info
            r = _HTTP.get(base + "/api/devices", cookies=_cookies(sid),
                          timeout=5, verify=False)
            if r.status_code == 200:
                devices = r.json()
                result["device_info"] = devices[0] if devices else {}
                if not gw_id and devices:
                    gw_id = devices[0].get("_id")
            # 2. Device config (includes route list)
            if gw_id:
                r2 = _HTTP.get(f"{base}/api/devices/{gw_id}", cookies=_cookies(sid),
                               timeout=5, verify=False)
                if r2.status_code == 200:
                    result["device_config"] = r2.json()
            # 3. All routes with full statistics
            if gw_id:
                r3 = _HTTP.get(f"{base}/api/gateway/{gw_id}/routes",
                               cookies=_cookies(sid), timeout=5, verify=False)
                if r3.status_code == 200:
                    routes_data = r3.json()
                    routes_list = routes_data.get("data", routes_data) \
                        if isinstance(routes_data, dict) else routes_data
                    result["routes_config"] = routes_list
                    # Per-route full statistics
                    route_stats = {}
                    for route in (routes_list or []):
                        rid = route.get("id")
                        if not rid:
                            continue
                        rs = _HTTP.get(
                            f"{base}/api/gateway/{gw_id}/statistics?routeID={rid}",
                            cookies=_cookies(sid), timeout=5, verify=False
                        )
                        if rs.status_code == 200:
                            route_stats[rid] = rs.json()
                    result["routes_statistics"] = route_stats
        except Exception as e:
            result["error"] = str(e)
        return _j.dumps(result, indent=2).encode("utf-8")

    @require_login
    @cherrypy.expose
    def gateway_route_stats(self, host=None, route_id=None):
        """Return full raw statistics for one route directly from the gateway API."""
        import json as _j
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not host or not route_id:
            return b'{"ok":false,"error":"missing host or route_id"}'
        try:
            with connect_db() as c:
                row = c.execute(
                    "SELECT protocol, port, username, password, gw_device_id, session_id "
                    "FROM srt_gateway_device WHERE host=?", (host,)
                ).fetchone()
        except Exception as e:
            return _j.dumps({"ok": False, "error": str(e)}).encode("utf-8")
        if not row:
            return b'{"ok":false,"error":"device not found"}'
        proto, port, user, pwd, gw_id, sid = row
        base = f'{proto}://{host}:{port}'
        from collect.scripts.srt_gateway import login, _HTTP, _cookies
        if not sid:
            sid = login(base, user, pwd)
            if not sid:
                return b'{"ok":false,"error":"login failed"}'
        try:
            r = _HTTP.get(
                f"{base}/api/gateway/{gw_id}/statistics?routeID={route_id}",
                cookies=_cookies(sid), timeout=5, verify=False
            )
            if r.status_code != 200:
                return _j.dumps({"ok": False, "error": f"HTTP {r.status_code}"}).encode("utf-8")
            data = r.json()
            # Also get the route config for static fields
            r2 = _HTTP.get(
                f"{base}/api/gateway/{gw_id}/routes/{route_id}",
                cookies=_cookies(sid), timeout=5, verify=False
            )
            config = r2.json() if r2.status_code == 200 else {}
            return _j.dumps({"ok": True, "stats": data, "config": config}, indent=2).encode("utf-8")
        except Exception as e:
            return _j.dumps({"ok": False, "error": str(e)}).encode("utf-8")

    @require_login
    @cherrypy.expose
    def gateway_data(self, host=None):
        import json as _j
        from datetime import datetime as _dtn
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not host or not SRT_POLLER:
            return b'{"ok":false}'
        routes = SRT_POLLER.last_payloads.get(host, {})
        route_list = list(routes.values())
        # Enrich each route with oldest_sample_days for retention badge
        try:
            with connect_db() as c:
                for r in route_list:
                    row = c.execute(
                        'SELECT MIN(ts) FROM srt_route_sample WHERE device_host=? AND route_id=?',
                        (host, r.get('id'))
                    ).fetchone()
                    oldest = row[0] if row else None
                    if oldest:
                        try:
                            dt = _dtn.strptime(str(oldest)[:19], '%Y-%m-%d %H:%M:%S')
                            r['oldest_sample_days'] = (_dtn.utcnow() - dt).days
                        except Exception:
                            r['oldest_sample_days'] = None
                    else:
                        r['oldest_sample_days'] = None
        except Exception:
            pass
        return _j.dumps({"ok": True, "routes": route_list}).encode("utf-8")

    @cherrypy.expose
    def logout(self):
        try:
            cherrypy.session.clear()
        except Exception:
            pass
        raise cherrypy.HTTPRedirect('/login')

    @require_login
    @cherrypy.expose
    def log_import(self, **params):
        """
        Import a single session JSON (the format produced by /log_download).
        - Creates a new live_session row (new ID).
        - Inserts all per-tick samples; expands links[] into individual rows.
        """
        import json, sqlite3
        # GET -> simple upload form
        if cherrypy.request.method.upper() != 'POST':
            return ("""
            <!doctype html><html><head><meta charset='utf-8'>
            <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css' rel='stylesheet'>
            <title>Import session</title></head><body class='p-4'>
            <div class='container' style='max-width:560px'>
              <h1 class='h5 mb-3'>Import session (JSON)</h1>
              <div class='alert alert-info small'>Select a JSON file generated by <code>/log_download</code> (single session).</div>
              <form method='post' enctype='multipart/form-data'>
                <input class='form-control mb-2' type='file' name='file' accept='application/json,.json' required>
                <button class='btn btn-primary'>Import</button>
                <a class='btn btn-outline-secondary ms-2' href='/logs_ui'>Cancel</a>
              </form>
            </div></body></html>
            """).encode('utf-8')

        # POST -> handle upload
        up = params.get('file')
        if not up or not hasattr(up, 'file'):
            return b"<html><body><p>Missing file</p><a href='/log_import'>Back</a></body></html>"
        try:
            payload = json.load(up.file)
        except Exception:
            return b"<html><body><p>Invalid JSON</p><a href='/log_import'>Back</a></body></html>"

        sess = (payload or {}).get("session") or {}
        samples = (payload or {}).get("samples") or []

        with connect_db() as c:
            # Ensure columns exist (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_session)").fetchall()}
            if 'title' not in cols:
                c.execute("ALTER TABLE live_session ADD COLUMN title TEXT")
            cols2 = {r[1] for r in c.execute("PRAGMA table_info(live_sample)").fetchall()}
            if 'rx_percent_lost' not in cols2:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_percent_lost INTEGER")
            if 'rx_lost_nb_packets' not in cols2:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_lost_nb_packets INTEGER")

            # Insert session (new ID)
            cur = c.execute(
                "INSERT INTO live_session(device_id, device_host, input_key, input_index, input_identifier, input_display_name, started_at, ended_at, title) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    sess.get("device_id"), sess.get("device_host"), sess.get("input_key"), sess.get("input_index"),
                    sess.get("input_identifier"), sess.get("input_display_name"),
                    sess.get("started_at"), sess.get("ended_at"), sess.get("title"),
                )
            )
            new_sid = cur.lastrowid

            # Insert samples: expand per-tick links[]
            for s in samples:
                base_vals = (
                    new_sid,
                    s.get("ts"),
                    s.get("year"), s.get("month"), s.get("day"),
                    s.get("hour"), s.get("minute"), s.get("second"),
                    s.get("latitude"), s.get("longitude"),
                    s.get("drops_video"), s.get("drops_ts"),
                )
                links = s.get("links") or [None]
                if not isinstance(links, list):
                    links = [None]
                for l in links:
                    if isinstance(l, dict):
                        name = l.get("name")
                        owdR = l.get("owdR")
                        rb   = l.get("rx_bitrate")
                        rpl  = l.get("rx_percent_lost")
                        rln  = l.get("rx_lost_nb_packets")
                    else:
                        name = None; owdR = None; rb = None; rpl = None; rln = None
                    c.execute(
                        "INSERT INTO live_sample(session_id, ts, year, month, day, hour, minute, second, latitude, longitude, drops_video, drops_ts, link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        base_vals + (name, owdR, rb, rpl, rln)
                    )

            c.commit()

        raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20imported")

    @cherrypy.expose
    def metrics(self):
        import sqlite3
        lines = []
        # Poller metrics
        running = 1 if (POLLER and getattr(POLLER, '_thr', None) and POLLER._thr.is_alive()) else 0
        interval = getattr(POLLER, 'interval', 0.0) if POLLER else 0.0
        last_cycle = 0
        if POLLER and getattr(POLLER, 'last_cycle_at', 0):
            last_cycle = max(0, int(time.time() - POLLER.last_cycle_at))
        lines.append('# HELP poller_running 1 if background poller thread is running')
        lines.append('# TYPE poller_running gauge')
        lines.append(f'poller_running {running}')
        lines.append('# HELP poller_interval_seconds Poller loop interval in seconds')
        lines.append('# TYPE poller_interval_seconds gauge')
        lines.append(f'poller_interval_seconds {interval}')
        lines.append('# HELP poller_last_cycle_seconds Seconds since last poller cycle')
        lines.append('# TYPE poller_last_cycle_seconds gauge')
        lines.append(f'poller_last_cycle_seconds {last_cycle}')

        with connect_db() as c:
            devs = c.execute("SELECT id,name,host,port FROM devices ORDER BY id ASC").fetchall()
            for d in devs:
                did, name, host, port = d
                # last sample timestamp and age
                last_ts = c.execute(
                    "SELECT MAX(ts) FROM live_sample WHERE session_id IN (SELECT id FROM live_session WHERE device_host=?)",
                    (host,)
                ).fetchone()[0]
                age = 'NaN'
                if last_ts:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(str(last_ts)[:19])
                        age = max(0, int((datetime.now() - dt).total_seconds()))
                    except Exception:
                        age = 'NaN'
                active = c.execute(
                    "SELECT COUNT(*) FROM live_session WHERE device_host=? AND ended_at IS NULL",
                    (host,)
                ).fetchone()[0]
                # emit with labels
                label = f'id="{did}",host="{host}",name="{name}"'
                lines.append('# HELP device_last_sample_age_seconds Age in seconds of last sample for this device host')
                lines.append('# TYPE device_last_sample_age_seconds gauge')
                lines.append(f'device_last_sample_age_seconds{{{label}}} {age}')
                lines.append('# HELP device_active_sessions Number of active sessions for this host')
                lines.append('# TYPE device_active_sessions gauge')
                lines.append(f'device_active_sessions{{{label}}} {active}')

        out = '\n'.join(lines) + '\n'
        cherrypy.response.headers['Content-Type'] = 'text/plain; version=0.0.4; charset=utf-8'
        return out.encode('utf-8')
    @require_login
    @cherrypy.expose
    @slow(0.25)
    def health(self):
        import sqlite3, html
        now = time.time()
        # Poller status
        running = False
        interval = None
        last_cycle_secs = None
        last_error = None
        try:
            if POLLER and getattr(POLLER, '_thr', None) and POLLER._thr.is_alive():
                running = True
                interval = getattr(POLLER, 'interval', None)
                if getattr(POLLER, 'last_cycle_at', 0):
                    last_cycle_secs = max(0, int(now - POLLER.last_cycle_at))
                last_error = getattr(POLLER, 'last_error', None)
        except Exception:
            pass

        # Devices overview and last sample / active sessions
        rows = []
        with connect_db() as c:
            devs = c.execute("SELECT id,name,protocol,host,port FROM devices ORDER BY id ASC").fetchall()
            for d in devs:
                did, name, proto, host, port = d
                # last sample ts for this host
                last_ts = c.execute(
                    "SELECT MAX(ts) FROM live_sample WHERE session_id IN (SELECT id FROM live_session WHERE device_host=?)",
                    (host,)
                ).fetchone()[0]
                # active sessions count for this host
                active = c.execute(
                    "SELECT COUNT(*) FROM live_session WHERE device_host=? AND ended_at IS NULL",
                    (host,)
                ).fetchone()[0]
                # compute age in seconds if ts is an ISO string
                age_sec = None
                try:
                    if last_ts:
                        from datetime import datetime
                        dt = datetime.strptime(str(last_ts)[:19], '%Y-%m-%dT%H:%M:%S') \
                            if 'T' in str(last_ts) else \
                            datetime.strptime(str(last_ts)[:19], '%Y-%m-%d %H:%M:%S')
                        age_sec = max(0, int((datetime.utcnow() - dt).total_seconds()))
                except Exception:
                    age_sec = None
                rows.append((did, name, host, port, last_ts, active, age_sec))

        def esc(x):
            return html.escape('' if x is None else str(x))

        def age_badge(age):
            if age is None:
                return '<span class="badge text-bg-secondary">n/a</span>'
            if age <= 10:
                return f'<span class="badge text-bg-success">{age}s</span>'
            if age <= 30:
                return f'<span class="badge text-bg-warning">{age}s</span>'
            return f'<span class="badge text-bg-danger">{age}s</span>'

        def spark_svg(host, srt=False):
            # Build a tiny SVG sparkline (120x24) from POLLER/SRT_POLLER.age_history[host]
            try:
                poller = SRT_POLLER if srt else POLLER
                hist = (poller.age_history.get(host) if poller and getattr(poller, 'age_history', None) else None)
                if not hist:
                    return ''
                pts = list(hist)[-120:]  # last ~2 minutes depending on interval
                # Extract ages and map None to previous or 0 for continuity
                ages = []
                prev = 0
                for (_, a) in pts:
                    if a is None:
                        a = prev
                    else:
                        prev = a
                    ages.append(a)
                if not ages:
                    return ''
                # Define scale
                W, H, P = 120, 24, 2
                max_age = max(ages) if max(ages) > 0 else 1
                # Clip max to 60s to keep dynamic range readable
                max_age = max(10, min(60, max_age))
                # Build polyline points
                n = len(ages)
                if n == 1:
                    xs = [P, W-P]
                    ys = [H/2, H/2]
                else:
                    xs = [P + i*(W-2*P)/(n-1) for i in range(n)]
                    ys = [H-P - (min(a, max_age)/max_age)*(H-2*P) for a in ages]
                path = ' '.join(f"{int(xs[i])},{int(ys[i])}" for i in range(n))
                # color by last point (green<=10, orange<=30, red>30)
                last = ages[-1]
                color = '#198754' if last <= 10 else ('#fd7e14' if last <= 30 else '#dc3545')
                return (
                    f'<svg width="120" height="24" viewBox="0 0 120 24" xmlns="http://www.w3.org/2000/svg">'
                    f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{path}" />'
                    f'</svg>'
                )
            except Exception:
                return ''

        # --- Database state ---
        import os
        db_path = str(DB_PATH)
        try:
            db_size_bytes = os.path.getsize(db_path)
        except Exception:
            db_size_bytes = 0
        def hsize(n):
            try:
                n = float(n)
            except Exception:
                return "n/a"
            units = ["B","KB","MB","GB","TB"]
            i = 0
            while n >= 1024 and i < len(units)-1:
                n /= 1024.0
                i += 1
            return f"{n:.1f} {units[i]}"
        sessions_total = 0
        sessions_active = 0
        samples_total = 0
        last_sample_any = None
        try:
            with connect_db(str(DB_PATH)) as _dbc:
                sessions_total = _dbc.execute("SELECT COUNT(*) FROM live_session").fetchone()[0] or 0
                sessions_active = _dbc.execute("SELECT COUNT(*) FROM live_session WHERE ended_at IS NULL").fetchone()[0] or 0
                samples_total = _dbc.execute("SELECT COUNT(*) FROM live_sample").fetchone()[0] or 0
                last_sample_any = _dbc.execute("SELECT MAX(ts) FROM live_sample").fetchone()[0]
        except Exception:
            pass

        # Build HTML
        body = [
            '<!doctype html>',
            '<html><head>',
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width,initial-scale=1">',
            '<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">',
            '<title>Health</title>',
            '</head><body class="p-3">',
            '<div class="d-flex justify-content-between align-items-center mb-3">',
            '<h1 class="h5 m-0">Health</h1>',
            '<div>',
            '<a class="btn btn-outline-secondary me-2" href="/">← Dashboard</a>',
            '<button class="btn btn-outline-primary" onclick="location.reload()">Refresh</button>',
            '</div></div>',
            '<div class="card mb-3"><div class="card-body d-flex flex-wrap gap-3">',
            f'<div><strong>Poller:</strong> ' + ("<span class=\"badge text-bg-success\">running</span>" if running else "<span class=\"badge text-bg-danger\">stopped</span>") + '</div>',
            f'<div><strong>Interval:</strong> {esc(interval)} s</div>',
            f'<div><strong>Last cycle:</strong> {esc(last_cycle_secs)} s ago</div>',
            (f'<div class="text-danger"><strong>Last error:</strong> {esc(last_error)}</div>' if last_error else ''),
            '<div class="form-check ms-auto">',
            '  <input class="form-check-input" type="checkbox" id="auto" checked>',
            '  <label class="form-check-label" for="auto">Auto-refresh (10s)</label>',
            '</div>',
            '</div></div>',
            # --- DB state card ---
            '<div class="card mb-3"><div class="card-body">',
            f'<div class="mb-1"><strong>DB file:</strong> <code>{esc(db_path)}</code></div>',
            f'<div class="mb-1"><strong>Size:</strong> {hsize(db_size_bytes)}</div>',
            '</div></div>',
            '<div class="table-responsive">',
            '<table class="table table-sm align-middle">',
            '<thead><tr>',
            '<th>Type</th><th>Name</th><th>Host</th><th>Port</th><th>Last sample ts</th><th>Age</th><th>Spark</th><th>Active sessions</th><th>Actions</th>',
            '</tr></thead><tbody>'
        ]
        if rows:
            for (did, name, host, port, last_ts, active, age_sec) in rows:
                body.append('<tr>')
                body.append(f'<td><span class="badge text-bg-secondary">StreamHub</span></td>')
                body.append(f'<td>{esc(name)}</td>')
                body.append(f'<td>{esc(host)}</td>')
                body.append(f'<td>{esc(port)}</td>')
                body.append(f'<td>{esc(last_ts)}</td>')
                body.append(f'<td>{age_badge(age_sec)}</td>')
                body.append(f'<td>{spark_svg(host)}</td>')
                body.append(f'<td>{esc(active)}</td>')
                body.append('<td>')
                body.append(f'<a class="btn btn-sm btn-outline-primary" href="/data?id={did}">Ping /data</a>')
                body.append('</td>')
                body.append('</tr>')
        else:
            body.append('<tr><td colspan="9" class="text-muted">No StreamHub devices</td></tr>')
        # SRT Gateway rows
        with connect_db() as _cgw:
            gw_devs = _cgw.execute(
                'SELECT id, name, host, port FROM srt_gateway_device ORDER BY id ASC'
            ).fetchall()
        for gw in gw_devs:
            gwid, gwname, gwhost, gwport = gw
            # Last sample ts for this gateway
            gw_last_ts = None
            gw_running_routes = 0
            try:
                with connect_db() as _cg:
                    r_ts = _cg.execute(
                        'SELECT MAX(ts) FROM srt_route_sample WHERE device_host=?', (gwhost,)
                    ).fetchone()[0]
                    gw_last_ts = r_ts
                    gw_running_routes = (_cg.execute(
                        'SELECT COUNT(DISTINCT route_id) FROM srt_session '
                        'WHERE device_host=? AND ended_at IS NULL', (gwhost,)
                    ).fetchone()[0] or 0)
            except Exception:
                pass
            gw_age_sec = None
            try:
                if gw_last_ts:
                    from datetime import datetime as _dth
                    dt = _dth.strptime(str(gw_last_ts)[:19], "%Y-%m-%d %H:%M:%S")
                    gw_age_sec = max(0, int((_dth.utcnow() - dt).total_seconds()))
            except Exception:
                pass
            # Poller status for SRT Gateway
            gw_poller_ok = bool(SRT_POLLER and getattr(SRT_POLLER, '_thr', None) and SRT_POLLER._thr.is_alive())
            gw_last_payload = (SRT_POLLER.last_payloads.get(gwhost) or {}) if SRT_POLLER else {}
            body.append('<tr>')
            body.append('<td><span class="badge text-bg-primary">SRT Gateway</span></td>')
            body.append(f'<td>{esc(gwname)}</td>')
            body.append(f'<td>{esc(gwhost)}</td>')
            body.append(f'<td>{esc(gwport)}</td>')
            body.append(f'<td>{esc(gw_last_ts)}</td>')
            body.append(f'<td>{age_badge(gw_age_sec)}</td>')
            body.append(f'<td>{spark_svg(gwhost, srt=True)}</td>')
            body.append(f'<td>{esc(gw_running_routes)} route(s) running</td>')
            body.append('<td>')
            body.append(f'<a class="btn btn-sm btn-outline-primary" href="/gateway_data_raw?host={gwhost}">Ping /data</a>')
            body.append('</td>')
            body.append('</tr>')
        body.extend(['</tbody></table></div>',
                     '<script>\n(function(){\n  var t=null;\n  function tick(){ if(document.getElementById(\'auto\').checked){ location.reload(); } }\n  t=setInterval(tick,10000);\n})();\n</script>',
                     '</body></html>'])

        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return '\n'.join(body).encode('utf-8')

    @require_login
    @cherrypy.expose
    @slow(0.25)
    def health_json(self):
        import sqlite3, json
        now = time.time()
        running = bool(POLLER and getattr(POLLER, '_thr', None) and POLLER._thr.is_alive())
        interval = getattr(POLLER, 'interval', None) if POLLER else None
        last_cycle_secs = None
        last_error = getattr(POLLER, 'last_error', None) if POLLER else None
        if POLLER and getattr(POLLER, 'last_cycle_at', 0):
            last_cycle_secs = max(0, int(now - POLLER.last_cycle_at))
        devices = []
        with connect_db() as c:
            devs = c.execute("SELECT id,name,protocol,host,port FROM devices ORDER BY id ASC").fetchall()
            for d in devs:
                did, name, proto, host, port = d
                last_ts = c.execute(
                    "SELECT MAX(ts) FROM live_sample WHERE session_id IN (SELECT id FROM live_session WHERE device_host=?)",
                    (host,)
                ).fetchone()[0]
                active = c.execute(
                    "SELECT COUNT(*) FROM live_session WHERE device_host=? AND ended_at IS NULL",
                    (host,)
                ).fetchone()[0]
                devices.append({
                    "id": did, "name": name, "host": host, "port": port,
                    "last_sample_ts": last_ts, "active_sessions": active
                })
        cherrypy.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return json.dumps({
            "poller": {
                "running": running,
                "interval": interval,
                "last_cycle_secs": last_cycle_secs,
                "last_error": last_error
            },
            "devices": devices
        }, ensure_ascii=False).encode('utf-8')
    @require_login
    @cherrypy.expose
    @slow(0.25)
    def index(self):
        with connect_db() as db:
            cur = db.execute("SELECT id,name,protocol,host,port,api_path,token FROM devices ORDER BY id DESC")
            cols = [c[0] for c in cur.description]
            devices = [dict(zip(cols, r)) for r in cur.fetchall()]
        return render("dashboard.html", devices=devices, page_title="Dashboard", user=current_user())

    @require_login
    @cherrypy.expose
    def devices(self):
        with connect_db() as db:
            cur = db.execute("SELECT id,name,protocol,host,port,api_path,token FROM devices ORDER BY id DESC")
            cols = [c[0] for c in cur.description]
            devices = [dict(zip(cols, r)) for r in cur.fetchall()]
            gw_cur = db.execute("SELECT id,name,host,port,protocol FROM srt_gateway_device ORDER BY id DESC")
            srt_gateways = [dict(zip([c[0] for c in gw_cur.description], r)) for r in gw_cur.fetchall()]
        return render("devices.html", devices=devices, srt_gateways=srt_gateways,
                      page_title="Devices", user=current_user())

    @require_login
    @cherrypy.expose
    def devices_add(self, name="StreamHub", protocol="https", host="", port="443", api_path="/rest-api/", token=""):
        name = (name or "StreamHub").strip() or "StreamHub"
        host = (host or "").strip()
        api_path = (api_path or "/rest-api/").strip() or "/rest-api/"
        try:
            port = int(port or 443)
        except Exception:
            port = 443
        if not host:
            raise cherrypy.HTTPRedirect("/devices")
        with connect_db() as db:
            # Enforce a max number of StreamHub instances based on MAX_STREAMHUB env (default 1)
            limit_env = os.getenv('MAX_STREAMHUB')
            try:
                limit = int(limit_env) if (limit_env is not None and str(limit_env).strip()) else 1
            except Exception:
                limit = 1
            if limit < 1:
                limit = 1
            cur = db.execute("SELECT COUNT(*) FROM devices")
            cnt = cur.fetchone()[0] or 0
            if cnt >= limit:
                raise cherrypy.HTTPRedirect(f"/devices?msg=Max%20{limit}%20StreamHub(s)%20can%20be%20configured")
            db.execute(
                "INSERT INTO devices(name,protocol,host,port,api_path,token) VALUES(?,?,?,?,?,?)",
                (name, protocol, host, port, api_path, token)
            )
        raise cherrypy.HTTPRedirect("/devices")

    @require_login
    @cherrypy.expose
    def devices_delete(self, id=None):
        if id:
            with connect_db() as db:
                db.execute("DELETE FROM devices WHERE id=?", (id,))
        raise cherrypy.HTTPRedirect("/devices")

    @require_login
    @cherrypy.expose
    @slow(0.25)
    def data(self, id=None, session_id=None, host=None):
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        # Accept either device id (id), a session_id (resolve to device host -> device id), or a host name directly
        dev_id = None
        if id not in (None, ""):
            dev_id = id
        elif session_id not in (None, ""):
            try:
                sid = int(session_id)
            except Exception:
                return b'{"ok": false, "error": "bad session_id"}'
            with connect_db() as db:
                row = db.execute("SELECT device_host FROM live_session WHERE id=?", (sid,)).fetchone()
            if not row:
                return b'{"ok": false, "error": "session_not_found"}'
            s_host = row[0]
            with connect_db() as db:
                r2 = db.execute("SELECT id FROM devices WHERE host=? LIMIT 1", (s_host,)).fetchone()
            if not r2:
                return b'{"ok": false, "error": "device_not_found_for_session_host"}'
            dev_id = str(r2[0])
        elif host not in (None, ""):
            with connect_db() as db:
                r3 = db.execute("SELECT id FROM devices WHERE host=? LIMIT 1", (host,)).fetchone()
            if not r3:
                return b'{"ok": false, "error": "device_not_found_for_host"}'
            dev_id = str(r3[0])
        else:
            return b'{"ok": false, "error": "missing id"}'
        with connect_db() as db:
            row = db.execute("SELECT id,name,protocol,host,port,api_path,token FROM devices WHERE id=?", (dev_id,)).fetchone()
        if not row:
            return b'{"ok": false, "error": "not found"}'
        keys = ["id","name","protocol","host","port","api_path","token"]
        d = dict(zip(keys, row))

        # --- PATCH ports API par défaut StreamHub ---
        # L’API StreamHub écoute sur 8893 (HTTP) et 8896 (HTTPS).
        # Si l’utilisateur a mis 80/443 (ou 0), on force le port API attendu.
        try:
            p = int(d.get("port") or 0)
        except Exception:
            p = 0
        if d.get("protocol") == "http"  and (p in (0, 80, 443)): p = 8893
        if d.get("protocol") == "https" and (p in (0, 80, 443)): p = 8896
        d["port"] = p
        # --------------------------------------------

        # Pour StreamHub “ancien probe”, on tape la racine + ?api_key=
        base = f"{d['protocol']}://{d['host']}:{d['port']}"
        ok, payload = fetch_streamhub(base, d.get("token") or None, timeout=5)
        import json
        
        # Observe payload for live logging
        try:
            device_id = (payload.get('characteristics') or {}).get('identifier') \
                or (payload.get('configuration') or {}).get('device', {}).get('Identifier') \
                or str(d['id'])
            device_host = d['host']
            LOGGER.observe_payload(device_id, device_host, payload)
        except Exception as _obs_err:
            cherrypy.log(f"observe_payload error: {_obs_err}")
        return json.dumps({"ok": ok, "payload": payload, "timestamp": int(time.time()*1000)}).encode("utf-8")



    @require_login
    @cherrypy.expose
    @slow(0.25)
    def logs(self):
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        import sqlite3, json
        with connect_db() as c:
            # Ensure 'title' column exists (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_session)").fetchall()}
            if 'title' not in cols:
                c.execute("ALTER TABLE live_session ADD COLUMN title TEXT")
            rows = c.execute("""
                SELECT id, device_id, device_host, input_key, input_index,
                       input_identifier, input_display_name, started_at, ended_at, title
                FROM live_session ORDER BY id DESC LIMIT 200
            """).fetchall()
            out = [{
                "id": r[0], "device_id": r[1], "device_host": r[2], "input_key": r[3], "input_index": r[4],
                "input_identifier": r[5], "input_display_name": r[6], "started_at": r[7], "ended_at": r[8],
                "title": r[9]
            } for r in rows]
        return json.dumps({"ok": True, "sessions": out}).encode("utf-8")

    @require_login
    @cherrypy.expose
    def log_download(self, session_id=None):
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not session_id:
            return b'{"ok": false, "error":"missing session_id"}'
        try:
            sid = int(session_id)
        except Exception:
            return b'{"ok": false, "error":"bad session_id"}'
        import sqlite3, json
        with connect_db() as c:
            # Ensure 'title' column exists (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_session)").fetchall()}
            if 'title' not in cols:
                c.execute("ALTER TABLE live_session ADD COLUMN title TEXT")
            # Ensure new columns exist (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_sample)").fetchall()}
            if 'rx_percent_lost' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_percent_lost INTEGER")
            if 'rx_lost_nb_packets' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_lost_nb_packets INTEGER")
            s = c.execute("SELECT id, device_id, device_host, input_key, input_index, input_identifier, input_display_name, started_at, ended_at, title FROM live_session WHERE id=?", (sid,)).fetchone()
            if not s:
                return b'{"ok": false, "error":"session_not_found"}'
            rows = c.execute("""
                SELECT ts, year, month, day, hour, minute, second,
                       latitude, longitude, drops_video, drops_ts,
                       link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets
                FROM live_sample WHERE session_id=? ORDER BY id ASC
            """, (sid,)).fetchall()
        payload = {
            "session": {
                "id": s[0], "device_id": s[1], "device_host": s[2], "input_key": s[3], "input_index": s[4],
                "input_identifier": s[5], "input_display_name": s[6], "started_at": s[7], "ended_at": s[8], "title": s[9]
            },
            "samples": []
        }
        # Group rows by timestamp so lat/lng and drops appear once per tick, with all links listed under that tick.
        by_ts = {}
        for r in rows:
            ts, year, month, day, hour, minute, second, lat, lng, dv, dt, link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets = r
            if ts not in by_ts:
                by_ts[ts] = {
                    "ts": ts,
                    "year": year, "month": month, "day": day,
                    "hour": hour, "minute": minute, "second": second,
                    "latitude": lat, "longitude": lng,
                    "drops_video": dv, "drops_ts": dt,
                    "links": []
                }
            else:
                if by_ts[ts]["latitude"] is None and lat is not None:
                    by_ts[ts]["latitude"] = lat
                if by_ts[ts]["longitude"] is None and lng is not None:
                    by_ts[ts]["longitude"] = lng
                if dv is not None:
                    by_ts[ts]["drops_video"] = dv
                if dt is not None:
                    by_ts[ts]["drops_ts"] = dt
            by_ts[ts]["links"].append({
                "name": link_name,
                "owdR": owdR,
                "rx_bitrate": rx_bitrate,
                "rx_percent_lost": rx_percent_lost,
                "rx_lost_nb_packets": rx_lost_nb_packets,
            })
        payload["samples"] = [by_ts[k] for k in sorted(by_ts.keys())]
        try:
            ts_sc = s[7][:19].replace("T"," ") if s[7] else ""
            ts_ec = s[8][:19].replace("T"," ") if s[8] else ""
            with connect_db() as c2:
                ev_r = c2.execute(
                    "SELECT ts,node,level,message FROM streamhub_log "
                    "WHERE device_host=? AND ts>=?" + (" AND ts<=?" if ts_ec else "") + " ORDER BY ts ASC",
                    (s[2], ts_sc) + ((ts_ec,) if ts_ec else ())
                ).fetchall() if ts_sc else []
            ev_r = _filter_logs_for_input(ev_r, s[4], s[5], device_id=s[2])
            payload["events"] = [{"ts":r[0],"node":r[1],"level":r[2],"message":r[3]} for r in ev_r]
        except Exception:
            payload["events"] = []
        data = json.dumps(payload, ensure_ascii=False, separators=(',',':'))
        cherrypy.response.headers['Content-Disposition'] = f'attachment; filename=session_{sid}.json'
        return data.encode("utf-8")



    @require_login
    @cherrypy.expose
    def logs_ui(self, msg=None):
        # Simple HTML page to list sessions and offer downloads/purge
        import sqlite3, html
        with connect_db() as c:
            # Ensure 'title' column exists (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_session)").fetchall()}
            if 'title' not in cols:
                c.execute("ALTER TABLE live_session ADD COLUMN title TEXT")
            rows = c.execute("""
                SELECT id, device_id, device_host, input_key, input_index,
                       input_identifier, input_display_name, started_at, ended_at, title
                FROM live_session ORDER BY id DESC LIMIT 500
            """).fetchall()

        def esc(s):
            return html.escape("" if s is None else str(s))

        def _parse_iso(x):
            try:
                from datetime import datetime
                return datetime.fromisoformat(str(x)[:19]) if x else None
            except Exception:
                return None

        def _fmt_dur(total_seconds):
            try:
                s = int(total_seconds)
            except Exception:
                return ''
            sign = '-' if s < 0 else ''
            s = abs(s)
            d, r = divmod(s, 86400)
            h, r = divmod(r, 3600)
            m, s = divmod(r, 60)
            hhmmss = f"{h:02d}:{m:02d}:{s:02d}"
            return f"{sign}{d}d {hhmmss}" if d else f"{sign}{hhmmss}"

        def _fmt_ts(x):
            try:
                dt = _parse_iso(x)
                if dt:
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            s = '' if x is None else str(x)
            return s[:19].replace('T', ' ')

        def _day_label(ts):
            dt = _parse_iso(ts)
            return dt.date().isoformat() if dt else 'Unknown date'

        items = []
        current_day = None
        for r in rows:
            sid, dev_id, dev_host, key, idx, ident, disp, start, end, title = r

            # insert day divider when day changes (based on Started)
            day_lbl = _day_label(start)
            if day_lbl != current_day:
                items.append(
                    f'<tr><td colspan="11" class="p-0">'
                    f'<div class="bd-callout bd-callout-info w-100 mb-2" role="separator">{esc(day_lbl)}</div>'
                    f'</td></tr>'
                )
                current_day = day_lbl

            # compute duration
            dur_txt = ''
            try:
                st_dt = _parse_iso(start)
                if st_dt is not None:
                    from datetime import datetime
                    et_dt = _parse_iso(end) if end else None
                    ref = et_dt or datetime.now()
                    dur_txt = _fmt_dur((ref - st_dt).total_seconds())
            except Exception:
                dur_txt = ''

            # Safe HTML for Stop button (only if session is still open)
            stop_btn = ''
            if not end:
                stop_btn = (
                    '<form class="d-inline ms-1" method="post" action="/log_stop" '
                    'onsubmit="return confirm(\'Stop this session now?\');">'
                    '<input type="hidden" name="session_id" value="' + str(sid) + '">'
                    '<button class="btn btn-sm btn-warning" type="submit">Stop</button>'
                    '</form>'
                )

            items.append(f"""
            <tr>
              <td>{sid}</td>
              <td>{esc(dev_id)}</td>
              <td>{esc(dev_host)}</td>
              <td>{esc(idx)}</td>
              <td>{esc(ident)}</td>
              <td>{esc(disp)}</td>
              <td>{esc(_fmt_ts(start))}</td>
              <td>{esc(dur_txt)}</td>
              <td>
                <form class="d-flex" method="post" action="/log_rename">
                  <input type="hidden" name="session_id" value="{sid}">
                  <input class="form-control form-control-sm me-1" type="text" name="title" placeholder="Session name" value="{esc(title)}" />
                  <button class="btn btn-sm btn-outline-success" type="submit">Save</button>
                </form>
              </td>
              <td>{esc(_fmt_ts(end))}</td>
              <td>
                <a class="btn btn-sm btn-outline-primary" href="/log_download?session_id={sid}" download="session_{sid}.json">JSON</a>
                <a class="btn btn-sm btn-outline-secondary ms-1" href="/log_download_csv?session_id={sid}">CSV</a>
                <a class="btn btn-sm btn-outline-success ms-1" href="/log_download_geojson?session_id={sid}">GeoJSON</a>
                {'<a class="btn btn-sm btn-outline-danger ms-1" href="/log_pdf?session_id='+str(sid)+'">PDF</a>' if end else ''}
                <a class="btn btn-sm btn-outline-dark ms-1" href="/log_view?session_id={sid}">View</a>
                {stop_btn}
                <form class="d-inline ms-1" method="post" action="/log_delete" onsubmit="return confirm('Delete this session?');">
                  <input type="hidden" name="session_id" value="{sid}">
                  <button class="btn btn-sm btn-danger" type="submit">Delete</button>
                </form>
              </td>
            </tr>""")

        body = f"""
        <!doctype html>
        <html><head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width,initial-scale=1">
          <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
          <style>
            /* Lightweight callout styles for day separators */
            .bd-callout{{padding:.75rem 1rem;border:1px solid var(--bs-border-color);border-left-width:.25rem;border-radius:.25rem;background-color:var(--bs-body-bg);}} 
            .bd-callout-info{{border-left-color:#0d6efd;background:rgba(13,110,253,.06);}} /* light primary */
          </style>
          <title>Live sessions</title>
        </head>
        <body class="p-3">
          <div class="d-flex justify-content-between align-items-center mb-3">
            <h1 class="h4 m-0">Live sessions</h1>
            <div>
              <a class="btn btn-outline-secondary me-2" href="/">← Back to dashboard</a>
              <a class="btn btn-outline-primary me-2" href="/log_import">Import session</a>
              <form class="d-inline" method="post" action="/log_purge" onsubmit="return confirm('Purge all logs? This cannot be undone.');">
                <button class="btn btn-danger">Purge all</button>
              </form>
            </div>
          </div>
          {('<div class="alert alert-info">'+esc(msg)+'</div>' if msg else '')}
          <div class="table-responsive">
            <table class="table table-sm align-middle">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Device</th>
                  <th>Host</th>
                  <th>Input</th>
                  <th>Identifier</th>
                  <th>Display name</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th>Title</th>
                  <th>Ended</th>
                  <th>Download</th>
                </tr>
              </thead>
              <tbody>
                {''.join(items) if items else '<tr><td colspan="11" class="text-muted">No sessions</td></tr>'}
              </tbody>
            </table>
          </div>
        </body></html>
        """
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return body.encode('utf-8')

    @require_login
    @cherrypy.expose
    def log_download_csv(self, session_id=None):
        import sqlite3, csv, io, json
        cherrypy.response.headers["Content-Type"] = "text/csv; charset=utf-8"
        if not session_id:
            return b'error, missing session_id'
        try:
            sid = int(session_id)
        except Exception:
            return b'error, bad session_id'
        with connect_db() as c:
            # Ensure new columns exist (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_sample)").fetchall()}
            if 'rx_percent_lost' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_percent_lost INTEGER")
            if 'rx_lost_nb_packets' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_lost_nb_packets INTEGER")
            rows = c.execute("""
                SELECT ts, year, month, day, hour, minute, second,
                       latitude, longitude, drops_video, drops_ts,
                       link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets
                FROM live_sample WHERE session_id=? ORDER BY id ASC
            """, (sid,)).fetchall()
        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["ts","year","month","day","hour","minute","second","latitude","longitude","drops_video","drops_ts","link_name","owdR","rx_bitrate","rx_percent_lost","rx_lost_nb_packets"])
        for r in rows:
            w.writerow(r)
        data = output.getvalue()
        cherrypy.response.headers['Content-Disposition'] = f'attachment; filename=session_{sid}.csv'
        return data.encode('utf-8')

    @require_login
    @cherrypy.expose
    def log_download_geojson(self, session_id=None):
        import sqlite3, json
        cherrypy.response.headers["Content-Type"] = "application/geo+json; charset=utf-8"
        if not session_id:
            return b'{"error":"missing session_id"}'
        try:
            sid = int(session_id)
        except Exception:
            return b'{"error":"bad session_id"}'

        with connect_db() as c:
            # Ensure columns exist (migration-safe)
            cols = {r[1] for r in c.execute("PRAGMA table_info(live_sample)").fetchall()}
            if 'rx_percent_lost' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_percent_lost INTEGER")
            if 'rx_lost_nb_packets' not in cols:
                c.execute("ALTER TABLE live_sample ADD COLUMN rx_lost_nb_packets INTEGER")

            s = c.execute(
                "SELECT id, device_id, device_host, input_key, input_index, input_identifier, input_display_name, started_at, ended_at, title "
                "FROM live_session WHERE id=?",
                (sid,)
            ).fetchone()
            if not s:
                return b'{"error":"session_not_found"}'

            rows = c.execute(
                """
                SELECT ts, year, month, day, hour, minute, second,
                       latitude, longitude, drops_video, drops_ts,
                       link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets
                FROM live_sample WHERE session_id=? ORDER BY id ASC
                """,
                (sid,)
            ).fetchall()

        # Group by timestamp (tick)
        by_ts = {}
        for r in rows:
            (ts, year, month, day, hour, minute, second,
             lat, lng, dv, dt,
             link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets) = r
            if ts not in by_ts:
                by_ts[ts] = {
                    "ts": ts,
                    "year": year, "month": month, "day": day,
                    "hour": hour, "minute": minute, "second": second,
                    "latitude": lat, "longitude": lng,
                    "drops_video": dv, "drops_ts": dt,
                    "links": []
                }
            else:
                # prefer non-null GPS if some rows have it per tick
                if by_ts[ts]["latitude"] is None and lat is not None:
                    by_ts[ts]["latitude"] = lat
                if by_ts[ts]["longitude"] is None and lng is not None:
                    by_ts[ts]["longitude"] = lng
                if dv is not None:
                    by_ts[ts]["drops_video"] = dv
                if dt is not None:
                    by_ts[ts]["drops_ts"] = dt
            by_ts[ts]["links"].append({
                "name": link_name,
                "owdR": owdR,
                "rx_bitrate": rx_bitrate,
                "rx_percent_lost": rx_percent_lost,
                "rx_lost_nb_packets": rx_lost_nb_packets,
            })

        # Build FeatureCollection
        def worst_quality(links):
            # Good if owd<100; Fair if 100<owd<200; Poor if owd>=200; default poor if unknown
            q = 'good'
            for l in links:
                owd = l.get('owdR')
                if isinstance(owd, (int, float)):
                    if owd >= 200:
                        return 'poor'
                    if 100 < owd < 200 and q == 'good':
                        q = 'fair'
                else:
                    # keep as is; unknown OWD will not upgrade beyond current q
                    pass
            return q

        # Main path coordinates (LineString): [lng, lat]
        ordered_ticks = [by_ts[k] for k in sorted(by_ts.keys())]
        path_coords = []
        for t in ordered_ticks:
            lat, lng = t.get('latitude'), t.get('longitude')
            if lat is not None and lng is not None:
                path_coords.append([float(lng), float(lat)])

        features = []
        # 1) Main LineString path (if any coordinate present)
        if path_coords:
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": path_coords},
                "properties": {
                    "type": "path",
                    "session_id": s[0],
                    "device_id": s[1],
                    "device_host": s[2],
                    "input_key": s[3],
                    "input_index": s[4],
                    "input_identifier": s[5],
                    "input_display_name": s[6],
                    "started_at": s[7],
                    "ended_at": s[8],
                    "title": s[9]
                }
            })

        # 2) Per-tick points with full attributes (including links)
        for t in ordered_ticks:
            lat, lng = t.get('latitude'), t.get('longitude')
            geom = None
            if lat is not None and lng is not None:
                geom = {"type": "Point", "coordinates": [float(lng), float(lat)]}
            else:
                # If no GPS at this tick, still emit a Point with null geometry? GeoJSON requires geometry or null.
                geom = None

            props = {
                "type": "sample",
                "ts": t.get('ts'),
                "year": t.get('year'), "month": t.get('month'), "day": t.get('day'),
                "hour": t.get('hour'), "minute": t.get('minute'), "second": t.get('second'),
                "drops_video": t.get('drops_video'),
                "drops_ts": t.get('drops_ts'),
                "worst_quality": worst_quality(t.get('links') or []),
                "links": t.get('links') or []
            }
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props
            })

        fc = {"type": "FeatureCollection", "features": features}
        data = json.dumps(fc, ensure_ascii=False, separators=(',',':'))
        cherrypy.response.headers['Content-Disposition'] = f'attachment; filename=session_{sid}.geojson'
        return data.encode('utf-8')
    
    @require_login
    @cherrypy.expose
    @slow(0.25)
    def log_view(self, session_id=None):
        import sqlite3, html, json
        if not session_id:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Missing%20session_id")
        try:
            sid = int(session_id)
        except Exception:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Bad%20session_id")

        # Ensure session exists and get its title/device for the header
        with connect_db() as c:
            row = c.execute("SELECT id, device_id, device_host, input_display_name, started_at, ended_at, title FROM live_session WHERE id=?", (sid,)).fetchone()
        if not row:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20not%20found")
        s_id, s_dev, s_host, s_disp, s_start, s_end, s_title = row

        # Resolve devices.id from host to allow /data ping (keeps LOGGER.observe_payload alive during follow-live)
        dev_row_id = None
        try:
            with connect_db() as dconn:
                drow = dconn.execute("SELECT id FROM devices WHERE host=? LIMIT 1", (s_host,)).fetchone()
                if drow:
                    dev_row_id = int(drow[0])
        except Exception:
            dev_row_id = None

        def esc(x):
            return html.escape("" if x is None else str(x))

        def _parse_iso(x):
            try:
                from datetime import datetime
                return datetime.fromisoformat(str(x)[:19]) if x else None
            except Exception:
                return None

        def _fmt_ts(x):
            dt = _parse_iso(x)
            if dt:
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            s = '' if x is None else str(x)
            return s[:19].replace('T',' ')

        page_title = f"Session #{s_id} — {s_title or (s_disp or s_dev) or ''}"

        import json as _json
        _t_end_js = ("new Date(" + _json.dumps(str(s_end)) + ").getTime()") if s_end else "Date.now()"
        _events_js_tpl = '(function(){\nvar SESSION_ID=__SID__;\nvar T_START=new Date(__TSTART__).getTime();\nvar T_END=__TEND__;\nvar NL=String.fromCharCode(10);\nvar LEVEL_COLOR={ERROR:"#dc3545",WARNING:"#fd7e14",WARN:"#fd7e14",INFO:"#0d6efd",DEBUG:"#6c757d"};\nvar LEVEL_BADGE={ERROR:"<span class=\'badge\' style=\'background:#dc3545\'>ERROR</span>",WARNING:"<span class=\'badge\' style=\'background:#fd7e14\'>WARN</span>",INFO:"<span class=\'badge\' style=\'background:#0d6efd\'>INFO</span>",DEBUG:"<span class=\'badge\' style=\'background:#6c757d\'>DEBUG</span>"};\nvar tip=document.createElement("div");\ntip.style.cssText="position:fixed;background:#212529;color:#fff;padding:5px 10px;border-radius:5px;font-size:11px;pointer-events:none;display:none;max-width:480px;z-index:9999;white-space:pre-wrap;";\ndocument.body.appendChild(tip);\nfunction renderEvents(evs){\nvar el=document.getElementById("eventsTimeline");\nvar listWrap=document.getElementById("eventsListWrap");\nvar tbody=document.getElementById("eventsList");\nif(!evs||!evs.length){el.innerHTML="<span class=\'text-muted small\'>Aucun event trouv\\u00e9 pour cette session.</span>";listWrap.style.display="none";return;}\nvar W=1000,H=44,cy=H/2,tEnd=T_END;if(tEnd<=T_START)tEnd=T_START+1;\nvar dur=tEnd-T_START||1;\nvar svg=\'<svg width="100%" height="\'+H+\'" viewBox="0 0 \'+W+\' \'+H+\'" xmlns="http://www.w3.org/2000/svg" style="display:block">\';\nsvg+=\'<rect x="0" y="\'+(cy-2)+\'" width="\'+W+\'" height="4" fill="#dee2e6" rx="2"/>\';\nevs.forEach(function(ev,i){\nvar t=new Date(ev.ts).getTime();\nvar x=Math.max(6,Math.min(W-6,Math.round((t-T_START)/dur*W)));\nvar col=LEVEL_COLOR[ev.level]||"#0d6efd";\nvar msg=(ev.message||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/"/g,"&quot;");\nsvg+=\'<line x1="\'+x+\'" y1="4" x2="\'+x+\'" y2="\'+(H-4)+\'" stroke="\'+col+\'" stroke-width="1.5" opacity="0.4"/>\';\nsvg+=\'<circle cx="\'+x+\'" cy="\'+cy+\'" r="5" fill="\'+col+\'" data-idx="\'+i+\'" data-ts="\'+ev.ts+\'" data-lvl="\'+(ev.level||"")+\'" data-msg="\'+msg+\'" style="cursor:pointer"/>\';\n});\nsvg+=\'</svg>\';el.innerHTML=svg;\nvar rows="";\nevs.forEach(function(ev,i){\nvar badge=LEVEL_BADGE[ev.level]||LEVEL_BADGE["INFO"];\nvar msg=(ev.message||"").replace(/&/g,"&amp;").replace(/</g,"&lt;");\nrows+=\'<tr data-idx="\'+i+\'" style="cursor:pointer"><td class="text-muted">\'+ev.ts+\'</td><td>\'+badge+\'</td><td>\'+msg+\'</td></tr>\';\n});\ntbody.innerHTML=rows;listWrap.style.display="block";\nfunction highlightDot(idx,on){var c=el.querySelector(\'circle[data-idx="\'+idx+\'"]\');if(!c)return;c.setAttribute("r",on?"9":"5");c.setAttribute("stroke",on?"#fff":"none");c.setAttribute("stroke-width",on?"2":"0");}\nfunction highlightRow(idx,on){var tr=tbody.querySelector(\'tr[data-idx="\'+idx+\'"]\');if(!tr)return;tr.style.background=on?"#e8f4fd":"";if(on)tr.scrollIntoView({block:"nearest",behavior:"smooth"});}\ntbody.querySelectorAll("tr").forEach(function(tr){var idx=tr.dataset.idx;tr.addEventListener("mouseenter",function(){highlightDot(idx,true);});tr.addEventListener("mouseleave",function(){highlightDot(idx,false);});});\nel.querySelectorAll("circle").forEach(function(c){\nc.addEventListener("mouseenter",function(){highlightDot(c.dataset.idx,true);highlightRow(c.dataset.idx,true);tip.textContent=c.dataset.ts+"  ["+c.dataset.lvl+"]"+NL+c.dataset.msg;tip.style.display="block";});\nc.addEventListener("mousemove",function(e){tip.style.left=(e.clientX+14)+"px";tip.style.top=(e.clientY-10)+"px";});\nc.addEventListener("mouseleave",function(){highlightDot(c.dataset.idx,false);highlightRow(c.dataset.idx,false);tip.style.display="none";});\n});}\nfunction fetchEvents(){fetch("/session_events?session_id="+SESSION_ID).then(function(r){return r.json();}).then(function(data){renderEvents(data.ok?data.events:[]);}).catch(function(){document.getElementById("eventsTimeline").innerHTML="<span class=\'text-muted small\'>Erreur chargement events.</span>";});}\nwindow.refreshEvents=fetchEvents;fetchEvents();\n})();'
        _events_js = (_events_js_tpl
            .replace('__SID__', str(s_id))
            .replace('__TSTART__', _json.dumps(str(s_start or '')))
            .replace('__TEND__', _t_end_js)
        )
        _events_html_tpl = '<div class="mt-4"><div class="fw-semibold small mb-1">Events <span class="text-muted fw-normal">(logs StreamHub)</span></div><div id="eventsTimeline" style="min-height:44px;border:1px solid #dee2e6;border-radius:4px;padding:4px 8px;"><span class="text-muted small">Chargement…</span></div><div id="eventsListWrap" class="mt-2" style="display:none;"><table class="table table-sm table-hover mb-0" style="font-size:0.78rem;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;border:1px solid #dee2e6;border-radius:4px;"><thead class="table-light"><tr><th style="width:160px">Timestamp</th><th style="width:70px">Level</th><th>Message</th></tr></thead><tbody id="eventsList"></tbody></table></div></div>'
        _events_timeline_html = _events_html_tpl + '<script>' + _events_js + '</script>'

        esc_page = esc(page_title)
        esc_start = esc(_fmt_ts(s_start))
        esc_end = (esc(_fmt_ts(s_end)) if s_end else 'live')

        body = """
        <!doctype html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width,initial-scale=1">
            <title>""" + esc_page + """</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
            <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
            <style>
              #map { height: 70vh; }
              .badge-link { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
              .legend-box span { display:inline-block; width:12px; height:12px; border-radius:2px; margin-right:6px; }
              .dash-swatch { display:inline-block; width:24px; height:0; border-top:3px dashed #ff0000; margin-right:6px; vertical-align:middle; }
            </style>
        </head>
        <body class="p-3">
            <div class="d-flex align-items-center justify-content-between mb-2">
              <div>
                <h1 class="h5 m-0">""" + esc_page + """</h1>
                <div class="text-muted small">Started: """ + esc_start + """ — Ended: """ + (esc_end or 'live') + """</div>
              </div>
              <div>
                <a class="btn btn-outline-secondary me-2" href="/logs_ui">← Sessions</a>
                <a class="btn btn-outline-primary" href="/log_download?session_id=""" + str(s_id) + """" download="session_""" + str(s_id) + """.json">Download JSON</a>
              </div>
            </div>

<div id="map" class="mb-3 border rounded"></div>

            <div class="row g-3">
              <div class="col-md-8">
                <label class="form-label">Timeline</label>
                <input id="scrub" type="range" class="form-range" min="0" max="0" step="1" value="0">
                <div class="d-flex justify-content-between align-items-center small">
                  <div id="tsStart" class="text-muted"></div>
                  <div class="fw-bold"><span id="tsNow"></span> <span id="gpsWarn" class="ms-2"></span></div>
                  <div id="tsEnd" class="text-muted"></div>
                </div>
              </div>
              <div class="col-md-4">
                <div class="row g-2 justify-content-end">
                  <div class="col-6 text-end">
                    <div class="legend-box small d-inline-block text-start">
                      <div class="form-check form-check-sm mb-1">
                        <input class="form-check-input" type="checkbox" id="chkGood" checked>
                        <label class="form-check-label"><span style="background:#ff00ff"></span> Good</label>
                      </div>
                      <div class="form-check form-check-sm mb-1">
                        <input class="form-check-input" type="checkbox" id="chkFair" checked>
                        <label class="form-check-label"><span style="background:#ff7f00"></span> Fair</label>
                      </div>
                      <div class="form-check form-check-sm mb-1">
                        <input class="form-check-input" type="checkbox" id="chkPoor" checked>
                        <label class="form-check-label"><span style="background:#ffff00"></span> Poor</label>
                      </div>
                    </div>
                  </div>
                  <div class="col-6 text-end">
                    <div class="legend-box small d-inline-block text-start">
                      <div class="form-check form-check-sm mb-2">
                        <input class="form-check-input" type="checkbox" id="chkDrops" checked>
                        <label class="form-check-label"><i class="dash-swatch"></i> Drops (red dashed)</label>
                      </div>
                      <div class="form-check form-check-sm mb-2">
                        <input class="form-check-input" type="checkbox" id="chkFollowLive" checked>
                        <label class="form-check-label" for="chkFollowLive">Follow live</label>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="mt-3">
              <div id="linkBadges" class="small"></div>
            </div>
            
            <div class="mt-4">
              <div class="d-flex justify-content-between align-items-center mb-2">
                <h2 class="h6 m-0">Link metrics</h2>
                <div class="d-flex align-items-center gap-2">
                  <small class="text-muted me-2">updates with timeline & follow live</small>
                  <div id="linkFilter" class="btn-group btn-group-sm" role="group" aria-label="Filter links">
                    <!-- checkboxes will be injected here by JS -->
                  </div>
                  <div class="btn-group btn-group-sm ms-2" role="group">
                    <button id="btnAllLinks" type="button" class="btn btn-outline-secondary">All</button>
                    <button id="btnNoneLinks" type="button" class="btn btn-outline-secondary">None</button>
                  </div>
                </div>
              </div>
              <div class="row g-3">
                <div class="col-12">
                  <canvas id="chartBitrate" height="30"></canvas>
                </div>
                <div class="col-12">
                  <canvas id="chartOWD" height="30"></canvas>
                </div>
                <div class="col-12">
                  <canvas id="chartLoss" height="30"></canvas>
                </div>
                <div class="col-12">
                  <canvas id="chartDrops" height="30"></canvas>
                </div>
              </div>
            </div>

            """ + _events_timeline_html + """

            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
            <script>
            (async function() {
              const sid = """ + str(s_id) + """;
              const deviceRowId = """ + (str(dev_row_id) if 'dev_row_id' in locals() and dev_row_id is not None else 'null') + """;
              const res = await fetch('/log_download?session_id=' + sid);
              if (!res.ok) {
                alert('Failed to load session JSON');
                return;
              }
              let data = await res.json();
              let samples = data.samples || [];
              // ===== Charts (Chart.js) =====
                const linkSet = new Set();
                for (const s of samples) {
                  for (const l of (s.links||[])) if (l && l.name) linkSet.add(String(l.name));
                }
                const linkNames = Array.from(linkSet).sort();

                // ===== Render link filter checkboxes =====
                const filterHost = document.getElementById('linkFilter');
                function renderLinkFilters(){
                  if (!filterHost) return;
                  filterHost.innerHTML = '';
                  linkNames.forEach(function(name, idx){
                    const id = 'lf_' + idx;
                    const wrap = document.createElement('div');
                    wrap.className = 'form-check form-check-inline m-0';
                    wrap.innerHTML = '<input class="form-check-input" type="checkbox" id="'+id+'" data-name="'+name+'" checked>'+
                                     '<label class="form-check-label small" for="'+id+'">'+name+'</label>';
                    filterHost.appendChild(wrap);
                  });
                  // handlers
                  filterHost.querySelectorAll('input[type="checkbox"]').forEach(function(cb){
                    cb.addEventListener('change', function(){
                      const nm = cb.getAttribute('data-name');
                      const on = cb.checked;
                      // toggle datasets in 3 charts
                      [chartRB, chartOWD, chartLOSS].forEach(function(ch){
                        const ds = ch.data.datasets.find(d => d.label === nm);
                        if (ds) ds.hidden = !on;
                        ch.update('none');
                      });
                    });
                  });
                }

                // Build empty per-link arrays
                function emptySeries() { return new Array(samples.length).fill(null); }
                const seriesRB   = Object.fromEntries(linkNames.map(n => [n, emptySeries()]));   // kb/s
                const seriesOWD  = Object.fromEntries(linkNames.map(n => [n, emptySeries()]));   // ms
                const seriesLOSS = Object.fromEntries(linkNames.map(n => [n, emptySeries()]));   // %
                const labels = samples.map(s => s.ts);
                const dropsVideo = samples.map(s => (typeof s.drops_video === 'number' ? s.drops_video : 0));
                const dropsTs    = samples.map(s => (typeof s.drops_ts    === 'number' ? s.drops_ts    : 0));

                function toNum(x){ if (typeof x==='number') return x; if (x==null) return null; var n=parseFloat(String(x).replace(',','.')); return isNaN(n)?null:n; }

                // Fill series
                for (let i=0;i<samples.length;i++){
                  const s = samples[i];
                  const seen = new Set();
                  for (const l of (s.links||[])){
                    if (!l || !l.name) continue;
                    const name = String(l.name);
                    seen.add(name);
                    const rb   = toNum(l.rx_bitrate);
                    const owd  = toNum(l.owdR);
                    const loss = toNum(l.rx_percent_lost);
                    if (name in seriesRB)   seriesRB[name][i]   = (rb!=null?rb:null);
                    if (name in seriesOWD)  seriesOWD[name][i]  = (owd!=null?owd:null);
                    if (name in seriesLOSS) seriesLOSS[name][i] = (loss!=null?loss:null);
                  }
                }

                // Stable colors per link
                const basePalette = ['#0077b6','#ff6f00','#6a4c93','#00a896','#d7263d','#118ab2','#ef476f','#06d6a0','#ff9f1c','#8338ec','#3a86ff','#8ac926','#1982c4','#ff595e','#6a994e','#b56576'];
                const colorForLink = (name) => {
                  const idx = linkNames.indexOf(name);
                  return basePalette[(idx>=0?idx:0) % basePalette.length];
                };

                // Cursor plugin draws a vertical line at current index
                let currentIndex = 0;
                const cursorPlugin = {
                  id: 'cursorLine',
                  afterDatasetsDraw(chart) {
                    if (!chart || !chart.scales || !chart.scales.x) return;
                    const xScale = chart.scales.x;
                    const yScale = chart.scales.y;
                    if (currentIndex < 0 || currentIndex >= (labels.length||0)) return;
                    const ctx = chart.ctx;
                    const x = xScale.getPixelForValue(currentIndex);
                    ctx.save();
                    ctx.strokeStyle = '#111';
                    ctx.lineWidth = 1;
                    ctx.setLineDash([4,3]);
                    ctx.beginPath();
                    ctx.moveTo(x, yScale.top);
                    ctx.lineTo(x, yScale.bottom);
                    ctx.stroke();
                    ctx.restore();
                  }
                };

                function buildDatasets(seriesMap){
                  return linkNames.map(name => ({
                    label: name,
                    data: seriesMap[name],
                    spanGaps: false,
                    pointRadius: 0,
                    stepped: false,
                    borderWidth: 2,
                    borderColor: colorForLink(name),
                    backgroundColor: colorForLink(name),
                  }));
                }

                // Create charts
                const ctxRB   = document.getElementById('chartBitrate').getContext('2d');
                const ctxOWD  = document.getElementById('chartOWD').getContext('2d');
                const ctxLOSS = document.getElementById('chartLoss').getContext('2d');
                const ctxDROP = document.getElementById('chartDrops').getContext('2d');

                const chartRB = new Chart(ctxRB, {
                  type: 'line',
                  data: { labels: labels, datasets: buildDatasets(seriesRB) },
                  options: { responsive: true, animation:false, plugins:{legend:{position:'top'}}, scales:{ y:{ title:{display:true, text:'kb/s'}}, x:{display:false} } },
                  plugins: [cursorPlugin]
                });
                const chartOWD = new Chart(ctxOWD, {
                  type: 'line',
                  data: { labels: labels, datasets: buildDatasets(seriesOWD) },
                  options: { responsive: true, animation:false, plugins:{legend:{position:'top'}}, scales:{ y:{ title:{display:true, text:'OWD (ms)'}}, x:{display:false} } },
                  plugins: [cursorPlugin]
                });
                const chartLOSS = new Chart(ctxLOSS, {
                  type: 'line',
                  data: { labels: labels, datasets: buildDatasets(seriesLOSS) },
                  options: { responsive: true, animation:false, plugins:{legend:{position:'top'}}, scales:{ y:{ min:0, max:100, title:{display:true, text:'Loss (%)'}}, x:{display:false} } },
                  plugins: [cursorPlugin]
                });
                const chartDROP = new Chart(ctxDROP, {
                  type: 'line',
                  data: { labels: labels, datasets: [
                    { label:'Dropped Video', data: dropsVideo, borderColor:'#dc3545', backgroundColor:'#dc3545', pointRadius:0, borderWidth:2 },
                    { label:'Dropped TS',    data: dropsTs,    borderColor:'#fd7e14', backgroundColor:'#fd7e14', pointRadius:0, borderWidth:2 }
                  ] },
                  options: { responsive: true, animation:false, plugins:{legend:{position:'top'}}, scales:{ y:{ title:{display:true, text:'Cumulative drops'}}, x:{display:false} } },
                  plugins: [cursorPlugin]
                });

                function updateCursor(i){
                  currentIndex = i;
                  chartRB.update('none');
                  chartOWD.update('none');
                  chartLOSS.update('none');
                  chartDROP.update('none');
                }

                renderLinkFilters();
                const btnAll = document.getElementById('btnAllLinks');
                const btnNone = document.getElementById('btnNoneLinks');
                function setAllLinks(on){
                  if (!filterHost) return;
                  filterHost.querySelectorAll('input[type="checkbox"]').forEach(function(cb){ cb.checked = on; });
                  [chartRB, chartOWD, chartLOSS].forEach(function(ch){
                    ch.data.datasets.forEach(function(ds){ ds.hidden = !on; });
                    ch.update('none');
                  });
                }
                if (btnAll)  btnAll.addEventListener('click', function(){ setAllLinks(true); });
                if (btnNone) btnNone.addEventListener('click', function(){ setAllLinks(false); });
              const isLive = """ + ("false" if s_end else "true") + """;
              if (!samples.length) {
                alert('No samples');
                return;
              }

              // Build Leaflet map
              const map = L.map('map');
              const osm = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '&copy; OpenStreetMap' });
              osm.addTo(map);

              // Helpers
              const toLatLng = function(s) { return (s.latitude!=null && s.longitude!=null) ? L.latLng(s.latitude, s.longitude) : null; };
              // Haversine distance in meters between two Leaflet LatLng
              function distMeters(a, b) {
                const R = 6371000; // m
                const toRad = function(x){ return x * Math.PI / 180; };
                const dLat = toRad(b.lat - a.lat);
                const dLng = toRad(b.lng - a.lng);
                const la = toRad(a.lat), lb = toRad(b.lat);
                const h = Math.sin(dLat/2)**2 + Math.cos(la)*Math.cos(lb)*Math.sin(dLng/2)**2;
                return 2 * R * Math.asin(Math.sqrt(h));
              }
              const MOVE_EPS_M = 3;      // under 3 m: consider same position
              const STALE_TICKS = 30;     // ~60 s if your sampling is 2 s per tick

              // Quality scoring per link
              function scoreLink(link) {
                function toNum(x){
                  if (typeof x === 'number') return x;
                  if (x == null) return null;
                  var n = parseFloat(String(x).replace(',','.'));
                  return isNaN(n) ? null : n;
                }
                const owd  = toNum(link.owdR);
                if (typeof owd === 'number') {
                  if (owd < 100) return 'good';
                  if (owd > 100 && owd < 200) return 'fair';
                  if (owd >= 200) return 'poor';
                }
                return 'poor';
              }
              const colorFor = function(q) { return q==='good' ? '#ff00ff' : (q==='fair' ? '#ff7f00' : '#ffff00'); };

              // Buckets of polylines per category for toggle visibility
              const layersGood = [];   // fuchsia
              const layersFair = [];   // vivid orange
              const layersPoor = [];   // fluorescent yellow
              const layersDrop = [];   // red dashed

              function applyLegendFilters() {
                const showGood  = document.getElementById('chkGood').checked;
                const showFair  = document.getElementById('chkFair').checked;
                const showPoor  = document.getElementById('chkPoor').checked;
                const showDrops = document.getElementById('chkDrops').checked;
                const sets = [
                  [layersGood, showGood],
                  [layersFair, showFair],
                  [layersPoor, showPoor],
                  [layersDrop, showDrops]
                ];
                for (const [arr, show] of sets) {
                  for (const line of arr) {
                    if (show) { if (!line._map) line.addTo(map); }
                    else { if (line._map) map.removeLayer(line); }
                  }
                }
              }

              let cursorMarker = null; // moving point that follows the timeline

              // Build overall path and per-link paths
              let latlngs = [];
              let perLink = new Map(); // name -> {latlngs:[], segs:[]}
              for (let i = 0; i < samples.length; ++i) {
                const s = samples[i];
                const ll = toLatLng(s);
                if (ll) latlngs.push(ll);

                const links = (s.links||[]).filter(function(l) { return l && l.name; });
                for (let j = 0; j < links.length; ++j) {
                  const l = links[j];
                  const key = l.name;
                  if (!perLink.has(key)) perLink.set(key, {latlngs:[], segs:[] });
                  const q = scoreLink(l);
                  const ll2 = ll; // per-tick uses same GPS
                  if (ll2) perLink.get(key).segs.push({ ll: ll2, q: q });
                }
              }
              // Precompute ticks since last movement for each index
              let coords = samples.map(function(s){ return toLatLng(s); });
              let ticksSinceMove = new Array(samples.length).fill(Infinity);
              let prevLL = null, lastMovedIdx = -1;
              for (let i = 0; i < coords.length; ++i) {
                const ll = coords[i];
                if (ll) {
                  if (!prevLL) { prevLL = ll; lastMovedIdx = i; }
                  else {
                    try {
                      if (distMeters(ll, prevLL) > MOVE_EPS_M) { lastMovedIdx = i; prevLL = ll; }
                    } catch (e) { /* ignore */ }
                  }
                }
                ticksSinceMove[i] = (lastMovedIdx >= 0) ? (i - lastMovedIdx) : Infinity;
              }

let lastCoord = coords.length ? coords[coords.length-1] : null;

function appendSample(i){
  const s = samples[i];
  const ll = toLatLng(s);
  if (ll) {
    latlngs.push(ll);
    try {
      if (lastCoord == null) { lastCoord = ll; lastMovedIdx = i; }
      else if (distMeters(ll, lastCoord) > MOVE_EPS_M) { lastCoord = ll; lastMovedIdx = i; }
    } catch(e) {}
  }
  ticksSinceMove[i] = (lastMovedIdx >= 0) ? (i - lastMovedIdx) : Infinity;

  const links = (s.links||[]).filter(function(l){ return l && l.name; });
  for (let j=0;j<links.length;++j){
    const l = links[j];
    const key = l.name;
    if (!perLink.has(key)) perLink.set(key, {latlngs:[], segs:[]});
    const q = scoreLink(l);
    if (ll) perLink.get(key).segs.push({ ll: ll, q: q });
  }

  let worst = 'good';
  for (let j=0;j<(s.links||[]).length;++j){
    const q = scoreLink(s.links[j]);
    if (q === 'poor') { worst = 'poor'; break; }
    if (q === 'fair' && worst === 'good') worst = 'fair';
  }
  const curVid = (typeof s.drops_video === 'number') ? s.drops_video : 0;
  const curTs  = (typeof s.drops_ts === 'number') ? s.drops_ts : 0;

  const prev = segs.length ? segs[segs.length-1] : null;
  const prevVid = (segs._prevVid || 0), prevTs = (segs._prevTs || 0);
  const newDrop = (curVid > prevVid) || (curTs > prevTs);

  const seg = { ll: ll || (prev && prev.ll) || null, q: worst, drop: newDrop };
  segs.push(seg);
  segs._prevVid = curVid; segs._prevTs = curTs;

  if (prev && (prev.ll || seg.ll)){
    let opts = { color: colorFor(prev.q), weight: 4, opacity: 0.9 };
    let bucket = null;
    if (seg.drop) { opts = { color: '#ff0000', weight: 4, opacity: 0.9, dashArray: '6,6' }; bucket = layersDrop; }
    else if (prev.q === 'good') bucket = layersGood;
    else if (prev.q === 'fair') bucket = layersFair;
    else bucket = layersPoor;
    const a = prev.ll || seg.ll; const b = seg.ll || prev.ll;
    const line = L.polyline([a,b], opts).addTo(map);
    bucket.push(line);
  }
}

async function repoll(){
  try{
    const r = await fetch('/log_download?session_id=' + sid);
    if (!r.ok) return;
    const d = await r.json();
    const incoming = d.samples || [];
    if (incoming.length <= samples.length) return;
    const wasAtEnd = (parseInt(scrub.value,10) === parseInt(scrub.max,10));
    for (let i=samples.length; i<incoming.length; ++i){
      samples.push(incoming[i]);
      coords.push(toLatLng(incoming[i]));
      ticksSinceMove.push(Infinity);
      appendSample(i);
    }
    // === charts: append labels and per-link values ===
    labels.push(incoming[incoming.length-1].ts);
    dropsVideo.push(typeof incoming[incoming.length-1].drops_video === 'number' ? incoming[incoming.length-1].drops_video : 0);
    dropsTs.push(typeof incoming[incoming.length-1].drops_ts === 'number' ? incoming[incoming.length-1].drops_ts : 0);

    // Append per link at the last tick only (charts are per tick)
    const lastS = incoming[incoming.length-1];
    const seen = new Set();
    for (const l of (lastS.links||[])){
      if (!l || !l.name) continue;
      const name = String(l.name);
      if (!(name in seriesRB)) { // new link seen live; add full-length null series
        seriesRB[name]   = new Array(labels.length-1).fill(null);
        seriesOWD[name]  = new Array(labels.length-1).fill(null);
        seriesLOSS[name] = new Array(labels.length-1).fill(null);
        linkNames.push(name);
        chartRB.data.datasets.push({label:name, data:seriesRB[name], pointRadius:0, borderWidth:2, borderColor:colorForLink(name), backgroundColor:colorForLink(name)});
        chartOWD.data.datasets.push({label:name, data:seriesOWD[name], pointRadius:0, borderWidth:2, borderColor:colorForLink(name), backgroundColor:colorForLink(name)});
        chartLOSS.data.datasets.push({label:name, data:seriesLOSS[name], pointRadius:0, borderWidth:2, borderColor:colorForLink(name), backgroundColor:colorForLink(name)});
        // add filter checkbox for the new link
        if (filterHost) {
          const id = 'lf_' + linkNames.length;
          const wrap = document.createElement('div');
          wrap.className = 'form-check form-check-inline m-0';
          wrap.innerHTML = '<input class="form-check-input" type="checkbox" id="'+id+'" data-name="'+name+'" checked>'+
                           '<label class="form-check-label small" for="'+id+'">'+name+'</label>';
          filterHost.appendChild(wrap);
          const cb = wrap.querySelector('input');
          cb.addEventListener('change', function(){
            const nm = cb.getAttribute('data-name');
            const on = cb.checked;
            [chartRB, chartOWD, chartLOSS].forEach(function(ch){
              const ds = ch.data.datasets.find(d => d.label === nm);
              if (ds) ds.hidden = !on;
              ch.update('none');
            });
          });
        }
      }
      const rb   = toNum(l.rx_bitrate);
      const owd  = toNum(l.owdR);
      const loss = toNum(l.rx_percent_lost);
      seriesRB[name].push(rb!=null?rb:null);
      seriesOWD[name].push(owd!=null?owd:null);
      seriesLOSS[name].push(loss!=null?loss:null);
      seen.add(name);
    }
    // Pad other links with null to keep alignment
    for (const nm of linkNames){
      if (!seen.has(nm)){
        if (seriesRB[nm].length   < labels.length) seriesRB[nm].push(null);
        if (seriesOWD[nm].length  < labels.length) seriesOWD[nm].push(null);
        if (seriesLOSS[nm].length < labels.length) seriesLOSS[nm].push(null);
      }
    }
    chartRB.data.labels = labels;
    chartOWD.data.labels = labels;
    chartLOSS.data.labels = labels;
    chartDROP.data.labels = labels;
    chartDROP.data.datasets[0].data = dropsVideo;
    chartDROP.data.datasets[1].data = dropsTs;
    chartRB.update('none'); chartOWD.update('none'); chartLOSS.update('none'); chartDROP.update('none');
    const chkAll = document.getElementById('chkAllLinks');
    if (chkAll && chkAll.checked) { clearPerLink(); drawPerLink(); }
    scrub.max = Math.max(0, samples.length - 1);
    if (wasAtEnd || (chkFollow && chkFollow.checked)) { scrub.value = scrub.max; renderAt(parseInt(scrub.max,10)); }
  } catch(e){}
}


              // Overall polyline colored by worst quality per tick; mark NEW drops only
              let segs = [];
              let prevDropVid = 0, prevDropTs = 0; // cumulative counters observed so far
              for (let i = 0; i < samples.length; ++i) {
                const s = samples[i];
                const ll = toLatLng(s);
                if (!ll) continue;

                // Quality from links only
                let worst = 'good';
                for (let j = 0; j < (s.links||[]).length; ++j) {
                  const l = s.links[j];
                  const q = scoreLink(l);
                  if (q === 'poor') { worst = 'poor'; break; }
                  if (q === 'fair' && worst === 'good') worst = 'fair';
                }

                // Mark segment red-dashed only if drops INCREASED since previous tick
                const curVid = (typeof s.drops_video === 'number') ? s.drops_video : 0;
                const curTs  = (typeof s.drops_ts === 'number') ? s.drops_ts : 0;
                const newDrop = (curVid > prevDropVid) || (curTs > prevDropTs);
                segs.push({ ll: ll, q: worst, drop: newDrop });
                prevDropVid = curVid;
                prevDropTs  = curTs;
              }
              // Draw segments into category buckets; visibility controlled by checkboxes
              let last = null;
              for (let i = 0; i < segs.length; ++i) {
                const seg = segs[i];
                if (!last) { last = seg; continue; }
                let opts = { color: colorFor(last.q), weight: 4, opacity: 0.9 };
                let bucket = null;
                if (seg.drop) {
                  opts = { color: '#ff0000', weight: 4, opacity: 0.9, dashArray: '6,6' };
                  bucket = layersDrop;
                } else if (last.q === 'good') {
                  bucket = layersGood;
                } else if (last.q === 'fair') {
                  bucket = layersFair;
                } else {
                  bucket = layersPoor;
                }
                const line = L.polyline([last.ll, seg.ll], opts).addTo(map);
                bucket.push(line);
                last = seg;
              }
              // Apply initial visibility from legend toggles
              applyLegendFilters();

              // Per-link optional polylines (thin)
              const linkLayers = new Map();
              function drawPerLink() {
                for (const [name, obj] of perLink.entries()) {
                  if (linkLayers.has(name)) continue;
                  let prev = null;
                  const llayers = [];
                  for (let i = 0; i < obj.segs.length; ++i) {
                    const seg = obj.segs[i];
                    if (!prev) { prev = seg; continue; }
                    llayers.push(L.polyline([prev.ll, seg.ll], { color: colorFor(seg.q), weight: 2, opacity: 0.5 }).addTo(map));
                    prev = seg;
                  }
                  linkLayers.set(name, llayers);
                }
              }
              function clearPerLink() {
                for (const arr of linkLayers.values()) arr.forEach(function(l) { map.removeLayer(l); });
                linkLayers.clear();
              }

              // Fit map
              if (latlngs.length) {
                map.fitBounds(L.latLngBounds(latlngs), { padding:[20,20] });
                L.circleMarker(latlngs[0], { radius:5, color:'#333', fill:true, fillOpacity:1 }).addTo(map).bindTooltip('Start');
                L.circleMarker(latlngs[latlngs.length-1], { radius:5, color:'#333', fill:true, fillOpacity:1 }).addTo(map).bindTooltip('End');
              }

              // Timeline & badges
              const scrub = document.getElementById('scrub');
              const tsStart = document.getElementById('tsStart');
              const tsNow = document.getElementById('tsNow');
              const tsEnd = document.getElementById('tsEnd');
              const linkBadges = document.getElementById('linkBadges');
              scrub.max = Math.max(0, samples.length - 1);
              function fmtTs(ts){ return ts?ts.replace('T',' ').substring(0,19):ts; }
              tsStart.textContent = fmtTs(samples[0].ts);
              tsEnd.textContent = fmtTs(samples[samples.length-1].ts);
              function renderAt(i) {
                const s = samples[i];
                tsNow.textContent = fmtTs(s.ts);
                // Move the cursor point to the current GPS position (always reflect latest lat/lng; no warnings)
                var llc = (s && s.latitude!=null && s.longitude!=null) ? L.latLng(s.latitude, s.longitude) : null;
                if (llc) {
                  if (cursorMarker) {
                    cursorMarker.setLatLng(llc);
                    cursorMarker.setStyle({ color: '#111' });
                  } else {
                    cursorMarker = L.circleMarker(llc, { radius: 6, color: '#111', weight: 2, fill: true, fillOpacity: 1 }).addTo(map);
                  }
                }
                const links = (s.links||[]);
                // Sort links by name for stable display
                links.sort(function(a,b){ return String(a.name).localeCompare(String(b.name)); });
                let dropsHtml = '';
                if (s.drops_video && s.drops_video > 0) {
                  dropsHtml += '<span class="badge me-1 mb-1" style="background:#ff0000;color:#fff;">Dropped Video: ' + s.drops_video + '</span>';
                }
                if (s.drops_ts && s.drops_ts > 0) {
                  dropsHtml += '<span class="badge me-1 mb-1" style="background:#ff0000;color:#fff;">Dropped TS: ' + s.drops_ts + '</span>';
                }
                let html = dropsHtml;
                for (let j = 0; j < links.length; ++j) {
                  const l = links[j];
                  const q = scoreLink(l);
                  const bg = colorFor(q); // sync badge color with map segments
                  const rb = (typeof l.rx_bitrate==='number'? l.rx_bitrate : 0);
                  const fg = '#000'; // readable on light green / light blue / orange
                  const owd = (l.owdR!=null? l.owdR : '–');
                  const loss = (l.rx_percent_lost!=null? l.rx_percent_lost+'%' : '–');
                  html += '<span class="badge me-1 mb-1 badge-link" style="background:' + bg + '; color:' + fg + ';">' + l.name + ': ' + rb + ' kb/s • OWD ' + owd + ' ms • loss ' + loss + '</span>';
                }
                linkBadges.innerHTML = html || '<span class="text-muted">No links</span>';
                updateCursor(i);
              }
              scrub.addEventListener('input', function(e){ renderAt(parseInt(scrub.value,10)||0); });
              renderAt(0);
              updateCursor(0);

              // Toggle per-link overlays (checkbox removed by default; keep graceful behavior)
              const chkAllLinks = document.getElementById('chkAllLinks');
              if (chkAllLinks) {
                chkAllLinks.addEventListener('change', function() {
                  if (chkAllLinks.checked) drawPerLink(); else clearPerLink();
                });
                if (chkAllLinks.checked) { drawPerLink(); } else { clearPerLink(); }
              } else {
                // No checkbox present: ensure overlays stay hidden on load
                clearPerLink();
              }

              // Legend toggles for Good/Fair/Poor/Drops
              ['chkGood','chkFair','chkPoor','chkDrops'].forEach(function(id){
                const el = document.getElementById(id);
                if (el) el.addEventListener('change', applyLegendFilters);
              });


            // Follow live
            const chkFollow = document.getElementById('chkFollowLive');
            let pollTimer = null;
            function setPolling(on){
              if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
              if (on) {
                pollTimer = setInterval(async function(){
                  try {
                    await repoll();
                    if (deviceRowId !== null && deviceRowId !== 'null') {
                      fetch('/data?id=' + deviceRowId).catch(()=>{});
                    }
                    if (window.refreshEvents) window.refreshEvents();
                  } catch(e){}
                }, 4000);
              }
            }
            if (chkFollow) {
              chkFollow.addEventListener('change', function(){
                setPolling(chkFollow.checked && isLive);
              });
              setPolling(chkFollow.checked && isLive);
            }
            })();
            </script>
          </body></html>
          """
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return body.encode('utf-8')

    @require_login
    @cherrypy.expose
    def log_purge(self, **kwargs):
        import sqlite3
        with connect_db() as c:
            c.execute("DELETE FROM live_sample")
            c.execute("DELETE FROM live_session")
        # redirect back to UI with message
        raise cherrypy.HTTPRedirect("/logs_ui?msg=Logs%20purged")
    
    @require_login
    @cherrypy.expose
    def log_stop(self, session_id=None):
        import sqlite3
        from datetime import datetime
        if not session_id:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Missing%20session_id")
        try:
            sid = int(session_id)
        except Exception:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Bad%20session_id")
        now_iso = datetime.utcnow().replace(tzinfo=None).isoformat()
        with connect_db() as c:
            c.execute(
                "UPDATE live_session SET ended_at=? WHERE id=? AND ended_at IS NULL",
                (now_iso, sid)
            )
        raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20stopped")

    @require_login
    @cherrypy.expose
    def log_delete(self, session_id=None):
        import sqlite3
        if not session_id:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Missing%20session_id")
        try:
            sid = int(session_id)
        except Exception:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Bad%20session_id")
        with connect_db() as c:
            # Delete samples first, then the session
            c.execute("DELETE FROM live_sample WHERE session_id=?", (sid,))
            c.execute("DELETE FROM live_session WHERE id=?", (sid,))
        raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20deleted")
        
    @require_login
    @cherrypy.expose
    def log_rename(self, session_id=None, title=""):
        import sqlite3
        if not session_id:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Missing%20session_id")
        try:
            sid = int(session_id)
        except Exception:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Bad%20session_id")
        title = (title or "").strip()
        with connect_db() as c:
            c.execute("UPDATE live_session SET title=? WHERE id=?", (title if title else None, sid))
        raise cherrypy.HTTPRedirect("/logs_ui?msg=Title%20saved")
    
    @require_login
    @cherrypy.expose
    def session_events(self, session_id=None):
        import json
        from datetime import datetime as _dt2
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not session_id:
            return b'{"ok":false,"error":"missing session_id"}'
        try:
            sid = int(session_id)
        except Exception:
            return b'{"ok":false,"error":"bad session_id"}'
        with connect_db() as c:
            s = c.execute(
                "SELECT device_host, started_at, ended_at, input_index, input_identifier "
                "FROM live_session WHERE id=?", (sid,)
            ).fetchone()
            if not s:
                return b'{"ok":false,"error":"session_not_found"}'
            host, started_at, ended_at, input_index, input_identifier = s
            ts_start_cmp = started_at[:19].replace('T', ' ')
            ts_end_cmp   = (ended_at[:19].replace('T', ' ') if ended_at else _dt2.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
            rows = c.execute(
                "SELECT ts, node, level, message FROM streamhub_log "
                "WHERE device_host=? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
                (host, ts_start_cmp, ts_end_cmp)
            ).fetchall()
        raw_count = len(rows)
        rows = _filter_logs_for_input(rows, input_index, input_identifier, device_id=host)
        cherrypy.log(f"[session_events] sid={sid} host={host} input_index={input_index!r} "
                     f"ident={input_identifier!r} raw={raw_count} filtered={len(rows)}")
        events = [{"ts": r[0], "node": r[1], "level": r[2], "message": r[3]} for r in rows]
        return json.dumps({"ok": True, "events": events}).encode("utf-8")

    @require_login
    @cherrypy.expose
    def log_pdf(self, session_id=None):
        import io
        from datetime import datetime
        if not session_id:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Missing%20session_id")
        try:
            sid = int(session_id)
        except Exception:
            raise cherrypy.HTTPRedirect("/logs_ui?msg=Bad%20session_id")

        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import mm
            from reportlab.platypus import (
                SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
            )
            from reportlab.graphics.shapes import Drawing, PolyLine, String, Line, Rect
            from reportlab.graphics import renderPDF
        except ImportError:
            cherrypy.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
            return b'reportlab not installed. Run: pip install reportlab'

        # ── Fetch data ──────────────────────────────────────────────────────
        with connect_db() as c:
            s = c.execute(
                "SELECT id, device_id, device_host, input_key, input_index, "
                "input_identifier, input_display_name, started_at, ended_at, title "
                "FROM live_session WHERE id=?", (sid,)
            ).fetchone()
            if not s:
                raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20not%20found")
            if not s[8]:
                raise cherrypy.HTTPRedirect("/logs_ui?msg=Session%20still%20live")

            rows = c.execute(
                "SELECT ts, link_name, owdR, rx_bitrate, rx_percent_lost, "
                "rx_lost_nb_packets, drops_video, drops_ts "
                "FROM live_sample WHERE session_id=? ORDER BY id ASC", (sid,)
            ).fetchall()

            ts_start_cmp = s[7][:19].replace('T', ' ') if s[7] else ''
            ts_end_cmp   = s[8][:19].replace('T', ' ') if s[8] else ''
            _ev_raw = c.execute(
                "SELECT ts, level, message FROM streamhub_log "
                "WHERE device_host=? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
                (s[2], ts_start_cmp, ts_end_cmp)
            ).fetchall() if ts_start_cmp else []
            ev_rows = _filter_logs_for_input(_ev_raw, s[4], s[5], device_id=s[2])

            dev_name = c.execute(
                "SELECT name FROM devices WHERE host=? LIMIT 1", (s[2],)
            ).fetchone()

            # First GPS fix for this session
            gps_row = c.execute(
                "SELECT latitude, longitude FROM live_sample "
                "WHERE session_id=? AND latitude IS NOT NULL AND longitude IS NOT NULL "
                "ORDER BY id ASC LIMIT 1", (sid,)
            ).fetchone()
            first_gps = (round(gps_row[0], 6), round(gps_row[1], 6)) if gps_row else None

        # ── Compute per-link stats ───────────────────────────────────────────
        from collections import defaultdict
        link_stats = defaultdict(lambda: {
            'rb': [], 'owd': [], 'loss': [], 'lost_pkts': 0
        })
        drops_video_max = 0
        drops_ts_max = 0
        bitrate_series = []   # (ts_str, total_rb) for chart

        by_ts = {}
        for r in rows:
            ts, lname, owd, rb, rpl, rlnp, dv, dt = r
            if ts not in by_ts:
                by_ts[ts] = {'total_rb': 0, 'dv': dv or 0, 'dt': dt or 0}
            if lname:
                if rb is not None:
                    link_stats[lname]['rb'].append(rb)
                    by_ts[ts]['total_rb'] += rb
                if owd is not None:
                    link_stats[lname]['owd'].append(owd)
                if rpl is not None:
                    link_stats[lname]['loss'].append(rpl)
                if rlnp:
                    link_stats[lname]['lost_pkts'] += rlnp
            if dv:
                drops_video_max = max(drops_video_max, dv)
            if dt:
                drops_ts_max = max(drops_ts_max, dt)

        for ts_key in sorted(by_ts.keys()):
            bitrate_series.append((ts_key, by_ts[ts_key]['total_rb']))

        def _avg(lst): return round(sum(lst)/len(lst), 1) if lst else 0
        def _max(lst): return max(lst) if lst else 0
        def _fmt_dur(secs):
            secs = int(secs)
            h, r = divmod(secs, 3600)
            m, s2 = divmod(r, 60)
            return f"{h:02d}:{m:02d}:{s2:02d}"
        def _parse_dt(x):
            try: return datetime.fromisoformat(str(x)[:19])
            except: return None

        started_dt = _parse_dt(s[7])
        ended_dt   = _parse_dt(s[8])
        duration   = _fmt_dur((ended_dt - started_dt).total_seconds()) if started_dt and ended_dt else "—"
        generated  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        session_title = s[9] or ''
        title_str  = session_title or s[6] or s[1] or f"Session #{sid}"
        # Filename: include session title slug if set
        import re as _re
        title_slug = _re.sub(r'[^\w\-]', '_', session_title)[:40].strip('_') if session_title else ''
        pdf_filename = f"session_{sid}{'_'+title_slug if title_slug else ''}_report.pdf"
        sh_name    = dev_name[0] if dev_name else s[2]

        # ── Build PDF ────────────────────────────────────────────────────────
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf, pagesize=A4,
            leftMargin=15*mm, rightMargin=15*mm,
            topMargin=15*mm, bottomMargin=15*mm,
            title=f"StreamPilot — {title_str}"
        )
        W_doc = A4[0] - 30*mm

        styles = getSampleStyleSheet()
        sN  = styles['Normal']
        sH1 = ParagraphStyle('h1', parent=styles['Heading1'], fontSize=14, spaceAfter=4)
        sH2 = ParagraphStyle('h2', parent=styles['Heading2'], fontSize=11, spaceAfter=3, spaceBefore=8)
        sMono = ParagraphStyle('mono', parent=sN, fontName='Courier', fontSize=8)
        sSmall = ParagraphStyle('small', parent=sN, fontSize=8, textColor=colors.grey)

        COL_HDR = colors.HexColor('#1a3a5c')
        COL_ROW = colors.HexColor('#f0f4f8')

        story = []

        # Header
        story.append(Paragraph("StreamPilot — Session Report", sH1))
        story.append(Paragraph(f"Generated {generated}", sSmall))
        story.append(HRFlowable(width="100%", thickness=1, color=COL_HDR, spaceAfter=6))

        # Session info table
        info_data = [
            ["Session", f"#{sid}  {title_str}"],
            ["StreamHub", f"{sh_name}  ({s[2]})"],
            ["Input", f"#{s[4]}  {s[5] or '—'}  {s[6] or ''}".strip()],
            ["Started", s[7][:19].replace('T', ' ') if s[7] else '—'],
            ["Ended",   s[8][:19].replace('T', ' ') if s[8] else '—'],
            ["Duration", duration],
            ["Dropped video / TS", f"{drops_video_max} / {drops_ts_max}"],
        ]
        if first_gps:
            info_data.append(["First GPS fix", f"lat {first_gps[0]}  lng {first_gps[1]}"])
        info_tbl = Table(info_data, colWidths=[35*mm, W_doc - 35*mm])
        info_tbl.setStyle(TableStyle([
            ('FONTNAME',  (0,0), (0,-1), 'Helvetica-Bold'),
            ('FONTSIZE',  (0,0), (-1,-1), 9),
            ('TEXTCOLOR', (0,0), (0,-1), COL_HDR),
            ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.white, COL_ROW]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#c0ccd8')),
            ('TOPPADDING', (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ]))
        story.append(info_tbl)
        story.append(Spacer(1, 8))

        # Per-link stats table
        if link_stats:
            story.append(Paragraph("Link metrics", sH2))
            tbl_data = [["Link", "Avg kb/s", "Max kb/s", "Avg OWD ms", "Max OWD ms", "Avg loss %", "Lost pkts"]]
            for lname, ls in sorted(link_stats.items()):
                tbl_data.append([
                    lname,
                    str(_avg(ls['rb'])),
                    str(_max(ls['rb'])),
                    str(_avg(ls['owd'])),
                    str(_max(ls['owd'])),
                    str(_avg(ls['loss'])),
                    str(ls['lost_pkts']),
                ])
            cw = [50*mm] + [(W_doc-50*mm)/6]*6
            lt = Table(tbl_data, colWidths=cw)
            lt.setStyle(TableStyle([
                ('BACKGROUND',   (0,0), (-1,0), COL_HDR),
                ('TEXTCOLOR',    (0,0), (-1,0), colors.white),
                ('FONTNAME',     (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTNAME',     (0,1), (-1,-1), 'Helvetica'),
                ('FONTSIZE',     (0,0), (-1,-1), 8),
                ('ALIGN',        (1,0), (-1,-1), 'RIGHT'),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, COL_ROW]),
                ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#c0ccd8')),
                ('TOPPADDING',   (0,0), (-1,-1), 3),
                ('BOTTOMPADDING',(0,0), (-1,-1), 3),
            ]))
            story.append(lt)
            story.append(Spacer(1, 6))

        # Bitrate chart (SVG polyline via reportlab Drawing)
        if len(bitrate_series) >= 2:
            story.append(Paragraph("Total bitrate over time (kb/s)", sH2))
            CH_W, CH_H = float(W_doc), 60.0
            pad_l, pad_r, pad_t, pad_b = 10.0, 6.0, 6.0, 14.0
            draw_w = CH_W - pad_l - pad_r
            draw_h = CH_H - pad_t - pad_b

            values = [v for _, v in bitrate_series]
            max_v  = max(values) or 1
            n      = len(values)

            d = Drawing(CH_W, CH_H)
            # background
            d.add(Rect(0, 0, CH_W, CH_H, fillColor=colors.HexColor('#f8fafc'), strokeColor=colors.HexColor('#dee2e6'), strokeWidth=0.5))
            # horizontal grid lines (0, 25%, 50%, 75%, 100%)
            for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
                gy = pad_b + frac * draw_h
                d.add(Line(pad_l, gy, CH_W - pad_r, gy,
                           strokeColor=colors.HexColor('#dee2e6'), strokeWidth=0.4))
                label_val = int(max_v * frac)
                d.add(String(pad_l - 2, gy - 3, str(label_val),
                             fontSize=5, fillColor=colors.grey, textAnchor='end'))
            # polyline
            pts = []
            for i, v in enumerate(values):
                x = pad_l + (i / (n - 1)) * draw_w
                y = pad_b + (v / max_v) * draw_h
                pts += [x, y]
            d.add(PolyLine(pts, strokeColor=colors.HexColor('#0d6efd'), strokeWidth=1.2, fillColor=None))
            # x-axis labels (start / mid / end)
            for frac, label in ((0.0, bitrate_series[0][0][11:19]),
                                (0.5, bitrate_series[n//2][0][11:19]),
                                (1.0, bitrate_series[-1][0][11:19])):
                x = pad_l + frac * draw_w
                d.add(String(x, 2, label, fontSize=5, fillColor=colors.grey, textAnchor='middle'))
            story.append(d)
            story.append(Spacer(1, 6))

        # StreamHub events
        if ev_rows:
            story.append(Paragraph("StreamHub events", sH2))
            ev_data = [["Timestamp", "Level", "Message"]]
            for ev in ev_rows:
                ev_data.append([ev[0], ev[1] or '', ev[2] or ''])
            ev_cw = [40*mm, 18*mm, W_doc - 58*mm]
            ev_t = Table(ev_data, colWidths=ev_cw, repeatRows=1)
            lv_colors = {
                'ERROR':   colors.HexColor('#f8d7da'),
                'WARNING': colors.HexColor('#fff3cd'),
                'WARN':    colors.HexColor('#fff3cd'),
            }
            ev_style = [
                ('BACKGROUND',   (0,0), (-1,0), COL_HDR),
                ('TEXTCOLOR',    (0,0), (-1,0), colors.white),
                ('FONTNAME',     (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTNAME',     (0,1), (-1,-1), 'Courier'),
                ('FONTSIZE',     (0,0), (-1,-1), 7),
                ('GRID',         (0,0), (-1,-1), 0.3, colors.HexColor('#c0ccd8')),
                ('TOPPADDING',   (0,0), (-1,-1), 2),
                ('BOTTOMPADDING',(0,0), (-1,-1), 2),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, COL_ROW]),
            ]
            for i, ev in enumerate(ev_rows, start=1):
                lvl = (ev[1] or '').upper()
                if lvl in lv_colors:
                    ev_style.append(('BACKGROUND', (0,i), (-1,i), lv_colors[lvl]))
            ev_t.setStyle(TableStyle(ev_style))
            story.append(ev_t)

        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey))
        story.append(Paragraph(
            f"StreamPilot — Alexandre Licinio © 2026 — Stream smarter. Pilot with precision. Broadcast better.",
            sSmall
        ))

        doc.build(story)
        pdf_bytes = buf.getvalue()
        cherrypy.response.headers['Content-Type'] = 'application/pdf'
        cherrypy.response.headers['Content-Disposition'] = (
            f'attachment; filename={pdf_filename}'
        )
        return pdf_bytes

    @require_login
    @cherrypy.expose
    def geoip(self, ip=None):
        """Return cached lat/lng from DB only — no external API calls."""
        import json as _j
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        cherrypy.response.headers["Cache-Control"] = "max-age=60"
        if not ip or ip in ("0.0.0.0", "localhost", "127.0.0.1"):
            return _j.dumps({"ok": False, "reason": "invalid ip"}).encode("utf-8")
        try:
            with connect_db() as c:
                row = c.execute(
                    "SELECT lat, lng, label FROM srt_endpoint_location WHERE address=?", (ip,)
                ).fetchone()
            if row and row[0] is not None:
                return _j.dumps({"ok": True, "ip": ip,
                                  "lat": row[0], "lng": row[1], "label": row[2] or ip}).encode("utf-8")
            return _j.dumps({"ok": False, "ip": ip, "reason": "not in DB"}).encode("utf-8")
        except Exception as e:
            return _j.dumps({"ok": False, "ip": ip, "reason": str(e)}).encode("utf-8")

    @require_login
    @cherrypy.expose
    def gateway_geoip(self, msg=None):
        """Manual GeoIP editor — all IPs seen across all gateways."""
        import html as _h
        def esc(x): return _h.escape('' if x is None else str(x))

        # Collect all IPs seen: from srt_route_sample connections + srt_event
        seen_ips = set()
        with connect_db() as c:
            # IPs from srt_event
            for row in c.execute('SELECT DISTINCT ip_address FROM srt_event WHERE ip_address IS NOT NULL').fetchall():
                if row[0] and row[0] not in ('0.0.0.0',): seen_ips.add(row[0])
            # IPs from srt_connection_sample
            for row in c.execute('SELECT DISTINCT address FROM srt_connection_sample WHERE address IS NOT NULL').fetchall():
                if row[0] and row[0] not in ('0.0.0.0',): seen_ips.add(row[0])
            # Gateway IPs themselves
            for row in c.execute('SELECT DISTINCT host FROM srt_gateway_device').fetchall():
                if row[0]: seen_ips.add(row[0])
            # Existing saved locations
            saved = {r[0]: (r[1], r[2], r[3]) for r in c.execute(
                'SELECT address, lat, lng, label FROM srt_endpoint_location'
            ).fetchall()}

        msg_html = ('<div class="alert alert-success">'+esc(msg)+'</div>') if msg else ''

        rows_html = ''
        for ip in sorted(seen_ips):
            s = saved.get(ip)
            lat_val  = esc(s[0]) if s and s[0] is not None else ''
            lng_val  = esc(s[1]) if s and s[1] is not None else ''
            lbl_val  = esc(s[2]) if s and s[2] else ''
            saved_badge = ('<span class="badge text-bg-success">saved</span>'
                           if s and s[0] is not None else
                           '<span class="badge text-bg-secondary">not set</span>')
            _fid = 'gf_' + esc(ip).replace('.', '_')
            _del = (
                '<form method="post" action="/gateway_geoip_delete" class="d-inline ms-2">'
                '<input type="hidden" name="ip" value="'+esc(ip)+'">'
                '<button class="btn btn-sm btn-outline-danger" type="submit">Clear</button></form>'
                if s and s[0] is not None else ''
            )
            rows_html += (
                # Invisible form element outside <tr> — inputs reference it via form= attribute
                '<form method="post" action="/gateway_geoip_save" id="'+_fid+'"></form>'
                '<tr>'
                '<td class="font-monospace small">'+esc(ip)+'</td>'
                '<td>'+saved_badge+'</td>'
                '<td class="small text-muted">'+lbl_val+'</td>'
                '<td><input type="hidden" name="ip" value="'+esc(ip)+'" form="'+_fid+'">'
                '<input form="'+_fid+'" class="form-control form-control-sm" name="lat" '
                'value="'+lat_val+'" placeholder="48.8566" style="width:110px"></td>'
                '<td><input form="'+_fid+'" class="form-control form-control-sm" name="lng" '
                'value="'+lng_val+'" placeholder="2.3522" style="width:110px"></td>'
                '<td><input form="'+_fid+'" class="form-control form-control-sm" name="label" '
                'value="'+lbl_val+'" placeholder="Paris, France" style="width:160px"></td>'
                '<td><button form="'+_fid+'" class="btn btn-sm btn-primary" type="submit">Save</button>'
                + _del +
                '</td>'
                '</tr>'
            )
        if not rows_html:
            rows_html = '<tr><td colspan="7" class="text-muted text-center p-4">No IPs detected yet. Start a route.</td></tr>'

        html_body = (
            '<!doctype html><html><head>'
            '<meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">'
            '<title>GeoIP — StreamPilot</title>'
            '</head><body class="p-4">'
            '<div class="d-flex justify-content-between align-items-center mb-3">'
            '<h1 class="h4 m-0">GeoIP — Manual coordinates</h1>'
            '<a class="btn btn-outline-secondary" href="/gateway_dashboard">← SRT Gateway</a>'
            '</div>'
            + msg_html +
            '<p class="text-muted small mb-3">Enter GPS coordinates for each detected IP. '
            'These are used to plot source, gateway and client positions on the route maps.</p>'
            '<div class="table-responsive">'
            '<table class="table table-sm align-middle">'
            '<thead class="table-light"><tr>'
            '<th>IP</th><th>Status</th><th>Label</th>'
            '<th>Latitude</th><th>Longitude</th><th>Description</th><th>Action</th>'
            '</tr></thead>'
            '<tbody>'+rows_html+'</tbody>'
            '</table></div>'
            '</body></html>'
        )
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return html_body.encode('utf-8')

    @require_login
    @cherrypy.expose
    def gateway_geoip_save(self, ip=None, lat=None, lng=None, label=None):
        def _s(v): return (v[-1] if isinstance(v, list) else (v or '')).strip()
        ip = _s(ip); lat = _s(lat); lng = _s(lng); label = _s(label)
        if ip:
            try:
                lat_f = float(lat) if lat else None
                lng_f = float(lng) if lng else None
            except Exception:
                raise cherrypy.HTTPRedirect('/gateway_geoip?msg=Invalid+coordinates')
            with connect_db() as c:
                c.execute(
                    'INSERT OR REPLACE INTO srt_endpoint_location (address, label, lat, lng) '
                    'VALUES (?,?,?,?)',
                    (ip, label or None, lat_f, lng_f)
                )
        raise cherrypy.HTTPRedirect('/gateway_geoip?msg=Saved')

    @require_login
    @cherrypy.expose
    def gateway_geoip_delete(self, ip=None):
        if ip:
            with connect_db() as c:
                c.execute('DELETE FROM srt_endpoint_location WHERE address=?', (ip,))
        raise cherrypy.HTTPRedirect('/gateway_geoip?msg=Cleared')


    @require_login
    @cherrypy.expose
    def geoip_all(self):
        """Return all saved IP coordinates as a JSON dict {ip: {lat,lng,label}}."""
        import json as _j
        cherrypy.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        try:
            with connect_db() as c:
                rows = c.execute(
                    'SELECT address, lat, lng, label FROM srt_endpoint_location '
                    'WHERE lat IS NOT NULL AND lng IS NOT NULL'
                ).fetchall()
            result = {r[0]: {'lat': r[1], 'lng': r[2], 'label': r[3] or r[0]} for r in rows}
        except Exception as e:
            cherrypy.log(f'[geoip_all] error: {e}')
            result = {}
        return _j.dumps(result).encode('utf-8')


    @require_login
    @cherrypy.expose
    def gateway_report_create(self, host=None, route_id=None, route_name=None,
                               name=None, started_at=None, ended_at=None,
                               tz_offset=None):
        """Create a named report session.
        Dates are stored as LOCAL TIME (as entered by user).
        tz_offset (JS getTimezoneOffset, e.g. -60 for UTC+1) is stored
        so we can convert to/from UTC when querying samples.
        """
        from datetime import datetime as _dtr
        def _s(v): return ((v[-1] if isinstance(v, list) else v) or '').strip()
        host       = _s(host); route_id = _s(route_id); route_name = _s(route_name)
        name       = _s(name) or 'Report'
        started_at = _s(started_at).replace('T', ' ')
        ended_at   = _s(ended_at).replace('T', ' ')
        try: tz_min = int(_s(tz_offset))
        except Exception: tz_min = 0
        if not (host and route_id and started_at and ended_at):
            raise cherrypy.HTTPRedirect(
                f'/gateway_route?host={host}&route_id={route_id}&msg=Missing+fields')
        try:
            _dtr.strptime(started_at[:16], '%Y-%m-%d %H:%M')
            _dtr.strptime(ended_at[:16],   '%Y-%m-%d %H:%M')
        except Exception:
            raise cherrypy.HTTPRedirect(
                f'/gateway_route?host={host}&route_id={route_id}&msg=Invalid+dates')
        created_at = _dtr.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        with connect_db() as c:
            c.execute(
                'INSERT INTO srt_report_session '
                '(device_host, route_id, route_name, name, started_at, ended_at, created_at, tz_offset) '
                'VALUES (?,?,?,?,?,?,?,?)',
                (host, route_id, route_name or route_id, name,
                 started_at, ended_at, created_at, tz_min)
            )
        raise cherrypy.HTTPRedirect(
            f'/gateway_route?host={host}&route_id={route_id}&tab=tabReports&msg=Report+created')

    @require_login
    @cherrypy.expose
    def gateway_reports_json(self, host=None, route_id=None):
        """Return list of report sessions for a route with sample/event counts."""
        import json as _j
        cherrypy.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        if not host or not route_id:
            return _j.dumps({'ok': False}).encode('utf-8')
        with connect_db() as c:
            rows = c.execute(
                'SELECT id, name, started_at, ended_at, created_at '
                'FROM srt_report_session '
                'WHERE device_host=? AND route_id=? ORDER BY id DESC',
                (host, route_id)
            ).fetchall()
            reports = []
            for r in rows:
                rid, name, ts_s, ts_e, ts_c = r
                sc = c.execute(
                    'SELECT COUNT(*) FROM srt_route_sample '
                    'WHERE device_host=? AND route_id=? AND ts >= ? AND ts <= ?',
                    (host, route_id, ts_s, ts_e)
                ).fetchone()[0]
                ec = c.execute(
                    'SELECT COUNT(*) FROM srt_event '
                    'WHERE device_host=? AND route_id=? AND ts >= ? AND ts <= ?',
                    (host, route_id, ts_s, ts_e)
                ).fetchone()[0]
                reports.append({
                    'id': rid, 'name': name,
                    'started_at': ts_s, 'ended_at': ts_e,
                    'created_at': ts_c or '', 'sample_count': sc, 'event_count': ec
                })
        return _j.dumps({'ok': True, 'reports': reports}).encode('utf-8')


    @require_login
    @cherrypy.expose
    def gateway_report_delete(self, report_id=None, host=None, route_id=None):
        """Delete a report session."""
        if report_id:
            with connect_db() as c:
                c.execute('DELETE FROM srt_report_session WHERE id=?', (report_id,))
        raise cherrypy.HTTPRedirect(
            f'/gateway_route?host={host}&route_id={route_id}&tab=tabReports')

    @require_login
    @cherrypy.expose
    def gateway_report_pdf(self, report_id=None, tz_offset=None):
        """Generate a PDF report for a SRT report session.
        Dates in DB: started_at/ended_at are LOCAL TIME, tz_offset is JS getTimezoneOffset().
        Gateway samples are in UTC. We convert local→UTC for the query.
        The PDF shows both local time and UTC.
        """
        import io
        from datetime import datetime as _dtr, timedelta as _tdd
        if not report_id:
            raise cherrypy.HTTPRedirect('/gateway_logs')
        with connect_db() as c:
            rep = c.execute(
                'SELECT id, device_host, route_id, route_name, name, '
                'started_at, ended_at, tz_offset '
                'FROM srt_report_session WHERE id=?', (int(report_id),)
            ).fetchone()
        if not rep:
            raise cherrypy.HTTPRedirect('/gateway_logs')
        rid, host, route_id, route_name, rep_name, ts_start_local, ts_end_local, tz_offset_min = rep
        tz_offset_min = tz_offset_min or 0

        # ── UTC conversion ─────────────────────────────────────────────────
        # tz_offset_min = JS getTimezoneOffset() = UTC - local (minutes)
        # Paris UTC+1: tz_offset_min = -60 → local = UTC + 60min → UTC = local - 60min
        # To go from local → UTC: add tz_offset_min
        def local_to_utc(ts_str):
            dt = _dtr.strptime(str(ts_str)[:16], '%Y-%m-%d %H:%M')
            return (dt + _tdd(minutes=tz_offset_min)).strftime('%Y-%m-%d %H:%M:%S')

        def utc_to_local(ts_str):
            dt = _dtr.strptime(str(ts_str)[:19], '%Y-%m-%d %H:%M:%S')
            return (dt - _tdd(minutes=tz_offset_min)).strftime('%Y-%m-%d %H:%M:%S')

        ts_start_utc = local_to_utc(ts_start_local)
        ts_end_utc   = local_to_utc(ts_end_local)

        # Timezone label e.g. "UTC+1" or "UTC-5"
        tz_hours = -tz_offset_min / 60
        tz_label = f"UTC{'+' if tz_hours >= 0 else ''}{tz_hours:g}"

        with connect_db() as c:
            route_samples = c.execute(
                'SELECT ts, total_bitrate_mbps, src_rtt_ms, src_loss_pct, active_connections '
                'FROM srt_route_sample WHERE device_host=? AND route_id=? '
                'AND ts >= ? AND ts <= ? ORDER BY ts ASC',
                (host, route_id, ts_start_utc, ts_end_utc)
            ).fetchall()
            client_samples_raw = c.execute(
                'SELECT ts, address, direction, bitrate_mbps, rtt_ms, loss_pct, buffer_ms '
                'FROM srt_connection_sample WHERE device_host=? AND route_id=? '
                'AND ts >= ? AND ts <= ? ORDER BY ts ASC',
                (host, route_id, ts_start_utc, ts_end_utc)
            ).fetchall()
            events = c.execute(
                'SELECT ts, event_type, detail, ip_address FROM srt_event '
                'WHERE device_host=? AND route_id=? AND ts >= ? AND ts <= ? ORDER BY ts ASC',
                (host, route_id, ts_start_utc, ts_end_utc)
            ).fetchall()
            gw = c.execute('SELECT name FROM srt_gateway_device WHERE host=?', (host,)).fetchone()
        gw_name  = gw[0] if gw else host
        generated_utc   = _dtr.utcnow().strftime('%d/%m/%Y %H:%M:%S UTC')

        from collections import defaultdict
        client_data    = defaultdict(list)      # addr → [(ts,br,rtt,loss)]
        client_srt_ver = {}                       # addr → latest srt_version
        for row in client_samples_raw:
            ts, addr, direction, br, rtt, loss, buf = row[:7]
            srt_ver = row[7] if len(row) > 7 else ''
            if addr:
                client_data[addr].append((ts, br, rtt, loss))
                if srt_ver:
                    client_srt_ver[addr] = srt_ver

        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import mm
            from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                            Table, TableStyle, HRFlowable, KeepTogether)
            from reportlab.graphics.shapes import Drawing, PolyLine, String, Rect, Line

            styles = getSampleStyleSheet()
            def ps(name, **kw):
                return ParagraphStyle(name, parent=styles['Normal'], **kw)

            buf_io = io.BytesIO()
            doc = SimpleDocTemplate(buf_io, pagesize=A4,
                leftMargin=15*mm, rightMargin=15*mm,
                topMargin=15*mm, bottomMargin=15*mm)

            story = []
            story.append(Paragraph(
                f'SRT Report — {rep_name}',
                ps('h1', fontSize=16, fontName='Helvetica-Bold', spaceAfter=3)))
            story.append(Paragraph(
                f'Route: {route_name} &nbsp;|&nbsp; Gateway: {gw_name} ({host})<br/>'
                f'Period ({tz_label}): {ts_start_local[:16]} → {ts_end_local[:16]}<br/>'
                f'Period (UTC): {ts_start_utc[:16]} → {ts_end_utc[:16]}<br/>'
                f'Generated: {generated_utc}',
                ps('meta', fontSize=8, textColor=colors.grey, spaceAfter=6)))
            story.append(HRFlowable(width='100%', thickness=0.5,
                                    color=colors.lightgrey, spaceAfter=8))

            # ── Mini graph helper ─────────────────────────────────────────
            def mini_graph(samples_xy, w_mm=170, h_mm=36,
                           color=colors.HexColor('#0d6efd'), y_label=''):
                """Draw a graph with time-axis labels every 30 min (local + UTC)."""
                from datetime import datetime as _dg, timedelta as _tg
                vals = [v for _, v in samples_xy if v is not None]
                if not vals:
                    return Paragraph('<i>No data</i>',
                                     ps('nd', fontSize=8, textColor=colors.grey))
                # Reserve bottom area for time labels (local + UTC = 2 rows × 6pt)
                LABEL_H = 16  # points reserved for labels (local + UTC)
                W = w_mm*mm; H = h_mm*mm
                PLOT_H = H - LABEL_H
                d = Drawing(W, H)
                d.add(Rect(0, LABEL_H, W, PLOT_H,
                           fillColor=colors.HexColor('#f8f9fa'),
                           strokeColor=colors.lightgrey, strokeWidth=0.3))
                mn, mx = min(vals), max(vals); rng = mx - mn or 1
                n = len(samples_xy)
                # Build ts → x mapping from first/last timestamps
                ts_list = [s[0] for s in samples_xy if s[0]]
                t0 = ts_list[0] if ts_list else None
                t1 = ts_list[-1] if ts_list else None
                def ts_to_x(ts_str):
                    try:
                        dt = _dg.strptime(str(ts_str)[:19], '%Y-%m-%d %H:%M:%S')
                        dt0 = _dg.strptime(str(t0)[:19], '%Y-%m-%d %H:%M:%S')
                        dt1 = _dg.strptime(str(t1)[:19], '%Y-%m-%d %H:%M:%S')
                        span = (dt1 - dt0).total_seconds() or 1
                        return 6 + (W - 12) * (dt - dt0).total_seconds() / span
                    except Exception:
                        return None
                # Draw polyline — split on gaps to avoid false continuity
                # Detect gap threshold: if ts gap > 3× median interval, break line
                from datetime import datetime as _dgap
                from reportlab.graphics.shapes import PolyLine as PL
                # Compute median sample interval to set gap threshold
                ts_valid = [s[0] for s in samples_xy if s[0] and s[1] is not None]
                if len(ts_valid) >= 2:
                    try:
                        dts = []
                        for _i in range(1, min(20, len(ts_valid))):
                            _a = _dgap.strptime(str(ts_valid[_i-1])[:19], '%Y-%m-%d %H:%M:%S')
                            _b = _dgap.strptime(str(ts_valid[_i])[:19],   '%Y-%m-%d %H:%M:%S')
                            dts.append(abs((_b - _a).total_seconds()))
                        dts.sort()
                        _med_interval = dts[len(dts)//2] if dts else 10
                        _gap_threshold = max(60, _med_interval * 4)
                    except Exception:
                        _gap_threshold = 60
                else:
                    _gap_threshold = 60

                # Build list of segments, breaking on gaps
                segments = []
                current_seg = []
                prev_ts_dt = None
                for i, (ts, v) in enumerate(samples_xy):
                    if v is None: continue
                    x_pos = ts_to_x(ts)
                    if x_pos is None:
                        x_pos = 6 + (W-12)*i/max(n-1, 1)
                    y_pos = LABEL_H + 4 + (PLOT_H-8)*(v-mn)/rng
                    # Check for gap
                    if prev_ts_dt and ts:
                        try:
                            cur_dt = _dgap.strptime(str(ts)[:19], '%Y-%m-%d %H:%M:%S')
                            gap_sec = (cur_dt - prev_ts_dt).total_seconds()
                            if gap_sec > _gap_threshold:
                                if len(current_seg) >= 4:
                                    segments.append(current_seg)
                                current_seg = []
                        except Exception:
                            pass
                    current_seg.extend([x_pos, y_pos])
                    if ts:
                        try:
                            prev_ts_dt = _dgap.strptime(str(ts)[:19], '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            pass
                if len(current_seg) >= 4:
                    segments.append(current_seg)
                for seg in segments:
                    d.add(PL(seg, strokeColor=color, strokeWidth=1.2, strokeLineCap=1))
                # Y-axis labels
                d.add(String(2, H-8, f'{mx:.1f}', fontSize=5, fillColor=colors.grey))
                d.add(String(2, LABEL_H+3, f'{mn:.1f}', fontSize=5, fillColor=colors.grey))
                if y_label:
                    d.add(String(W-2, (H+LABEL_H)/2, y_label, fontSize=5,
                                 fillColor=colors.grey, textAnchor='end'))
                # ── Time axis labels ─────────────────────────────────
                if t0 and t1:
                    try:
                        dt0 = _dg.strptime(str(t0)[:19], '%Y-%m-%d %H:%M:%S')
                        dt1 = _dg.strptime(str(t1)[:19], '%Y-%m-%d %H:%M:%S')
                        span_sec = (dt1 - dt0).total_seconds()
                        # Choose interval based on span
                        if   span_sec <= 600:    _interval_min = 1
                        elif span_sec <= 1800:   _interval_min = 5
                        elif span_sec <= 5400:   _interval_min = 10
                        elif span_sec <= 10800:  _interval_min = 15
                        elif span_sec <= 21600:  _interval_min = 30
                        else:                    _interval_min = 60

                        def _draw_tick(tick_dt, x_t, is_boundary=False):
                            """Draw a tick mark with local+UTC labels."""
                            if x_t is None: return
                            # Clamp to drawing area
                            x_t = max(6.0, min(float(W) - 6.0, float(x_t)))
                            # Vertical grid line (skip at edges)
                            if 8 < x_t < W - 8:
                                d.add(Line(x_t, LABEL_H, x_t, float(H),
                                           strokeColor=colors.HexColor('#cccccc'),
                                           strokeWidth=0.4))
                            # Local time (top label row)
                            loc_dt = tick_dt + _tg(minutes=-tz_offset_min)
                            lbl_loc = loc_dt.strftime('%H:%M')
                            lbl_utc = tick_dt.strftime('%H:%M') + 'Z'
                            anchor = 'middle'
                            if x_t < 15:   anchor = 'start'
                            if x_t > W-15: anchor = 'end'
                            d.add(String(x_t, LABEL_H - 3,
                                         lbl_loc, fontSize=5,
                                         fillColor=colors.HexColor('#0a58ca'),
                                         textAnchor=anchor))
                            d.add(String(x_t, 1.5,
                                         lbl_utc, fontSize=4.5,
                                         fillColor=colors.HexColor('#666666'),
                                         textAnchor=anchor))

                        # Always draw start and end labels
                        _draw_tick(dt0, ts_to_x(dt0.strftime('%Y-%m-%d %H:%M:%S')), True)
                        _draw_tick(dt1, ts_to_x(dt1.strftime('%Y-%m-%d %H:%M:%S')), True)

                        # Draw intermediate ticks
                        minutes = dt0.minute + dt0.second / 60.0
                        offset_to_next = (_interval_min - minutes % _interval_min) % _interval_min
                        if offset_to_next == 0:
                            offset_to_next = _interval_min  # skip dt0, already drawn
                        tick = dt0 + _tg(minutes=offset_to_next)
                        while tick < dt1:
                            x_t = ts_to_x(tick.strftime('%Y-%m-%d %H:%M:%S'))
                            _draw_tick(tick, x_t)
                            tick += _tg(minutes=_interval_min)
                    except Exception as _te:
                        pass  # never crash on label drawing
                return d

            def stats_row(lst):
                if not lst: return '—', '—', '—'
                fmt = lambda v: f'{v:.2f}'
                return fmt(sum(lst)/len(lst)), fmt(max(lst)), fmt(min(lst))

            def section_graphs(samples, label, color):
                elems = []
                for title, idx, col, lbl in [
                    (f'{label} — Bitrate (Mb/s)',  1, color,                           'Mb/s'),
                    (f'{label} — RTT (ms)',         2, colors.HexColor('#198754'),      'ms'),
                    (f'{label} — Loss (%)',          3, colors.HexColor('#dc3545'),      '%'),
                ]:
                    elems.append(Paragraph(title, ps('gh'+label+str(idx),
                        fontSize=9, fontName='Helvetica-Bold', spaceBefore=3, spaceAfter=2)))
                    h = 20 if idx == 3 else 28
                    elems.append(mini_graph([(r[0], r[idx]) for r in samples],
                                            color=col, y_label=lbl, h_mm=h))
                brs   = [r[1] for r in samples if r[1] is not None]
                rtts  = [r[2] for r in samples if r[2] is not None]
                losses= [r[3] for r in samples if r[3] is not None]
                tbl = Table(
                    [['Metric', 'Avg', 'Max', 'Min'],
                     ['Bitrate (Mb/s)', *stats_row(brs)],
                     ['RTT (ms)',       *stats_row(rtts)],
                     ['Loss (%)',       *stats_row(losses)]],
                    colWidths=[50*mm, 35*mm, 35*mm, 35*mm])
                tbl.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f0f0f0')),
                    ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE',   (0,0), (-1,-1), 8),
                    ('GRID',       (0,0), (-1,-1), 0.3, colors.lightgrey),
                    ('TOPPADDING', (0,0), (-1,-1), 3),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 3),
                ]))
                elems.extend([Spacer(1, 2*mm), tbl])
                return elems

            # ── Source ───────────────────────────────────────────────────
            # Fetch latest source SRT version from live payload
            _src_srt_ver = ''
            if SRT_POLLER:
                _lp = (SRT_POLLER.last_payloads.get(host) or {}).get(route_id, {})
                _src_conns = (_lp.get('source') or {}).get('connections') or []
                if _src_conns:
                    _src_srt_ver = _src_conns[0].get('srt_version', '')
                if not _src_srt_ver:
                    _src_srt_ver = (_lp.get('source') or {}).get('srt_version', '')
            if route_samples:
                _src_title = 'Source' + (f' — SRT {_src_srt_ver}' if _src_srt_ver else '')
                story.append(Paragraph(_src_title,
                    ps('sec', fontSize=11, fontName='Helvetica-Bold',
                       spaceBefore=4, spaceAfter=3,
                       textColor=colors.HexColor('#0d6efd'))))
                story.extend(section_graphs(
                    [(r[0], r[1], r[2], r[3]) for r in route_samples],
                    'Source', colors.HexColor('#0d6efd')))
                story.append(Spacer(1, 6*mm))
            else:
                story.append(Paragraph('<i>No source samples in this period.</i>',
                    ps('ns', fontSize=9, textColor=colors.grey, spaceAfter=6)))

            # ── Clients ───────────────────────────────────────────────────
            if client_data:
                story.append(HRFlowable(width='100%', thickness=0.5,
                                        color=colors.lightgrey, spaceAfter=4))
                story.append(Paragraph(f'Clients ({len(client_data)})',
                    ps('sec2', fontSize=11, fontName='Helvetica-Bold',
                       spaceBefore=4, spaceAfter=3,
                       textColor=colors.HexColor('#fd7e14'))))
                cli_colors = ['#fd7e14','#6610f2','#0dcaf0','#20c997',
                              '#d63384','#198754','#ffc107','#0d6efd']
                for ci, (addr, csamples) in enumerate(sorted(client_data.items())):
                    col = colors.HexColor(cli_colors[ci % len(cli_colors)])
                    _cli_ver = client_srt_ver.get(addr, '')
                    _cli_title = f'Client: {addr}' + (f' — SRT {_cli_ver}' if _cli_ver else '')
                    cli_elems = [Paragraph(_cli_title,
                        ps(f'cli{ci}', fontSize=9, fontName='Helvetica-Bold',
                           spaceBefore=4, spaceAfter=2, textColor=col))]
                    cli_elems.extend(section_graphs(csamples, addr, col))
                    cli_elems.append(Spacer(1, 4*mm))
                    story.append(KeepTogether(cli_elems[:3]))
                    story.extend(cli_elems[3:])
                story.append(Spacer(1, 4*mm))
            else:
                story.append(Paragraph('<i>No client samples in this period.</i>',
                    ps('nc', fontSize=9, textColor=colors.grey, spaceAfter=4)))

            # ── Events — dual timezone columns ────────────────────────────
            story.append(HRFlowable(width='100%', thickness=0.5,
                                    color=colors.lightgrey, spaceAfter=4))
            story.append(Paragraph(f'Events ({len(events)})',
                ps('ev', fontSize=11, fontName='Helvetica-Bold',
                   spaceBefore=4, spaceAfter=3)))
            if events:
                cs = ps('cs', fontSize=7)
                ev_data = [[f'{tz_label}', 'UTC', 'Event', 'Detail', 'IP']]
                for ts_utc, etype, detail, ip in events:
                    ts_loc = utc_to_local(str(ts_utc)[:19]) if ts_utc else ''
                    ev_data.append([
                        Paragraph(str(ts_loc)[:16], cs),
                        Paragraph(str(ts_utc)[:16], cs),
                        Paragraph(etype or '', cs),
                        Paragraph(detail or '', cs),
                        Paragraph(ip or '', cs),
                    ])
                evtbl = Table(ev_data,
                              colWidths=[28*mm, 28*mm, 28*mm, 62*mm, 24*mm],
                              repeatRows=1)
                evtbl.setStyle(TableStyle([
                    ('BACKGROUND',    (0,0), (-1,0), colors.HexColor('#f0f0f0')),
                    ('FONTNAME',      (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE',      (0,0), (-1,0), 7),
                    ('GRID',          (0,0), (-1,-1), 0.3, colors.HexColor('#dddddd')),
                    ('ROWBACKGROUNDS',(0,1), (-1,-1),
                     [colors.white, colors.HexColor('#fafafa')]),
                    ('VALIGN',        (0,0), (-1,-1), 'TOP'),
                    ('TOPPADDING',    (0,0), (-1,-1), 2),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                    ('LEFTPADDING',   (0,0), (-1,-1), 3),
                ]))
                story.append(evtbl)
            else:
                story.append(Paragraph('<i>No events in this period.</i>',
                    ps('ne', fontSize=9, textColor=colors.grey)))

            story.append(Spacer(1, 6*mm))
            story.append(Paragraph('StreamPilot — SRT Gateway Report',
                ps('footer', fontSize=7, textColor=colors.grey)))

            doc.build(story)
            pdf_bytes = buf_io.getvalue()
            safe = rep_name.replace(' ', '_').replace('/', '-')[:40]
            cherrypy.response.headers['Content-Type'] = 'application/pdf'
            cherrypy.response.headers['Content-Disposition'] =                 f'attachment; filename="report_{safe}.pdf"'
            return pdf_bytes

        except ImportError:
            raise cherrypy.HTTPError(500, 'reportlab not installed')
        except Exception as _e:
            cherrypy.log(f'[gateway_report_pdf] error: {_e}')
            raise cherrypy.HTTPError(500, str(_e))


    @require_login
    @cherrypy.expose
    def gateway_report_delete(self, report_id=None, host=None, route_id=None):
        """Delete a report session."""
        if report_id:
            with connect_db() as c:
                c.execute('DELETE FROM srt_report_session WHERE id=?', (report_id,))
        raise cherrypy.HTTPRedirect(
            f'/gateway_route?host={host}&route_id={route_id}&tab=tabReports')

    @require_login
    @cherrypy.expose
    def gateway_slack_status(self, host=None):
        import json as _j
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not host:
            return _j.dumps({"configured": False}).encode("utf-8")
        try:
            with connect_db() as c:
                sc = c.execute("SELECT webhook_url FROM slack_config WHERE id=1").fetchone()
                if not (sc and sc[0] and sc[0].strip()):
                    return _j.dumps({"configured": False}).encode("utf-8")
                gw = c.execute("SELECT id FROM srt_gateway_device WHERE host=?", (host,)).fetchone()
                if not gw:
                    return _j.dumps({"configured": False}).encode("utf-8")
                gs = c.execute("SELECT notify_paused FROM srt_gw_slack WHERE gw_id=?",
                               (gw[0],)).fetchone()
                paused = bool(gs and gs[0])
        except Exception:
            return _j.dumps({"configured": False}).encode("utf-8")
        return _j.dumps({"configured": True, "paused": paused}).encode("utf-8")


    @require_login
    @cherrypy.expose
    def slack_status(self, device_id=None):
        import json
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        if not device_id:
            return json.dumps({"configured": False}).encode("utf-8")
        try:
            did = int(device_id)
        except Exception:
            return json.dumps({"configured": False}).encode("utf-8")
        try:
            with connect_db() as c:
                sc = c.execute("SELECT webhook_url FROM slack_config WHERE id=1").fetchone()
                configured = bool(sc and sc[0] and sc[0].strip())
                if not configured:
                    return json.dumps({"configured": False}).encode("utf-8")
                ds = c.execute(
                    "SELECT notify_paused FROM device_slack WHERE device_id=?", (did,)
                ).fetchone()
                paused = bool(ds and ds[0])
        except Exception:
            return json.dumps({"configured": False}).encode("utf-8")
        return json.dumps({"configured": True, "paused": paused}).encode("utf-8")


    @require_login
    @cherrypy.expose
    def settings(self, msg=None):
        import json as _j, html as _h
        with connect_db() as c:
            sc = c.execute("SELECT webhook_url,channel,username FROM slack_config WHERE id=1").fetchone()
            devs = c.execute("SELECT id,name,host FROM devices ORDER BY id ASC").fetchall()
            ds_map = {}
            for d in devs:
                c.execute("INSERT OR IGNORE INTO device_slack(device_id) VALUES(?)", (d[0],))
                _SCOLS = ('device_id,notify_session,notify_drops,notify_owd_threshold,'
                          'notify_bitrate_min,notify_poller_error,notify_logs,notify_connection,notify_paused,ignore_contains')
                r = c.execute('SELECT ' + _SCOLS + ' FROM device_slack WHERE device_id=?', (d[0],)).fetchone()
                if r:
                    ds_map[d[0]] = dict(zip(_SCOLS.split(','), r))

        def esc(x): return _h.escape('' if x is None else str(x))
        def chk(v): return 'checked' if v else ''

        webhook  = sc[0] if sc else ''
        channel  = sc[1] if sc else '#streampilot'
        username = sc[2] if sc else 'StreamPilot'

        dev_html = ''
        for did, dname, dhost in devs:
            ds = ds_map.get(did, {})
            paused = bool(ds.get('notify_paused'))
            try:
                ignore_txt = '\n'.join(_j.loads(ds.get('ignore_contains') or '[]'))
            except Exception:
                ignore_txt = ''
            badge = '<span class="badge text-bg-danger ms-2">Notifications OFF</span>' if paused else '<span class="badge text-bg-success ms-2">Notifications ON</span>'
            pause_btn = (
                f'''<form class="d-inline ms-2" method="post" action="/settings_device_resume">
                  <input type="hidden" name="device_id" value="{did}">
                  <button class="btn btn-sm btn-success" type="submit">Resume notifications</button>
                </form>'''
                if paused else
                f'''<form class="d-inline ms-2" method="post" action="/settings_device_pause">
                  <input type="hidden" name="device_id" value="{did}">
                  <button class="btn btn-sm btn-danger" type="submit">Stop notifications</button>
                </form>'''
            )
            dev_html += f'''
            <div class="card mb-3">
              <div class="card-header d-flex align-items-center">
                <strong>{esc(dname)}</strong>
                <span class="text-muted fw-normal small ms-2">({esc(dhost)})</span>
                {badge}
                {pause_btn}
              </div>
              <div class="card-body">
                <form method="post" action="/settings_device_save">
                  <input type="hidden" name="device_id" value="{did}">
                  <div class="row g-2 mb-2">
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_session" id="ns_{did}" {chk(ds.get("notify_session",1))}>
                      <label class="form-check-label" for="ns_{did}">Session start/stop</label>
                    </div></div>
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_drops" id="nd_{did}" {chk(ds.get("notify_drops",1))}>
                      <label class="form-check-label" for="nd_{did}">Dropped packets</label>
                    </div></div>
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_poller_error" id="np_{did}" {chk(ds.get("notify_poller_error",1))}>
                      <label class="form-check-label" for="np_{did}">Poller error</label>
                    </div></div>
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_logs" id="nl_{did}" {chk(ds.get("notify_logs",1))}>
                      <label class="form-check-label" for="nl_{did}">Forward all logs</label>
                    </div></div>
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_connection" id="nc_{did}" {chk(ds.get("notify_connection",1))}>
                      <label class="form-check-label" for="nc_{did}">Transmitter connection / disconnection</label>
                    </div></div>
                  </div>
                  <div class="row g-2 mb-2">
                    <div class="col-md-4">
                      <label class="form-label small">OWD alert threshold (ms, 0=off)</label>
                      <input class="form-control form-control-sm" type="number" name="notify_owd_threshold" min="0" value="{esc(ds.get('notify_owd_threshold') or 0)}">
                    </div>
                    <div class="col-md-4">
                      <label class="form-label small">Bitrate min alert (kb/s, 0=off)</label>
                      <input class="form-control form-control-sm" type="number" name="notify_bitrate_min" min="0" value="{esc(ds.get('notify_bitrate_min') or 0)}">
                    </div>
                  </div>
                  <div class="mb-2">
                    <label class="form-label small">Ignore contains (one filter per line)</label>
                    <textarea class="form-control form-control-sm" name="ignore_contains" rows="4" style="font-family:monospace;font-size:0.78rem">{esc(ignore_txt)}</textarea>
                  </div>
                  <button class="btn btn-sm btn-primary" type="submit">Save</button>
                </form>
              </div>
            </div>'''

        # SRT Gateway notification settings
        gw_html = ''
        with connect_db() as _cgwdb:
            gw_rows = _cgwdb.execute(
                'SELECT id, name, host FROM srt_gateway_device ORDER BY id ASC'
            ).fetchall()
        for gid, gname, ghost in gw_rows:
            with connect_db() as _cgs:
                _cgs.execute('INSERT OR IGNORE INTO srt_gw_slack(gw_id) VALUES(?)', (gid,))
                gs = dict(zip(
                    ['notify_source','notify_client','notify_paused',
                     'thr_bitrate_min','thr_loss_max','thr_lost_max',
                     'thr_dropped_max','thr_retransmit_max'],
                    _cgs.execute(
                        'SELECT notify_source,notify_client,notify_paused,'
                        'thr_bitrate_min,thr_loss_max,thr_lost_max,'
                        'thr_dropped_max,thr_retransmit_max '
                        'FROM srt_gw_slack WHERE gw_id=?', (gid,)
                    ).fetchone() or (1,1,0,0,0,0,0,0)
                ))
            gpaused = bool(gs.get('notify_paused'))
            gbadge = ('<span class="badge text-bg-danger ms-2">Notifications OFF</span>'
                      if gpaused else
                      '<span class="badge text-bg-success ms-2">Notifications ON</span>')
            gpause_btn = (
                f'''<form class="d-inline ms-2" method="post" action="/settings_gw_resume">
                  <input type="hidden" name="gw_id" value="{gid}">
                  <button class="btn btn-sm btn-success" type="submit">Resume</button>
                </form>''' if gpaused else
                f'''<form class="d-inline ms-2" method="post" action="/settings_gw_pause">
                  <input type="hidden" name="gw_id" value="{gid}">
                  <button class="btn btn-sm btn-danger" type="submit">Pause</button>
                </form>''')
            gw_html += f'''
            <div class="card mb-3">
              <div class="card-header d-flex align-items-center">
                <strong>{esc(gname)}</strong>
                <span class="text-muted fw-normal small ms-2">({esc(ghost)})</span>
                {gbadge}{gpause_btn}
              </div>
              <div class="card-body">
                <form method="post" action="/settings_gw_save">
                  <input type="hidden" name="gw_id" value="{gid}">
                  <div class="row g-2 mb-3">
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_source" id="gs_{gid}" {chk(gs.get('notify_source',1))}>
                      <label class="form-check-label" for="gs_{gid}">Source connected / disconnected</label>
                    </div></div>
                    <div class="col-auto"><div class="form-check">
                      <input class="form-check-input" type="checkbox" name="notify_client" id="gc_{gid}" {chk(gs.get('notify_client',1))}>
                      <label class="form-check-label" for="gc_{gid}">Client connected / disconnected</label>
                    </div></div>
                  </div>
                  <div class="row g-2 mb-3">
                    <div class="col-md-3">
                      <label class="form-label small">Bitrate min (Mb/s, 0=off)</label>
                      <input class="form-control form-control-sm" type="number" step="0.1" min="0"
                             name="thr_bitrate_min" value="{esc(gs.get('thr_bitrate_min') or 0)}">
                    </div>
                    <div class="col-md-3">
                      <label class="form-label small">Loss max (%, 0=off)</label>
                      <input class="form-control form-control-sm" type="number" step="0.1" min="0"
                             name="thr_loss_max" value="{esc(gs.get('thr_loss_max') or 0)}">
                    </div>
                    <div class="col-md-2">
                      <label class="form-label small">Lost pkts max (0=off)</label>
                      <input class="form-control form-control-sm" type="number" min="0"
                             name="thr_lost_max" value="{esc(gs.get('thr_lost_max') or 0)}">
                    </div>
                    <div class="col-md-2">
                      <label class="form-label small">Dropped pkts max (0=off)</label>
                      <input class="form-control form-control-sm" type="number" min="0"
                             name="thr_dropped_max" value="{esc(gs.get('thr_dropped_max') or 0)}">
                    </div>
                    <div class="col-md-2">
                      <label class="form-label small">Retransmit max (b/s, 0=off)</label>
                      <input class="form-control form-control-sm" type="number" min="0"
                             name="thr_retransmit_max" value="{esc(gs.get('thr_retransmit_max') or 0)}">
                    </div>
                  </div>
                  <button class="btn btn-sm btn-primary" type="submit">Save</button>
                </form>
              </div>
            </div>'''

        msg_html = f'<div class="alert alert-success">{esc(msg)}</div>' if msg else ''
        body = f'''<!doctype html><html><head>
          <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
          <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
          <title>Settings — StreamPilot</title>
        </head><body class="p-3">
          <div class="d-flex justify-content-between align-items-center mb-3">
            <h1 class="h4 m-0">Settings</h1>
            <a class="btn btn-outline-secondary" href="/">← Dashboard</a>
          </div>
          {msg_html}
          <h2 class="h5 mb-3">Slack notifications</h2>
          <div class="card mb-4">
            <div class="card-header fw-semibold">Global configuration</div>
            <div class="card-body">
              <form method="post" action="/settings_slack_save">
                <div class="row g-3">
                  <div class="col-md-6">
                    <label class="form-label">Webhook URL</label>
                    <input class="form-control" name="webhook_url" type="url"
                           placeholder="https://hooks.slack.com/services/..." value="{esc(webhook)}">
                  </div>
                  <div class="col-md-3">
                    <label class="form-label">Channel</label>
                    <input class="form-control" name="channel" placeholder="#streampilot" value="{esc(channel)}">
                  </div>
                  <div class="col-md-3">
                    <label class="form-label">Username</label>
                    <input class="form-control" name="username" placeholder="StreamPilot" value="{esc(username)}">
                  </div>
                </div>
                <div class="mt-3 d-flex gap-2">
                  <button class="btn btn-primary" type="submit">Save</button>
                  <a class="btn btn-outline-secondary" href="/settings_slack_test">Send test notification</a>
                </div>
              </form>
            </div>
          </div>
          <h2 class="h5 mb-3">Per-device settings</h2>
          {dev_html or '<div class="text-muted">No devices configured.</div>'}
          <h2 class="h5 mb-3 mt-4">SRT Gateway notifications</h2>
          {gw_html or '<div class="text-muted">No SRT Gateway configured.</div>'}
        </body></html>'''
        cherrypy.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return body.encode('utf-8')

    @require_login
    @cherrypy.expose
    def settings_slack_save(self, webhook_url='', channel='#streampilot', username='StreamPilot'):
        with connect_db() as c:
            c.execute("UPDATE slack_config SET webhook_url=?,channel=?,username=? WHERE id=1",
                      (webhook_url.strip(), channel.strip() or '#streampilot', username.strip() or 'StreamPilot'))
        raise cherrypy.HTTPRedirect("/settings?msg=Slack+config+saved")

    @require_login
    @cherrypy.expose
    def settings_slack_test(self):
        cfg = POLLER._load_slack_cfg() if POLLER else None
        if not cfg:
            raise cherrypy.HTTPRedirect("/settings?msg=No+webhook+configured")
        POLLER._slack_post(cfg[0], ':wave: *StreamPilot* — Hello World! Test notification.', cfg[1], cfg[2], color='#439FE0')
        raise cherrypy.HTTPRedirect("/settings?msg=Test+notification+sent")

    @require_login
    @cherrypy.expose
    def settings_device_save(self, device_id=None, notify_session=None, notify_drops=None,
                              notify_owd_threshold='0', notify_bitrate_min='0',
                              notify_poller_error=None, notify_logs=None,
                              notify_connection=None, ignore_contains=''):
        import json as _j
        if not device_id:
            raise cherrypy.HTTPRedirect("/settings")
        try: did = int(device_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        ns  = 1 if notify_session      else 0
        nd  = 1 if notify_drops        else 0
        npe = 1 if notify_poller_error else 0
        nl  = 1 if notify_logs         else 0
        nc  = 1 if notify_connection   else 0
        try: owd = max(0, int(notify_owd_threshold or 0))
        except: owd = 0
        try: rbm = max(0, int(notify_bitrate_min or 0))
        except: rbm = 0
        lines = [l.strip() for l in (ignore_contains or '').splitlines() if l.strip()]
        ign = _j.dumps(lines, ensure_ascii=False)
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO device_slack(device_id) VALUES(?)", (did,))
            c.execute("UPDATE device_slack SET notify_session=?,notify_drops=?,"
                      "notify_owd_threshold=?,notify_bitrate_min=?,"
                      "notify_poller_error=?,notify_logs=?,notify_connection=?,ignore_contains=? WHERE device_id=?",
                      (ns, nd, owd, rbm, npe, nl, nc, ign, did))
        raise cherrypy.HTTPRedirect("/settings?msg=Device+settings+saved")

    @require_login
    @cherrypy.expose
    def settings_device_pause(self, device_id=None):
        if not device_id: raise cherrypy.HTTPRedirect("/settings")
        try: did = int(device_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO device_slack(device_id) VALUES(?)", (did,))
            c.execute("UPDATE device_slack SET notify_paused=1 WHERE device_id=?", (did,))
        raise cherrypy.HTTPRedirect("/settings?msg=Notifications+paused")

    @require_login
    @cherrypy.expose
    def settings_device_resume(self, device_id=None):
        if not device_id: raise cherrypy.HTTPRedirect("/settings")
        try: did = int(device_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO device_slack(device_id) VALUES(?)", (did,))
            c.execute("UPDATE device_slack SET notify_paused=0 WHERE device_id=?", (did,))
        raise cherrypy.HTTPRedirect("/settings?msg=Notifications+resumed")


    @require_login
    @cherrypy.expose
    def settings_gw_save(self, gw_id=None, notify_source=None, notify_client=None,
                          thr_bitrate_min='0', thr_loss_max='0', thr_lost_max='0',
                          thr_dropped_max='0', thr_retransmit_max='0'):
        if not gw_id:
            raise cherrypy.HTTPRedirect("/settings")
        try: gid = int(gw_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        ns = 1 if notify_source else 0
        nc = 1 if notify_client  else 0
        def _f(v):
            try: return max(0.0, float(v or 0))
            except: return 0.0
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO srt_gw_slack(gw_id) VALUES(?)", (gid,))
            c.execute("UPDATE srt_gw_slack SET notify_source=?,notify_client=?,"
                      "thr_bitrate_min=?,thr_loss_max=?,thr_lost_max=?,"
                      "thr_dropped_max=?,thr_retransmit_max=? WHERE gw_id=?",
                      (ns, nc, _f(thr_bitrate_min), _f(thr_loss_max), _f(thr_lost_max),
                       _f(thr_dropped_max), _f(thr_retransmit_max), gid))
        raise cherrypy.HTTPRedirect("/settings?msg=SRT+Gateway+settings+saved")

    @require_login
    @cherrypy.expose
    def settings_gw_pause(self, gw_id=None):
        if not gw_id: raise cherrypy.HTTPRedirect("/settings")
        try: gid = int(gw_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO srt_gw_slack(gw_id) VALUES(?)", (gid,))
            c.execute("UPDATE srt_gw_slack SET notify_paused=1 WHERE gw_id=?", (gid,))
        raise cherrypy.HTTPRedirect("/settings?msg=SRT+Gateway+notifications+paused")

    @require_login
    @cherrypy.expose
    def settings_gw_resume(self, gw_id=None):
        if not gw_id: raise cherrypy.HTTPRedirect("/settings")
        try: gid = int(gw_id)
        except Exception: raise cherrypy.HTTPRedirect("/settings")
        with connect_db() as c:
            c.execute("INSERT OR IGNORE INTO srt_gw_slack(gw_id) VALUES(?)", (gid,))
            c.execute("UPDATE srt_gw_slack SET notify_paused=0 WHERE gw_id=?", (gid,))
        raise cherrypy.HTTPRedirect("/settings?msg=SRT+Gateway+notifications+resumed")


def run():
    _init_db()  # create tables/indexes once at startup
    # Attach CORS headers to every response (including 3xx redirects)
    def _cors():
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
    cherrypy.tools.cors = cherrypy.Tool('before_finalize', _cors, priority=60)

    port = int(os.getenv("StreamPilot", "5555"))
    mode = (os.getenv("SP_MODE") or "http").strip().lower()
    proxy_mode = (mode == "proxy")
    # Warn if default credentials are used
    _u, _p = _get_credentials()
    if _u == 'admin' and _p == 'admin':
        cherrypy.log('[auth] WARNING: using default credentials admin/admin — set -user and -password')
    if proxy_mode:
        cherrypy.log('[mode] Running behind reverse proxy (HTTPS) — proxy headers enabled, Secure cookie flag on')
    else:
        cherrypy.log('[mode] Running in direct HTTP mode')
    cfg = {
        "server.socket_port": port,
        "server.socket_host": "0.0.0.0",
        "tools.response_headers.on": True,
        "tools.response_headers.headers": [
            ("Access-Control-Allow-Origin", "*"),
        ],
        "tools.cors.on": True,
        "server.thread_pool": 32,
        "server.socket_timeout": 5,
        "tools.sessions.on": True,
        "tools.sessions.timeout": int(os.getenv("SP_SESSION_TIMEOUT_MIN", "480")),
        "tools.gzip.on": True,
        "tools.encode.on": True,
        "tools.encode.encoding": "utf-8",
        "tools.sessions.httponly": True,
        "log.screen": True,
        "tools.sessions.secret": os.getenv("SP_SESSION_SECRET", secrets.token_hex(16)),
    }
    if proxy_mode:
        cfg["tools.proxy.on"] = True
        cfg["tools.proxy.local"] = "X-Forwarded-Host"
        cfg["tools.proxy.remote"] = "X-Forwarded-For"
        cfg["tools.proxy.scheme"] = "X-Forwarded-Proto"
        cfg["tools.sessions.secure"] = True
    cherrypy.config.update(cfg)

    # Start background poller so sessions start/stop even when Dashboard is not open
    global POLLER, SRT_POLLER
    POLLER = BackgroundPoller(DB_PATH, interval=2)
    POLLER.start()
    SRT_POLLER = SRTGatewayPoller(DB_PATH, interval=5)
    SRT_POLLER.start()

    # Ensure clean shutdown
    def _on_stop():
        try:
            if POLLER:
                POLLER.stop()
        except Exception:
            pass
        try:
            if SRT_POLLER:
                SRT_POLLER.stop()
        except Exception:
            pass
    cherrypy.engine.subscribe('stop', _on_stop)

    # Enable static file serving for /static if not already enabled
    static_dir = BASE_DIR / 'static'
    static_dir.mkdir(parents=True, exist_ok=True)
    cherrypy.tree.mount(None, '/static', {'/': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': str(static_dir)
    }})
    cherrypy.quickstart(App())

if __name__ == "__main__":
    run()
