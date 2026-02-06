# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Alexandre Licinio
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
import base64, hashlib as _hl
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    
import os, time, sqlite3, cherrypy
import hashlib, secrets, functools
import threading
import random
from collections import deque
from mako.lookup import TemplateLookup
from collect.scripts.streamhub import fetch_streamhub
try:
    from .logger import LiveLogger  # cas package
except Exception:
    from logger import LiveLogger    # cas script direct

BASE_DIR = Path(__file__).resolve().parent
ROOT = BASE_DIR.parent.parent  # project root
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
                        except Exception as obs_err:
                            cherrypy.log(f"[poller] observe_payload error: {obs_err}")
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
                                    dt = datetime.fromisoformat(str(last_ts)[:19])
                                    age_sec = max(0, int((datetime.now() - dt).total_seconds()))
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

def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None => autocommit explicite, utile pour WAL
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, isolation_level=None, timeout=2.0)
    # PRAGMA de perf / robustesse
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=2000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")  # ~20Mo
    # Schéma minimal (tel que déjà en place chez toi)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS devices ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " name TEXT, protocol TEXT, host TEXT, port INTEGER, api_path TEXT, token TEXT)"
    )
    _ensure_db_indexes(conn)
    return conn

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




def _license_is_valid():
    return True

def _license_badge_html():
    return ''

def _license_expired_html():
    return b''

def current_user():
    return {'username': 'public'}

def require_login(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
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

class App:
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
                        # ts stored as ISO, parse lightweight: 'YYYY-MM-DDTHH:MM:SS'
                        from datetime import datetime
                        dt = datetime.fromisoformat(str(last_ts)[:19])
                        age_sec = max(0, int((datetime.now() - dt).total_seconds()))
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

        def spark_svg(host):
            # Build a tiny SVG sparkline (120x24) from POLLER.age_history[host]
            try:
                hist = (POLLER.age_history.get(host) if POLLER and getattr(POLLER, 'age_history', None) else None)
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
            f'<div class="mb-1"><strong>Sessions:</strong> {sessions_total} <span class="text-muted">(active {sessions_active})</span></div>',
            f'<div class="mb-0"><strong>Samples:</strong> {samples_total}' + (f' <span class="text-muted">(last: {esc(last_sample_any)})</span>' if last_sample_any else '') + '</div>',
            '<div class="mt-2"><a class="btn btn-sm btn-outline-dark" href="/logs_ui">Open logs</a></div>',
            '</div></div>',
            '<div class="table-responsive">',
            '<table class="table table-sm align-middle">',
            '<thead><tr>',
            '<th>ID</th><th>Name</th><th>Host</th><th>Port</th><th>Last sample ts</th><th>Age</th><th>Spark</th><th>Active sessions</th><th>Actions</th>',
            '</tr></thead><tbody>'
        ]
        if rows:
            for (did, name, host, port, last_ts, active, age_sec) in rows:
                body.append('<tr>')
                body.append(f'<td>{did}</td>')
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
            body.append('<tr><td colspan="9" class="text-muted">No devices</td></tr>')
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
        return render("devices.html", devices=devices, page_title="Devices", user=current_user())

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
              tsStart.textContent = samples[0].ts;
              tsEnd.textContent = samples[samples.length-1].ts;
              function renderAt(i) {
                const s = samples[i];
                tsNow.textContent = s.ts;
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
                    // Re-poll session samples
                    await repoll();
                    // Also ping /data to drive backend LOGGER.observe_payload
                    if (deviceRowId !== null && deviceRowId !== 'null') {
                      fetch('/data?id=' + deviceRowId).catch(()=>{});
                    }
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
        
def run():
    port = int(os.getenv("StreamPilot", "5555"))
    cherrypy.config.update({
        "server.socket_port": port,
        "server.socket_host": "0.0.0.0",
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
    })

    # Start background poller so sessions start/stop even when Dashboard is not open
    global POLLER
    POLLER = BackgroundPoller(DB_PATH, interval=2)
    POLLER.start()

    # Ensure clean shutdown
    def _on_stop():
        try:
            if POLLER:
                POLLER.stop()
        except Exception:
            pass
    cherrypy.engine.subscribe('stop', _on_stop)

    # Enable static file serving for /static if not already enabled
    cherrypy.tree.mount(None, '/static', {'/': {'tools.staticdir.on': True, 'tools.staticdir.dir': str(ROOT / 'static')}})
    cherrypy.quickstart(App())

if __name__ == "__main__":
    run()
