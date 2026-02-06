# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Alexandre Licinio
"""StreamHub collector – robust snapshot for mini-probe.

Returns (ok: bool, payload|str).
- Tolerates wrong content-types (text/html containing JSON)
- Normalises inputs (list or dict), status (int or string labels)
- Gathers preview/streamStats/linkStats for SST inputs that are ON
"""
from __future__ import annotations

import json
import os
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Any, Dict, Tuple

# --- Public API
__all__ = ["fetch_streamhub"]

# --- Static maps (aligned with original probe)
STATUS_MAP = {0: "off", 1: "idle", 2: "on", 3: "error", 4: "error"}
RECORDER_MAP = {0: "disabled", 1: "off", 2: "on", 3: "error", 4: "running"}
INTERCOM_MAP = {0: "disabled", 1: "off", 2: "on", 3: "error"}
INTERCOM_PROFILE_MAP = {0: "low", 1: "medium", 2: "high"}
VIDEO_IFB_PRESET_MAP = {0: "off", 1: "on"}

# --- Shared HTTP session (connection pooling + light retries)
_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "StreamPilot/1.0"})
_POOL = int(os.getenv("SP_HTTP_POOL", "50"))  # total sockets per scheme
_RETRY = Retry(
    total=2, backoff_factor=0.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=False  # retry on all idempotent-ish by our usage (GETs)
)
_ADAPTER = HTTPAdapter(pool_connections=_POOL, pool_maxsize=_POOL, max_retries=_RETRY)
_HTTP.mount("http://", _ADAPTER)
_HTTP.mount("https://", _ADAPTER)


# ===== Helpers

def _full_url(base: str, path: str, token: str) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}?api_key={token}"


def _get_json_or_text(session: requests.Session, url: str, timeout: int = 5) -> Tuple[bool, Any, Dict[str, Any]]:
    """GET url -> (is_json, data, meta)
    Never raises JSON decode; attempts to parse JSON even if content-type is wrong.
    """
    headers = {"Accept": "application/json", "Content-Type": "application/json; charset=utf-8"}
    try:
        r = session.get(url, headers=headers, timeout=(2, timeout), verify=False, allow_redirects=False)
    except Exception as e:  # network error
        return False, f"REQUEST_ERROR: {type(e).__name__}: {e}", {"url": url, "status": None, "ctype": None}

    ctype = (r.headers.get("content-type") or "").lower()
    meta = {"url": url, "status": r.status_code, "ctype": ctype}

    # Redirects (often to /login)
    if r.status_code in (301, 302, 303, 307, 308):
        return False, f"REDIRECT {r.status_code} to {r.headers.get('location')}", meta

    # Proper JSON
    if "application/json" in ctype:
        try:
            return True, r.json(), meta
        except Exception as e:
            return False, f"JSON_PARSE_ERROR: {type(e).__name__}: {e}", meta

    # Try to parse body anyway if it looks like JSON
    try:
        body = r.text if isinstance(r.text, str) else ""
    except Exception:
        body = ""
    probe = body.lstrip() if body else ""
    if probe.startswith("{") or probe.startswith("["):
        try:
            return True, json.loads(probe), meta
        except Exception:
            pass

    # Fallback diagnostic
    preview = (body[:400] if body else "").replace("\n", " ")
    return False, f"NON_JSON_RESPONSE (ctype={ctype}) preview={preview}", meta


# --- Logs helpers (for start/stop detection via StreamHub logs)
def _fetch_logs(session: requests.Session, base_url: str, token: str, timeout: int = 5, limit: int = 200):
    """Try multiple likely endpoints to get recent logs; normalize to list[str]."""
    paths = []
    envp = os.getenv("SP_SH_LOG_PATH")
    if envp:
        paths.append(envp)
    # common guesses if env not set
    paths += ["/logs", "/systemLogs", "/systemLog", "/log"]
    seen = set()
    for p in paths:
        if p in seen:
            continue
        seen.add(p)
        url = _full_url(base_url, f"{p}?limit={int(limit)}", token)
        ok, data, _ = _get_json_or_text(session, url, timeout=timeout)
        if not ok:
            continue
        msgs = []
        if isinstance(data, dict):
            src_list = None
            for key in ("logs", "entries", "data", "items", "list"):
                arr = data.get(key)
                if isinstance(arr, list):
                    src_list = arr
                    break
            if src_list is None:
                src_list = []
        elif isinstance(data, list):
            src_list = data
        else:
            src_list = []
        for it in src_list:
            if isinstance(it, str):
                msgs.append(it)
            elif isinstance(it, dict):
                m = it.get("message") or it.get("msg") or it.get("text") or it.get("log") or it.get("content")
                if m is not None:
                    msgs.append(str(m))
        if msgs:
            return True, msgs
    return False, []

# patterns
_PAT_START = re.compile(r"is\s+starting\s+a\s+live", re.IGNORECASE)
_PAT_STOP1 = re.compile(r"Live\s+is\s+stopped", re.IGNORECASE)
_PAT_STOP2 = re.compile(r"Disconnection\s+of", re.IGNORECASE)
_PAT_AVI = re.compile(r"product's\s+name", re.IGNORECASE)

def _detect_live_event_for_input(log_lines, source_index: int, identifier: str | None = None):
    """Return 'start' / 'stop' / None based on recent log lines.
    We match either "Source #<N>" or the input identifier when present.
    """
    if not log_lines:
        return None
    probe_tag = f"Source #{source_index}:"
    for line in reversed(log_lines[-500:]):  # scan most recent first
        if not isinstance(line, str):
            continue
        if probe_tag in line or (identifier and identifier in line):
            if _PAT_START.search(line) and _PAT_AVI.search(line):
                return "start"
            if _PAT_STOP1.search(line) or _PAT_STOP2.search(line):
                return "stop"
    return None


def _norm_status_code(item: Dict[str, Any]) -> int:
    """Accepts int, numeric-string, or labels (off/idle/on/error)."""
    if not item:
        return 0
    v = item.get("channelStatus", item.get("channelState", 0))
    if isinstance(v, str):
        s = v.strip().lower()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                return 0
        return {"off": 0, "idle": 1, "on": 2, "error": 3}.get(s, 0)
    try:
        return int(v)
    except Exception:
        return 0


# ===== Main collector

def fetch_streamhub(base_url: str, token: str | None = None, timeout: int = 5) -> Tuple[bool, Any]:
    """
    Snapshot StreamHub (compatible with legacy data_sql structure).
    Returns (ok, payload|error_string).
    """
    if not token:
        return False, "missing api_key token"

    session = _HTTP

    # 1) Characteristics (nbChannel)
    ok0, data0, meta0 = _get_json_or_text(session, _full_url(base_url, "/", token), timeout=timeout)
    if not ok0:
        return False, f"[{meta0['url']}] status={meta0['status']} ctype={meta0['ctype']} -> {data0}"
    sh_char = data0 if isinstance(data0, dict) else {}
    try:
        nb = int(sh_char.get("nbChannel", 0))
    except Exception:
        nb = 0

    # 2) Config / Outputs / Inputs
    ok_cfg, sh_cfg, meta_cfg = _get_json_or_text(session, _full_url(base_url, "/config", token), timeout=timeout)
    if not ok_cfg:
        return False, f"[{meta_cfg['url']}] status={meta_cfg['status']} ctype={meta_cfg['ctype']} -> {sh_cfg}"

    ok_out, sh_outputs, meta_out = _get_json_or_text(session, _full_url(base_url, "/outputs", token), timeout=timeout)
    if not ok_out:
        return False, f"[{meta_out['url']}] status={meta_out['status']} ctype={meta_out['ctype']} -> {sh_outputs}"

    ok_in, sh_inputs, meta_in = _get_json_or_text(session, _full_url(base_url, "/inputs", token), timeout=timeout)
    if not ok_in:
        return False, f"[{meta_in['url']}] status={meta_in['status']} ctype={meta_in['ctype']} -> {sh_inputs}"

    # 2b) Optional: recent logs to help detect start/stop events
    try:
        _log_limit = int(os.getenv("SP_SH_LOG_LIMIT", "200"))
    except Exception:
        _log_limit = 200
    logs_ok, log_lines = _fetch_logs(session, base_url, token, timeout=timeout, limit=_log_limit)

    # 3) Build payload
    payload: Dict[str, Any] = {"characteristics": sh_char, "configuration": sh_cfg, "inputs": {"channels": nb}}

    # Pre-allocate inputs
    for idx in range(nb):
        payload["inputs"][str(idx)] = {
            "live_departure_time": None,
            "live_duration": None,
            "data_consumed": None,
            "source_identifier": None,
            "source_name": None,
            "dropped_packets_total_live": None,
            "message_for_notification": None,
            "zero": {"dropped_packets": None, "data": None},
            "notifications": {"dropped": {"video": None, "audio": None, "ts": None}},
            "status_change": None,
            "name": None,
            "identifier": None,
            "status": None,
            "version": None,
            "intercom_status": None,
            "intercom_profile": None,
            "recorder_status": None,
            "message": None,
            "thumbnail": None,
            "audio_levels": {},
            "info": None,
            "protocol": None,
            "family_name": None,
            "links": {},
            "total_data": 0,
            "low_rx_bitrate": False,
            "low_tx_bitrate": False,
            "dropped": {},
            "sdi_outputs": {},
            "ip_outputs": {},
            "video_ifb": {"video_ifb_preset_status": None, "video_ifb_decoding_source": None, "video_ifb_decoder": None},
            "encoders": {},
            "locationStatus": None,
            "latitude": None,
            "longitude": None,
        }

    # Inputs normalisation (list or dict)
    raw_inputs = (sh_inputs or {}).get("inputs", sh_inputs or [])
    if isinstance(raw_inputs, dict):
        inputs_list = [raw_inputs[k] or {} for k in sorted(raw_inputs.keys(), key=lambda x: int(x) if str(x).isdigit() else x)]
    elif isinstance(raw_inputs, list):
        inputs_list = raw_inputs
    else:
        inputs_list = []

    outs_sdi = (sh_outputs or {}).get("output", [])
    outs_ip = (sh_outputs or {}).get("IPOutput", [])

    counters = {"on": 0, "idle": 0, "off": 0, "error": 0}

    # Iterate inputs
    for idx in range(min(nb, len(inputs_list))):
        i = inputs_list[idx] or {}
        k = str(idx)
        d = payload["inputs"][k]

        # status
        code = _norm_status_code(i)
        status = STATUS_MAP.get(code, "off")
        d["status"] = status
        if status == "on":
            counters["on"] += 1
        elif status == "idle":
            counters["idle"] += 1
        elif status == "off":
            counters["off"] += 1
        else:
            counters["error"] += 1

        # basics
        d["name"] = i.get("name")
        d["identifier"] = i.get("identifier")
        try:
            d["recorder_status"] = RECORDER_MAP.get(int(i.get("recorderStatus", 1)), "off")
        except Exception:
            d["recorder_status"] = "off"

        # geo
        if i.get("locationStatus") is not None:
            d["locationStatus"] = i.get("locationStatus")
            d["latitude"] = i.get("latitude")
            d["longitude"] = i.get("longitude")

        # encoders linkage (from configuration)
        enc_cfg = (sh_cfg or {}).get("enc", {})
        so_cfg = (sh_cfg or {}).get("streamingOutput", {})
        for eidx in range(len(enc_cfg)):
            ename = str(eidx + 1)
            enc = enc_cfg.get(ename) or {}
            if enc.get("enable") is True and enc.get("inputIndex") == idx:
                e = d["encoders"].setdefault(ename, {"streaming_outputs": {}})
                e["enable"] = True
                e["input_index_linked"] = idx
                # link streaming outputs for this encoder
                for soidx in range(len(so_cfg)):
                    soname = str(soidx + 1)
                    so = so_cfg.get(soname) or {}
                    if so.get("enable") is True and so.get("encoderIndex") == (int(ename) - 1):
                        e["streaming_outputs"].setdefault(soname, {})
                        e["streaming_outputs"][soname]["linked_streaming_output"] = True
                        e["streaming_outputs"][soname]["linked_streaming_output_name"] = so.get("name")
                        e["streaming_outputs"][soname]["linked_streaming_output_mode"] = so.get("mode")

        channel_type = i.get("channelType") or ""

        # IDLE (SST specifics)
        if status == "idle" and "SAFESTREAMS" in channel_type:
            d["protocol"] = "sst"
            d["family_name"] = i.get("familyName")
            d["message"] = i.get("message")
            d["version"] = i.get("version")
            d["intercom_status"] = INTERCOM_MAP.get(int(i.get("intercomStatus", 1)))
            d["intercom_profile"] = INTERCOM_PROFILE_MAP.get(int(i.get("intercomProfile", 1)))
            d["video_ifb"]["video_ifb_preset_status"] = VIDEO_IFB_PRESET_MAP.get(int(i.get("videoReturnProfile", 0)))
            if d["video_ifb"]["video_ifb_preset_status"] == "off":
                src_idx = i.get("videoReturnSrcIdx", -1)
                if isinstance(src_idx, int) and 0 <= src_idx < len(inputs_list):
                    d["video_ifb"]["video_ifb_decoding_source"] = (inputs_list[src_idx] or {}).get("identifier")
                    try:
                        d["video_ifb"]["video_ifb_decoder"] = "on" if (inputs_list[src_idx] or {}).get("channelStatus") == 2 else "off"
                    except Exception:
                        pass

        # ON (live details)
        if status == "on":
            d["source_identifier"] = i.get("identifier")
            d["source_name"] = i.get("name")
            try:
                d["info"] = (i.get("inputInfo") or "").split(" -")[0]
            except Exception:
                d["info"] = "no video/audio info"

            if "SAFESTREAMS" in channel_type:
                d["protocol"] = "sst"
                d["family_name"] = i.get("familyName")
                d["message"] = i.get("message")
                d["version"] = i.get("version")
                d["intercom_status"] = INTERCOM_MAP.get(int(i.get("intercomStatus", 1)))
                d["intercom_profile"] = INTERCOM_PROFILE_MAP.get(int(i.get("intercomProfile", 1)))
                d["video_ifb"]["video_ifb_preset_status"] = VIDEO_IFB_PRESET_MAP.get(int(i.get("videoReturnProfile", 0)))

                # preview
                ok_prev, prev, _ = _get_json_or_text(session, _full_url(base_url, f"/inputs/{idx+1}/preview", token), timeout=timeout)
                if ok_prev:
                    d["thumbnail"] = prev.get("thumbnail")
                    d["audio_levels"] = prev.get("audioLevels")

                # streamStats / linkStats
                ok_sst, sstats, _ = _get_json_or_text(session, _full_url(base_url, f"/inputs/{idx+1}/streamStats", token), timeout=timeout)
                ok_lst, lstats, _ = _get_json_or_text(session, _full_url(base_url, f"/inputs/{idx+1}/linkStats", token), timeout=timeout)
                if not ok_sst:
                    sstats = {}
                if not ok_lst:
                    lstats = {}

                # --- Link stats (per-link + totals)
                links_stats = (lstats or {}).get("links_stats", [])
                total_data = sum((ls.get("recv_bytes", 0) + ls.get("send_bytes", 0)) for ls in links_stats)
                d["total_data"] = total_data

                d["links"]["total_links"] = int(i.get("connectedLinks", 0) or 0)
                total_rx = sum(int(ls.get("rx_bitrate", 0) or 0) for ls in links_stats)
                total_tx = sum(int(ls.get("tx_bitrate", 0) or 0) for ls in links_stats)
                d["links"]["total_rx_bitrate_from_links"] = total_rx
                d["links"]["total_tx_bitrate_from_links"] = total_tx
                d["low_rx_bitrate"] = (total_rx < 1500)
                d["low_tx_bitrate"] = (total_tx < 1000)
                for j, ls in enumerate(links_stats):
                    # ensure a name is present for the frontend label
                    if not ls.get("name") and ls.get("itf_name"):
                        ls["name"] = ls.get("itf_name")
                    d["links"][str(j)] = ls

                # --- Dropped packets (rx_lost_packets) for video and mpegts-up
                def _sum_drop(arr):
                    s = 0
                    if isinstance(arr, list):
                        for it in arr:
                            try:
                                s += int((it or {}).get("rx_lost_packets") or 0)
                            except Exception:
                                pass
                    return s

                drops_video = _sum_drop(sstats.get("video") or [])
                drops_ts    = _sum_drop(sstats.get("mpegts-up") or sstats.get("mpegts_up") or [])
                d["streamStats"] = {
                    "video":       [{"rx_lost_packets": drops_video}],
                    "mpegts-up": [{"rx_lost_packets": drops_ts}],
                }
                # Expose for map segments logic (changed -> red)
                d.setdefault("notifications", {}).setdefault("dropped", {})
                d["notifications"]["dropped"]["video"] = drops_video
                d["notifications"]["dropped"]["ts"]    = drops_ts
            else:
                # Other protocols (NDI/RTMP/…)
                d["protocol"] = channel_type or None

            # SDI outputs
            for j in range(len(outs_sdi)):
                out = outs_sdi[j] or {}
                if out.get("enable") and out.get("input") == i.get("name"):
                    d["sdi_outputs"][str(j + 1)] = out.get("outputStandard")

            # IP outputs
            for j in range(len(outs_ip)):
                out = outs_ip[j] or {}
                if out.get("enable") and out.get("input") == i.get("name"):
                    key = str(j)
                    d["ip_outputs"][key] = {
                        "mode": out.get("mode"),
                        "name": out.get("name"),
                        "connections": out.get("connections"),
                        "status": {2: "on", 1: "idle", 3: "error"}.get(int(out.get("status", 1)), "idle"),
                    }

        # Detect log-driven live events (start/stop) for this input
        try:
            if logs_ok:
                ev = _detect_live_event_for_input(log_lines, idx + 1, d.get("identifier"))
                d["log_event"] = ev  # 'start' / 'stop' / None
        except Exception:
            d["log_event"] = d.get("log_event")

    # Aggregates
    payload["inputs"].update(counters)

    return True, payload
