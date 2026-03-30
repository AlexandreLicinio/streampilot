"""
StreamPilot — SRT Gateway collector
Haivision Media Gateway / SRT Gateway REST API v4.1
Auth: POST /api/session (cookie-based sessionID)
"""
from __future__ import annotations
import re, time, hashlib
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

_HTTP = requests.Session()
_HTTP.verify = False

# ── Auth ─────────────────────────────────────────────────────────────────────

def _cleanup_sessions(base_url: str, username: str, password: str,
                       timeout: int = 5) -> None:
    """Try to login and immediately logout to free a session slot.
    Called when HTTP 429 (max sessions) is hit.
    The gateway does not expose a sessions list endpoint so we just
    do a login+immediate logout to reclaim our own stale slot.
    """
    url = base_url.rstrip('/') + '/api/session'
    try:
        r = _HTTP.post(url, json={'username': username, 'password': password},
                       timeout=timeout, verify=False)
        if r.status_code == 200:
            sid = r.json().get('response', {}).get('sessionID')
            if sid:
                _HTTP.delete(url, cookies={'sessionID': str(sid)},
                             timeout=timeout, verify=False)
    except Exception:
        pass


def login(base_url: str, username: str, password: str,
          timeout: int = 5) -> Optional[str]:
    """POST /api/session — returns sessionID string or None on failure.
    On HTTP 429 (max sessions), attempts to free a slot then retries once.
    """
    url = base_url.rstrip('/') + '/api/session'
    for attempt in range(2):
        try:
            r = _HTTP.post(url, json={'username': username, 'password': password},
                           timeout=timeout, verify=False)
            if r.status_code == 200:
                sid = r.json().get('response', {}).get('sessionID')
                return str(sid) if sid else None
            if r.status_code == 429 and attempt == 0:
                import sys
                print(f'[srt_gw] 429 max sessions on {url}, freeing slot and retrying…',
                      file=sys.stderr)
                _cleanup_sessions(base_url, username, password, timeout)
                time.sleep(1)
                continue
            import sys
            print(f'[srt_gw] login HTTP {r.status_code} from {url}: {r.text[:200]}',
                  file=sys.stderr)
        except Exception as _e:
            import sys
            print(f'[srt_gw] login exception for {url}: {_e}', file=sys.stderr)
        break
    return None


def logout(base_url: str, session_id: str, timeout: int = 3) -> None:
    """DELETE /api/session"""
    try:
        _HTTP.delete(base_url.rstrip('/') + '/api/session',
                     cookies={'sessionID': session_id},
                     timeout=timeout, verify=False)
    except Exception:
        pass


def _cookies(session_id: str) -> Dict[str, str]:
    return {'sessionID': session_id}

# ── Device info ──────────────────────────────────────────────────────────────

def fetch_device_id(base_url: str, session_id: str,
                    timeout: int = 5) -> Tuple[bool, Optional[str]]:
    """GET /api/devices — returns (ok, device_id)."""
    try:
        r = _HTTP.get(base_url.rstrip('/') + '/api/devices',
                      cookies=_cookies(session_id),
                      timeout=timeout, verify=False)
        if r.status_code == 200:
            devices = r.json()
            if devices and isinstance(devices, list):
                return True, devices[0].get('_id')
    except Exception:
        pass
    return False, None

# ── Routes ───────────────────────────────────────────────────────────────────

def fetch_routes(base_url: str, session_id: str, gw_device_id: str,
                 timeout: int = 5) -> Tuple[bool, List[Dict]]:
    """GET /api/gateway/{deviceID}/routes — returns (ok, routes_list)."""
    url = f"{base_url.rstrip('/')}/api/gateway/{gw_device_id}/routes"
    try:
        r = _HTTP.get(url, cookies=_cookies(session_id),
                      timeout=timeout, verify=False)
        if r.status_code == 200:
            data = r.json()
            routes = data.get('data', data) if isinstance(data, dict) else data
            return True, routes if isinstance(routes, list) else []
    except Exception:
        pass
    return False, []

# ── Statistics ────────────────────────────────────────────────────────────────

def fetch_route_stats(base_url: str, session_id: str, gw_device_id: str,
                      route_id: str, timeout: int = 5) -> Tuple[bool, Dict]:
    """
    GET /api/gateway/{deviceID}/statistics?routeID={routeID}
    Returns (ok, stats_dict) — stats_dict keys: route, source, destinations.
    """
    url = (f"{base_url.rstrip('/')}/api/gateway/{gw_device_id}/statistics"
           f"?routeID={route_id}")
    try:
        r = _HTTP.get(url, cookies=_cookies(session_id),
                      timeout=timeout, verify=False)
        if r.status_code == 200:
            return True, r.json()
    except Exception:
        pass
    return False, {}

# ── Payload normalization ─────────────────────────────────────────────────────

def normalize_route(route: Dict, stats: Dict) -> Dict:
    """
    Merge route config + stats into a flat, StreamPilot-friendly dict.

    Returned keys:
      id, name, state,
      source: {name, protocol, address, port, mode, state, bitrate, rtt, loss,
               retransmit_rate, connections: [{address, port, bitrate, rtt, loss}]}
      destinations: [{id, name, protocol, address, port, state, bitrate, connections}]
      total_bitrate_mbps, active_connections
    """
    route_stat = stats.get('route', {})
    src_stat   = route_stat.get('source', {})
    dst_stats  = route_stat.get('destinations', [])

    def _src_connections(s: Dict) -> List[Dict]:
        conns = []
        for c in (s.get('connections') or []):
            conns.append({
                'address':        c.get('address', ''),
                'port':           c.get('port', 0),
                'srt_version':    c.get('srtVersion', ''),
                'bitrate_mbps':   _f(c.get('bitrate')),
                'rtt_ms':         _f(c.get('srtRoundTripTime')),
                'loss_pct':       _f(c.get('srtPacketLossRate')),
                'retransmit_bps': _f(c.get('srtRetransmitRate')),
                'buffer_ms':      _f(c.get('srtBufferLevel')),
                'state':          c.get('state', ''),
            })
        return conns

    def _dst_connections(d: Dict) -> List[Dict]:
        """Extract client connections from a destination.
        - Listener: clients are in clientsStat[].connections[]
        - Caller/Rendezvous: connection details are in connections[]
        """
        conns = []
        # Listener: clientsStat[i].connections[j]
        clients_stat = d.get('clientsStat') or []
        if clients_stat:
            for client in clients_stat:
                label   = client.get('label', '')
                address = client.get('address', '')
                port    = client.get('port', 0)
                br      = _f(client.get('bitrate'))
                # Prefer connection-level RTT/loss if available
                rtt  = None
                loss = None
                for conn in (client.get('connections') or []):
                    rtt  = _f(conn.get('srtRoundTripTime'))
                    loss = _f(conn.get('srtPacketLossRate'))
                    address = conn.get('address', address)
                    port    = conn.get('port', port)
                    break  # first connection is enough
                srt_ver = ''
                for conn in (client.get('connections') or []):
                    srt_ver = conn.get('srtVersion', '')
                    break
                conns.append({
                    'address':      address,
                    'port':         port,
                    'label':        label,
                    'srt_version':  srt_ver,
                    'display':      f'{address}:{port}' + (f' ({label})' if label else ''),
                    'bitrate_mbps': br,
                    'rtt_ms':       rtt,
                    'loss_pct':     loss,
                    'state':        'connected',
                })
            return conns
        # Caller/Rendezvous: direct connections[]
        for c in (d.get('connections') or []):
            conns.append({
                'address':      c.get('address', ''),
                'port':         c.get('port', 0),
                'label':        '',
                'bitrate_mbps': _f(c.get('bitrate')),
                'rtt_ms':       _f(c.get('srtRoundTripTime')),
                'loss_pct':     _f(c.get('srtPacketLossRate')),
                'state':        c.get('state', ''),
            })
        return conns

    def _f(v) -> Optional[float]:
        try: return float(v)
        except Exception: return None

    # Aggregate bitrate
    src_bitrate = _f(src_stat.get('bitrate'))

    def _dst_bitrate_total(d: Dict) -> float:
        """Actual output bandwidth for a destination.
        Listener: each client gets the full stream → sum clientsStat[].bitrate.
        Caller/Rendezvous: sum connected connections[].bitrate.
        """
        clients_stat = d.get('clientsStat') or []
        if clients_stat:
            # Listener: N clients each receiving the stream
            return sum(_f(c.get('bitrate')) or 0.0 for c in clients_stat)
        # Caller / Rendezvous: sum active connections
        conns = d.get('connections') or []
        if conns:
            return sum(_f(c.get('bitrate')) or 0.0
                       for c in conns
                       if (c.get('state') or '').lower() in
                          ('connected', 'connection established', 'ok', 'running', ''))
        return _f(d.get('bitrate')) or 0.0

    dst_bitrate_total = sum(_dst_bitrate_total(d) for d in dst_stats)
    total_bitrate = src_bitrate if src_bitrate is not None else dst_bitrate_total

    source_cfg = route.get('source', {})
    src = {
        'name':           source_cfg.get('name', ''),
        'protocol':       source_cfg.get('protocol', ''),
        'address':        source_cfg.get('address', ''),
        'port':           source_cfg.get('port', 0),
        'mode':           src_stat.get('mode', source_cfg.get('srtMode', '')),
        'state':          src_stat.get('state', ''),
        'srt_version':    (src_stat.get('connections') or [{}])[0].get('srtVersion', '') if src_stat.get('connections') else src_stat.get('srtVersion', ''),
        'bitrate_mbps':   src_bitrate,
        'rtt_ms':         _f(src_stat.get('srtRoundTripTime')),
        'loss_pct':       _f(src_stat.get('srtPacketLossRate')),
        'retransmit_bps': _f(src_stat.get('srtRetransmitRate')),
        'connections':    _src_connections(src_stat),
    }

    # Match stats to config by destination ID (not by position)
    dst_stats_by_id = {d.get('id'): d for d in dst_stats if d.get('id')}
    destinations = []
    for dst_cfg in route.get('destinations', []):
        did = dst_cfg.get('id', '')
        ds = dst_stats_by_id.get(did) or {}
        destinations.append({
            'id':           did,
            'name':         dst_cfg.get('name', ''),
            'protocol':     dst_cfg.get('protocol', ''),
            'address':      dst_cfg.get('address', ''),
            'port':         dst_cfg.get('port', 0),
            'mode':         ds.get('mode', dst_cfg.get('srtMode', '')),
            'state':        (ds.get('state') or ds.get('summaryStatusDetails') or ''),
            'bitrate_mbps': (_dst_bitrate_total(ds) if ds else None),
            'loss_pct':     _f(ds.get('srtPacketLossRate')),
            'rtt_ms':       _f(ds.get('srtRoundTripTime')),
            'connections':  _dst_connections(ds),
        })

    # Count active clients across all destinations.
    # _dst_connections() already extracted the right entries for each mode:
    #   Listener -> clientsStat[] entries
    #   Caller   -> connections[] entries (all, regardless of state field)
    # Sum len(connections) for all destinations.
    active_conns = sum(len(d['connections']) for d in destinations)

    return {
        'id':                route.get('id', ''),
        'name':              route.get('name', ''),
        'state':             route.get('state', route_stat.get('state', 'idle')),
        'elapsed':           route_stat.get('elapsedRunningTime', ''),
        'source':            src,
        'destinations':      destinations,
        'total_bitrate_mbps': total_bitrate,
        'active_connections': active_conns,
    }
