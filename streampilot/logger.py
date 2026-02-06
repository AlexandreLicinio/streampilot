# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Alexandre Licinio
import sqlite3, threading, time, datetime, queue
from typing import Dict, Tuple, Any

DB_PATH = None  # set at import by server
TICK_SECONDS = 2

class LiveLogger:
    def __init__(self, db_path):
        global DB_PATH
        DB_PATH = db_path
        self.db_path = str(db_path)
        self.lock = threading.RLock()
        self.last_payloads: Dict[str, dict] = {}
        self._stop = False
        self.sessions_en_cours: Dict[Tuple[str,str], Dict[str, Any]] = {}
        self._thread = threading.Thread(target=self._run_ticker, name="live_logger_ticker", daemon=True)
        self._thread.start()
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None, timeout=2.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=2000;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-20000;")  # ~20MB
        except Exception:
            pass
        return conn

    def _init_db(self):
        with self._conn() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS live_session (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              device_id TEXT NOT NULL,
              device_host TEXT,
              input_key TEXT NOT NULL,
              input_index INTEGER NOT NULL,
              input_identifier TEXT,
              input_display_name TEXT,
              started_at DATETIME NOT NULL,
              ended_at DATETIME,
              title TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_session_unique
              ON live_session(device_id, input_key, started_at);

            CREATE TABLE IF NOT EXISTS live_sample (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id INTEGER NOT NULL,
              ts DATETIME NOT NULL,
              year INTEGER, month INTEGER, day INTEGER,
              hour INTEGER, minute INTEGER, second INTEGER,
              latitude REAL, longitude REAL,
              drops_video INTEGER, drops_ts INTEGER,
              link_name TEXT, owdR INTEGER, rx_bitrate INTEGER,
              rx_percent_lost INTEGER, rx_lost_nb_packets INTEGER,
              FOREIGN KEY(session_id) REFERENCES live_session(id) ON DELETE CASCADE
            );
            """)
            # migration légère si title manquant
            cols_session = {r[1] for r in c.execute("PRAGMA table_info(live_session)").fetchall()}
            if 'title' not in cols_session:
                c.execute("ALTER TABLE live_session ADD COLUMN title TEXT")

    def _now_parts(self):
        now = datetime.datetime.utcnow()
        return now, now.year, now.month, now.day, now.hour, now.minute, now.second

    def _start_session(self, device_id, device_host, input_key, input_index, input_identifier, input_display_name):
        with self._conn() as c:
            now, *_ = self._now_parts()
            c.execute("""
              INSERT INTO live_session(device_id, device_host, input_key, input_index, input_identifier, input_display_name, started_at, title)
              VALUES (?,?,?,?,?,?,?,?)
            """, (device_id, device_host, input_key, input_index, input_identifier, input_display_name, now.isoformat(), None))
            session_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        return session_id

    def _end_session(self, session_id):
        with self._conn() as c:
            now, *_ = self._now_parts()
            c.execute("UPDATE live_session SET ended_at=? WHERE id=?", (now.isoformat(), session_id))

    def _insert_samples(self, session_id, gps, drops, link_rows):
        now, Y, M, D, h, m, s = self._now_parts()
        lat = (gps or {}).get('lat')
        lng = (gps or {}).get('lng')
        dv = (drops or {}).get('video', 0)
        dt = (drops or {}).get('ts', 0)
        rows = []
        if link_rows:
            for it in link_rows:
                rows.append((
                    session_id, now.isoformat(), Y,M,D,h,m,s,
                    lat, lng, dv, dt,
                    it.get('name'), it.get('owdR'), it.get('rx_bitrate'),
                    it.get('rx_percent_lost'), it.get('rx_lost_nb_packets')
                ))
        else:
            rows.append((session_id, now.isoformat(), Y,M,D,h,m,s, lat, lng, dv, dt, None, None, None, None, None))
        with self._conn() as c:
            c.executemany(
              """
              INSERT INTO live_sample(session_id, ts, year, month, day, hour, minute, second,
                latitude, longitude, drops_video, drops_ts,
                link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              """,
              rows
            )

    def _build_rows(self, session_id, gps, drops, link_rows):
        now, Y, M, D, h, m, s = self._now_parts()
        lat = (gps or {}).get('lat')
        lng = (gps or {}).get('lng')
        dv = (drops or {}).get('video', 0)
        dt = (drops or {}).get('ts', 0)
        rows = []
        if link_rows:
            for it in link_rows:
                rows.append((
                    session_id, now.isoformat(), Y, M, D, h, m, s,
                    lat, lng, dv, dt,
                    it.get('name'), it.get('owdR'), it.get('rx_bitrate'),
                    it.get('rx_percent_lost'), it.get('rx_lost_nb_packets')
                ))
        else:
            rows.append((session_id, now.isoformat(), Y, M, D, h, m, s, lat, lng, dv, dt, None, None, None, None, None))
        return rows

    def _insert_samples_batch(self, rows):
        if not rows:
            return
        with self._conn() as c:
            c.executemany(
                """
                INSERT INTO live_sample(session_id, ts, year, month, day, hour, minute, second,
                  latitude, longitude, drops_video, drops_ts,
                  link_name, owdR, rx_bitrate, rx_percent_lost, rx_lost_nb_packets)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows
            )

    def observe_payload(self, device_id: str, device_host: str, payload: dict):
        with self.lock:
            # cache last full payload per device id and host (some installs use one or the other)
            p = payload or {}
            self.last_payloads[device_id] = p
            if device_host:
                self.last_payloads[device_host] = p
            inputs = (payload or {}).get('inputs', {})
            # transitions — SST only (exclude RTMP/SRT/HLS/RTSP/NDI/File/...)
            for key, v in list(inputs.items()):
                if not str(key).isdigit():
                    continue
                v = v or {}
                k = (device_id, str(key))

                # Normalize protocol and status
                proto_raw = v.get('protocol')
                proto = str(proto_raw).strip().lower() if proto_raw is not None else ''
                status_raw = v.get('status')
                status = str(status_raw).strip().lower() if status_raw is not None else ''
                is_on = status in ('on', '2', 'running', 'live', 'true')

                # Enforce SST strictly for opening/keeping sessions
                is_sst = ('sst' in proto) if proto else False

                # If protocol is explicitly non-SST, close any existing session and skip
                if proto and not is_sst:
                    if k in self.sessions_en_cours:
                        self._end_session(self.sessions_en_cours[k]['session_id'])
                        del self.sessions_en_cours[k]
                    continue

                # Open only if SST and status is ON
                if is_sst and is_on and k not in self.sessions_en_cours:
                    input_index = int(key) + 1
                    input_identifier = v.get('identifier') or f'input {input_index}'
                    input_display_name = v.get('familyName') or v.get('family_name') or None
                    sid = self._start_session(device_id, device_host, str(key), input_index, input_identifier, input_display_name)
                    self.sessions_en_cours[k] = {'session_id': sid, 'last_tick': 0.0, 'last_gps': None, 'host': device_host}

                # Close if turns OFF (when proto is SST or proto missing but session exists)
                if (not is_on) and k in self.sessions_en_cours:
                    self._end_session(self.sessions_en_cours[k]['session_id'])
                    del self.sessions_en_cours[k]

    def _run_ticker(self):
        while not getattr(self, '_stop', False):
            try:
                with self.lock:
                    now = time.time()
                    # copy keys to avoid mutation during iteration
                    sessions = list(self.sessions_en_cours.items())
                    batch_rows = []
                    for (dev_id, input_key), info in sessions:
                        payload = self.last_payloads.get(dev_id)
                        if payload is None:
                            payload = self.last_payloads.get(info.get('host'))
                        payload = payload or {}
                        if not self.last_payloads.get(dev_id) and info.get('host') and self.last_payloads.get(info['host']):
                            try:
                                import cherrypy; cherrypy.log(f"ticker: fallback to host payload for dev {dev_id} host {info['host']}")
                            except Exception:
                                pass
                        inputs = (payload or {}).get('inputs', {})
                        v = inputs.get(input_key)
                        if v is None:
                            try:
                                v = inputs.get(int(input_key))
                            except Exception:
                                v = None
                        v = v or {}
                        gps = self._extract_gps(v)
                        if not gps:
                            gps = info.get('last_gps')
                        else:
                            info['last_gps'] = gps
                        drops = ((v.get('notifications') or {}).get('dropped')) or {}
                        link_rows = []
                        L = v.get('links') or {}
                        for lk, it in L.items():
                            if lk in ('total_links','total_rx_bitrate_from_links','total_tx_bitrate_from_links'):
                                continue
                            if not isinstance(it, dict):
                                link_rows.append({'name': lk, 'owdR': None, 'rx_bitrate': None, 'rx_percent_lost': None, 'rx_lost_nb_packets': None})
                                continue
                            # helpers to coerce to int safely
                            def to_int(x):
                                try:
                                    if x is None:
                                        return None
                                    if isinstance(x, bool):
                                        return int(x)
                                    if isinstance(x, (int, float)):
                                        return int(x)
                                    s = str(x).strip().replace(',', '.')
                                    # strip units if any (e.g., "123 kb/s")
                                    m = ''.join(ch for ch in s if (ch.isdigit() or ch in '.-'))
                                    return int(float(m)) if m not in ('', '-', '.') else None
                                except Exception:
                                    return None

                            name = it.get('name') or lk
                            # owdR fallbacks
                            owdR = it.get('owdR')
                            if owdR is None:
                                owdR = it.get('owd_r')
                            if owdR is None:
                                owdR = it.get('owd')
                            if owdR is None:
                                owdR = it.get('oneway')
                            if owdR is None:
                                owdR = it.get('rtt')  # as last resort
                            owdR = to_int(owdR)

                            # rx_bitrate fallbacks
                            rx_bitrate = it.get('rx_bitrate')
                            if rx_bitrate is None:
                                rx_bitrate = it.get('rxBitrate')
                            if rx_bitrate is None:
                                rx_bitrate = it.get('rx_kbits')
                            if rx_bitrate is None:
                                rb = it.get('bitrate') or it.get('rx')
                                if isinstance(rb, dict):
                                    rb = rb.get('kbits') or rb.get('value')
                                rx_bitrate = rb
                            rx_bitrate = to_int(rx_bitrate)

                            # loss metrics
                            rx_percent_lost = it.get('rx_percent_lost')
                            if rx_percent_lost is None:
                                rx_percent_lost = it.get('rx_percent_loss')
                            if rx_percent_lost is None:
                                rx_percent_lost = it.get('rx_loss_percent')
                            rx_percent_lost = to_int(rx_percent_lost)

                            rx_lost_nb_packets = it.get('rx_lost_nb_packets')
                            if rx_lost_nb_packets is None:
                                rx_lost_nb_packets = it.get('rx_lost_packets')
                            if rx_lost_nb_packets is None:
                                rx_lost_nb_packets = it.get('rx_lost_nb')
                            if rx_lost_nb_packets is None:
                                rx_lost_nb_packets = it.get('rx_lost')
                            rx_lost_nb_packets = to_int(rx_lost_nb_packets)
                            link_rows.append({
                                'name': name,
                                'owdR': owdR,
                                'rx_bitrate': rx_bitrate,
                                'rx_percent_lost': rx_percent_lost,
                                'rx_lost_nb_packets': rx_lost_nb_packets
                            })
                        # Always append a sample every tick (even if no links)
                        try:
                            import cherrypy
                            cherrypy.log(f"live_tick session={info['session_id']} dev={dev_id} in={input_key} links={len(link_rows)} lat={(gps or {}).get('lat')} lng={(gps or {}).get('lng')}")
                        except Exception:
                            pass
                        batch_rows.extend(self._build_rows(info['session_id'], gps, drops, link_rows))
                # flush outside the lock
                try:
                    self._insert_samples_batch(batch_rows)
                except Exception:
                    try:
                        import cherrypy; cherrypy.log('live_logger_ticker: batch insert error', traceback=True)
                    except Exception:
                        pass
                time.sleep(TICK_SECONDS)
            except Exception:
                # be resilient; never crash the ticker
                try:
                    import cherrypy; cherrypy.log('live_logger_ticker: exception', traceback=True)
                except Exception:
                    pass
                time.sleep(TICK_SECONDS)

    def _extract_gps(self, v):
      """Try very hard to find GPS coordinates inside v.
      Accepts top-level keys, common containers, and nested dicts/lists up to depth 3.
      Returns {'lat': float, 'lng': float} or None.
      """
      if not isinstance(v, dict):
          return None

      # helpers
      def to_float(x):
          try:
              if x is None:
                  return None
              if isinstance(x, (int, float)):
                  return float(x)
              s = str(x).strip()
              # remove trailing N/E/S/W if present
              if s and s[-1] in 'NESWnesw':
                  s = s[:-1]
              # replace comma decimal
              s = s.replace(',', '.')
              return float(s)
          except Exception:
              return None

      def pair(lat_val, lng_val):
          lat_f, lng_f = to_float(lat_val), to_float(lng_val)
          if lat_f is None or lng_f is None:
              return None
          return {'lat': lat_f, 'lng': lng_f}

      lat_keys = ('latitude','lat','Latitude','Lat','gps_lat','y')
      lng_keys = ('longitude','lng','lon','long','Longitude','Lng','Lon','Long','gps_lng','gps_lon','x')

      # 1) top-level direct keys
      for la in lat_keys:
          for lo in lng_keys:
              if la in v and lo in v:
                  p = pair(v.get(la), v.get(lo))
                  if p:
                      return p

      # 2) known containers (one level)
      containers = (
          'gps','GPS','location','position','geo','coordinates','geolocation','coord',
          'metadata','meta','state','extra','status','status_details'
      )
      for ckey in containers:
          g = v.get(ckey)
          if isinstance(g, dict):
              # 2a) dict with lat/lng
              for la in lat_keys:
                  for lo in lng_keys:
                      if la in g and lo in g:
                          p = pair(g.get(la), g.get(lo))
                          if p:
                              return p
              # 2b) array-like forms inside container
              for kk, vv in g.items():
                  # arrays [lat,lng]
                  if isinstance(vv, (list, tuple)) and len(vv) >= 2:
                      p = pair(vv[0], vv[1])
                      if p:
                          return p
                  # nested dict one more level
                  if isinstance(vv, dict):
                      for la in lat_keys:
                          for lo in lng_keys:
                              if la in vv and lo in vv:
                                  p = pair(vv.get(la), vv.get(lo))
                                  if p:
                                      return p
          # coordinates provided as list/tuple at container
          if isinstance(g, (list, tuple)) and len(g) >= 2:
              p = pair(g[0], g[1])
              if p:
                  return p

      # 3) last-known variants
      for ckey in ('last_gps','lastGps','last_position','lastPosition','last_location','lastLocation'):
          g = v.get(ckey)
          if isinstance(g, dict):
              for la in lat_keys:
                  for lo in lng_keys:
                      if la in g and lo in g:
                          p = pair(g.get(la), g.get(lo))
                          if p:
                              return p
      return None

    def stop(self):
        self._stop = True
