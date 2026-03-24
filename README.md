# StreamPilot — Stream smarter. Pilot with precision. Broadcast better.

<p align="center">
  <img src="StreamPilot.png" alt="StreamPilot logo" width="300"/>
</p>

<p align="center">
  <a href="http://localhost:5555">
    <img src="https://img.shields.io/badge/Launch-StreamPilot-brightgreen?style=for-the-badge" alt="Launch StreamPilot"/>
  </a>
</p>

**StreamPilot** is a web app for real‑time supervision and geo‑visualization (location) of **Haivision** transmitters. Stats from 4G/5G modems, ETH1‑2, Wi‑Fi and USB are recorded and displayed live during each session through several charts. You can use it during live production or site surveys to map precise coverage for public/private 4G/5G or any network interface (ETH1‑2, STARLINK, Wi‑Fi, USB) supported by the transmitter.
Ideal for mobile broadcast: cycle tours, marathons, triathlons, remote production, and private 5G deployments.

Raw data is provided by **Haivision StreamHub** via its REST API (HTTP/HTTPS). All network interfaces and GPS are monitored.
AIRxxx and PROxxx series are the ones with GPS sensors.

Also, it can provides powerfull pdf reports at the end of a live. The reports contains correlated logs from the StreamHub from each inputs in live to help you sharing the right information.

### What’s next for StreamPilot?

Today, StreamHub’s REST API doesn’t expose every modem details (Band, Operator name, SNR, RSSI, priority). The goal is for **StreamPilot** to actively **pilot each modem**, switching live to the best interface(s). This would raise transmission quality by automatically managing interface priorities.

### Why StreamPilot?

Just because I needed a tool for site surveys and live production in my company Wiftech. I am not working for Haivision but I did choose Aviwest as main gear to do live streaming for my clients. I encourage all the "bonded live video constructors" to share in their api all the raw data we need to make StreamPilot better.

---

### Roadmap

- [x] Haivision SST transmitters
- [x] Slack notifications
- [ ] Modem and priority control (requires deeper API access from Haivision)

---

## Prerequisites

- Debian 13
- Python ≥ 3.13

## Installation

You **don’t** need to be **root** to install or run StreamPilot.

1. Download the project and create a Python virtual environment in the folder:

```bash
git clone https://github.com/AlexandreLicinio/streampilot.git
cd streampilot
python3 -m venv --system-site-packages .
```

2. Install the main dependencies:

```bash
bin/python -m pip install CherryPy Mako requests reportlab
```

---

## Run the server

On the first run, all files and the database will be created automatically.
Set the listening port (example: 5555) and start the server from the project root:

```bash
bin/python -m streampilot -port 5555 -name "John Dear" -max_streamhubs 4 -user 'admin' -password 'password'
```
You can also specify port, name, max_streamhubs thru the following environment variables

> - `-port`: UI TCP port (default: 5555)
> - `-name`: Generic name displayed in the UI
> - `-max_streamhubs`: Maximum number of StreamHubs polled by the app (default: 4)
> - `-user`: Username for the login portal (default: admin)
> - `-password`: Password for the login portal (default: admin)

The app will be available at [http://localhost:5555](http://localhost:5555).

---

## Usage

<p align="center">
  <img src="streamhub_login_page.png" alt="StreamHub login page" width="700"/>
</p>

In the StreamHub side menu, go to **REST API doc**.

<p align="center">
  <img src="streamhub_api_page.png" alt="StreamHub API page" width="700"/>
</p>

Copy the **api_key**.

In StreamPilot [http://localhost:5555](http://localhost:5555), open **Devices** and add a StreamHub by filling in the fields. Once added, the device is polled as long as StreamPilot is running.

<p align="center">
  <img src="dashboard.png" alt="StreamPilot Dashboard" width="700"/>
</p>

As soon as a transmitter is online and GPS data is available via the API, its position appears on the Dashboard map.

<p align="center">
  <img src="in_live_dashboard.png" alt="RACK200 in live mode" width="700"/>
</p>

When the transmitter goes **live**, a session is created automatically.

<p align="center">
  <img src="session_live.png" alt="Logs dashboard" width="700"/>
</p>

Sessions are accessible from the **Logs** menu. Click **View** on the running session to see real‑time GPS and network interfaces status for the SST transmitter.

<p align="center">
  <img src="gps_session_example.png" alt="GPS session example" width="700"/>
</p>

While the transmitter is **live**, charts and timeline progress continuously. If you uncheck **Follow live**, you can move the timeline to inspect a specific moment (GPS + INTERFACES).

### Slack notifications:

<p align="center">
  <img src="slack_notifications_app.png" alt="Slack notifications app" width="700"/>
</p>

Follow the instructions to create a webhook [https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/), open **Settings** and configure the global settings. Then configure for each StreamHub.

In the **Ignore contains (one filter per line)** box, you can add all or part of a log to filter it and avoid receiving it. This is very useful for preventing alerts about irrelevant information.

```bash
StreamHub user admin
StreamHub is disconnected from Aviwest Hub service
StreamHub is connected to Aviwest Hub service
read ECONNRESET
Nodejs is restarting...
502 Server Error
HTTPSConnectionPool
```

You can define an OWD alert threshold and a bitrate alert.

<p align="center">
  <img src="settings_notifications_per_devices.png" alt="Per-devices settings for notifications" width="700"/>
</p>

---

## Sessions

### The map (gps):

When a transmitter goes live, a new session is created. If the GPS is receiving data from satellite, a map will display the exact position of the transmitter. If you tick the **Follow live** button the timeline will grow in real-time and fill all the charts with interfaces stats. In the map, a colored line will be draw between gps new points. The colour are based on **OWD** which is half of the RTT for each interfaces. 

| Signal        | OWD (in ms, rtt/2) |
| ------------- |:------------------:|
| Good          | less 100 ms        |
| Fair          | from 100 to 200 ms |
| Poor          | more than 200 ms   |

At the end (or even during a live) of a session you can move the cursor of the timeline to review the recorded data. A vertical line, based on the timeline cursor will appear on each charts. 

<p align="center">
  <img src="gps_map_legend.png" alt="GPS map" width="700"/>
</p>

### Events (logs StreamHub):

All the logs from the StreamHub are sorted per transmitter (input) and displayed both on a timeline and a table. Theses logs will can be also exported in PDF and JSON at the end of the live.

<p align="center">
  <img src="events_logs_timeline.png" alt="Events lots timeline" width="700"/>
</p>

---

## Haivision:

StreamHub is the main data and stats collector for all transmitters. Here are the data and stats actually provided by the api (in REST).

| endpoint        | api                |
| ----------------|:------------------:|
| bitrate         | OK                 |
| gps             | OK                 |
| OWD             | OK                 |
| RTT             | OK                 |
| loss            | OK                 |
| lost packets    | OK                 |
| mobile operator | NOK                |
| 4G/5G band      | NOK                |
| 3G/4G/5G        | NOK                |
| snr             | NOK                |
| rssi            | NOK                |
| priority        | NOK                |

**StreamPilot** is OKAY with theses firmwares:

| model         | firmware version   |
| ------------- |:------------------:|
| air series    | 6.2.0              |
| streamhub     | 4.4.6_SP1          |
| rack400       | 4.2.0              |
| rack2-3       | 6.2.0              |
| PRO3 series   | 6.2.0              |

---

## Features

- **Supervision** of Haivision StreamHub transmitters over the SST protocol
- **Real‑time geolocation** of SST inputs on an interactive map
- **Session timeline** with metrics: bitrate, OWD, losses, dropped packets
- **JSON/CSV export** of sessions with all measurements (GPS, links, drops…)
- **JSON import** of sessions with full measurements (GPS, links, drops…)
- **GeoJSON export** for external analysis (QGIS, Kepler.gl, geojson.io…)
- **PDF export** for reports and reviews.
- **GPS sessions** with individual deletion or full purge
- **Health view** (`/health`) with poller state, active sessions, last sample age per StreamHub
- **Sparklines** (mini SVG charts over 1–2 minutes) for last‑sample age
- **JSON endpoint** (`/health_json`) for external monitoring
- **Prometheus endpoint** (`/metrics`) for Grafana/Prometheus
- **Follow live** to keep the session view synced in real time
- **Background poller** independent from the UI (captures sessions even if the Dashboard isn’t open)
- **Light/Dark theme** toggle
- **Responsive dashboard** (Bootstrap 5)
- **Notifications slack** via webhook

---

## Monitoring & integrations

- **/health**: poller state, sessions, sample ages
- **/health_json**: external monitoring (JSON)
- **/metrics**: Prometheus endpoint for Grafana/Prometheus

---

## Branding

- Product: **StreamPilot**  
- Tagline: *Stream smarter. Pilot with precision. Broadcast better.*  
- Copyright: StreamPilot — Copyright (C) 2026 Alexandre Licinio  
- Author: Alexandre Licinio

---

## Contributing

Individuals and companies are welcome to contribute. If you’d like to get involved, feel free to reach out.

---

## License

StreamPilot — Copyright (C) 2026 Alexandre Licinio

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program in the file COPYING.LESSER. If not, see:
https://www.gnu.org/licenses/
