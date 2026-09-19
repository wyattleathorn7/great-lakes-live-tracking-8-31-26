#!/usr/bin/env python3
"""
Open Waters Secondary AIS Fetch — Production (private repo only)

- ONE WebSocket: wss://ais.openwaters.io/v1/stream?key=<OPENWATERS_TOKEN>
- Great Lakes bbox 41.0,-93.5,49.5,-66.0 = 233.75 sq° (personal 400 sufficient, anonymous 100 insufficient)
- Client-side MMSI filtering against ais/roster_118.json (118 vessels) — no 118 individual subscriptions
- In-memory 118 VesselState, KML flush throttled 7 sec, atomic tmp->replace, validation 118 unique
- Heading: 511 unavailable → COG fallback, never writes 511, 0→360
- Offline: retain placemark visibility 0, no deletion/estimation/substitution, 30-min staleness
- Reconnect: exponential backoff 1→60s + jitter, preserve state, snapshot recovery only for initial/reconnect
- No mock fallback, no continuous poll, snapshot GET sends the personal token (anonymous bbox is rejected with HTTP 400)
- Attribution: Source Open Waters (ais.openwaters.io) when actually used
- Token never printed: only "token present: YES/NO"

Usage (workflow): OPENWATERS_TOKEN env must be set (GitHub Secret)
  python3 ais/openwaters_fetch.py            # default 75 sec collection
  python3 ais/openwaters_fetch.py --duration 90
  python3 ais/openwaters_fetch.py --duration 0 --snapshot-only  # snapshot initial only
"""
import json
import pathlib
import time as time_module
import asyncio
import websockets
import urllib.request
import re
import html
import sys
import os
import random
from datetime import datetime, timezone
from collections import Counter
from dataclasses import dataclass, field

# --- Config ---
ROSTER_PATH = pathlib.Path(__file__).parent / "roster_118.json"
OUTPUT_KML = pathlib.Path(__file__).parent / "great_lakes_ais.kml"
SNAPSHOT_URL = "https://ais.openwaters.io/v1/vessels?bbox=41.0,-93.5,49.5,-66.0"
BBOX = [[41.0, -93.5, 49.5, -66.0]]
BBOX_CSV = "41.0,-93.5,49.5,-66.0"

ATTRIBUTION = "Source: Open Waters (ais.openwaters.io) — Volunteer + open feeds (Kystverket, Digitraffic, AISHub, AIS-catcher). Data may be delayed/incomplete/inaccurate. Not for navigation."
DISCLAIMER = "AIS data can be delayed, incomplete, or inaccurate and is not for navigation. Positions via Open Waters aggregated network."

roster = json.load(open(ROSTER_PATH))
roster_mmsi_set = set(str(r["mmsi"]) for r in roster)
roster_mmsi_map = {str(r["mmsi"]): r for r in roster}

# roster operator/type lookup for KML extended fields
def _roster_entry(mmsi):
    r = roster_mmsi_map.get(str(mmsi))
    if not r:
        return {}
    # roster_118.json fields: code, vessel, operator, type, imo, mmsi, call, flag, length
    return r

@dataclass
class VesselState:
    mmsi: str
    roster_entry: dict
    lat: float = None
    lon: float = None
    sog: float = None
    cog: float = None
    heading: float = None
    source: str = None
    station: str = None
    msg_type: str = None
    seen: str = None
    last_update: float = field(default_factory=lambda: 0)
    status: str = "offline"
    dirty: bool = False

state = {mmsi: VesselState(mmsi=mmsi, roster_entry=roster_mmsi_map[mmsi]) for mmsi in roster_mmsi_set}

stats = Counter()
stats["reconnects"] = 0
stats["events_received"] = 0
stats["roster_matched"] = 0
stats["unrostered_discarded"] = 0
stats["position_changes"] = 0
stats["kml_flushes"] = 0
stats["queue_depth_max"] = 0
errors = []
event_queue = asyncio.Queue(maxsize=1024)

def kml_heading(h):
    try:
        hi = int(float(h))
    except:
        return None
    if hi == 511:
        return None
    if hi == 0:
        return 360
    return hi

def friendly_utc_stamp(stamp):
    """Format a UTC timestamp for Google Earth balloon descriptions.

    Accepts ISO '2026-09-19T02:01:01Z' (or 'YYYY-MM-DD HH:MM:SS UTC') and returns
    12-hour Great Lakes local time with AM/PM plus a UTC reference, e.g.
    '10:01 PM EDT Thu, Sep 18 (02:01 UTC Fri, Sep 19)'.
    Unparseable input is returned unchanged so descriptions never go blank.
    """
    text = str(stamp or "").strip()
    dt = None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M UTC"):
        try:
            dt = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        local = dt.astimezone(ZoneInfo("America/Detroit"))
    except Exception:
        local = dt
    return (local.strftime("%-I:%M %p %Z %a, %b %d")
            + dt.strftime(" (%H:%M UTC %a, %b %d)"))

def build_kml():
    kml_lines = []
    kml_lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    kml_lines.append('<kml xmlns="http://www.opengis.net/kml/2.2"><Document>')
    kml_lines.append('  <name>Great Lakes Commercial &amp; Operational Ships — AIS Live (Open Waters — 118)</name>')
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kml_lines.append(f'  <description><![CDATA[Production AIS vessel layer — 118 permanent placemarks (MMSI primary key, never name-matched).<br/>Source: {ATTRIBUTION}<br/>BBOX {BBOX_CSV} | Fetched {html.escape(fetched_at)} | One WebSocket bbox + roster filter 118, no individual MMSI subscriptions<br/>MMSI→placemark never changes, heading via &lt;IconStyle&gt;&lt;heading&gt; (HEADING 511 unavailable → COG fallback labelled), offline retained visibility 0.<br/>118/118 High, ceiling 180 not approached. Not for navigation.]]></description>')
    kml_lines.append('  <Style id="vesselActive"><IconStyle><scale>1.1</scale><Icon><href>icons/Copilot_20260831_192510.png</href></Icon><hotSpot x="0.5" y="0.5" xunits="fraction" yunits="fraction"/></IconStyle><LabelStyle><scale>0.7</scale></LabelStyle><BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle></Style>')
    kml_lines.append('  <Style id="vesselOffline"><IconStyle><color>ff808080</color><scale>0.7</scale><Icon><href>icons/Copilot_20260831_192510.png</href></Icon></IconStyle><LabelStyle><scale>0.6</scale></LabelStyle></Style>')
    # Human-friendly run timestamp (12-hour Great Lakes local + UTC ref).
    # ExtendedData below keeps the machine-readable ISO value unchanged.
    fetched_friendly = friendly_utc_stamp(fetched_at)
    for mmsi in sorted(roster_mmsi_set):
        entry = roster_mmsi_map[mmsi]
        # entry from JSON: dict with vessel, operator, type, imo, mmsi, call, flag, length, code
        s = state[mmsi]
        has_pos = s.lat is not None and s.lon is not None
        is_live = has_pos and s.seen and (time_module.time() - s.last_update) < 1800
        vessel_name = entry.get("vessel", mmsi)
        code = entry.get("code", "")
        operator = entry.get("operator", "")
        vtype = entry.get("type", "")
        imo = entry.get("imo", "")
        call = entry.get("call", "")
        flag = entry.get("flag", "")
        length = entry.get("length", "")
        if is_live:
            h_val = kml_heading(s.heading) if s.heading is not None else None
            cog_val = None
            try:
                if s.cog is not None and float(s.cog) != 360:
                    cog_val = float(s.cog)
            except:
                pass
            heading_src = "HEADING"
            icon_h = h_val
            if icon_h is None and cog_val is not None:
                try:
                    icon_h = int(cog_val)
                except:
                    pass
                heading_src = "COG (HEADING unavailable, fallback)"
            # string representations
            heading_str = f"{s.heading}°" if s.heading not in (511, "511", None) and kml_heading(s.heading) is not None else "not available (COG fallback)" if cog_val is not None else "not available"
            cog_str = f"{s.cog}°" if cog_val is not None else "not available"
            sog_str = f"{s.sog} kn" if s.sog not in (102.3, 1024, None) and s.sog is not None else "not available"
            # sanitize heading for display: never show 511
            if s.heading == 511 or str(s.heading) == "511":
                heading_str = "not available (COG fallback)"
            desc = f"<![CDATA[<b>{html.escape(vessel_name)}</b> ({html.escape(code)})<br/>Operator: {html.escape(operator)}<br/>Type: {html.escape(vtype)}<br/>MMSI: {mmsi} | IMO: {imo} | Call: {html.escape(call)} | Flag: {flag} | Length: {length}<br/>Position: {s.lat:.5f}, {s.lon:.5f}<br/>HEADING: {heading_str} (source: {heading_src}) | COG: {cog_str} | SOG: {sog_str}<br/>AIS TIME: {html.escape(friendly_utc_stamp(s.seen))} | Fetched: {html.escape(fetched_friendly)}<br/>Source: Open Waters — attribution preserved<br/><i>{html.escape(DISCLAIMER)}</i>]]>"
            extended = f'<ExtendedData><Data name="mmsi"><value>{mmsi}</value></Data><Data name="imo"><value>{imo}</value></Data><Data name="heading"><value>{s.heading if s.heading is not None else ""}</value></Data><Data name="cog"><value>{s.cog if s.cog is not None else ""}</value></Data><Data name="sog"><value>{s.sog if s.sog is not None else ""}</value></Data><Data name="ais_time"><value>{html.escape(str(s.seen))}</value></Data><Data name="fetched_at"><value>{html.escape(fetched_at)}</value></Data><Data name="source"><value>Open Waters (ais.openwaters.io)</value></Data><Data name="callsign"><value>{html.escape(call)}</value></Data></ExtendedData>'
            kml_lines.append(f'  <Placemark id="{mmsi}"><name>{html.escape(vessel_name)}</name><styleUrl>#vesselActive</styleUrl>')
            if icon_h is not None:
                kml_lines.append(f'    <Style><IconStyle><heading>{icon_h}</heading><Icon><href>icons/Copilot_20260831_192510.png</href></Icon></IconStyle></Style>')
            kml_lines.append(f'    <description>{desc}</description>')
            kml_lines.append(f'    <Point><coordinates>{s.lon:.5f},{s.lat:.5f},0</coordinates></Point>')
            kml_lines.append(f'    {extended}')
            kml_lines.append(f'  </Placemark>')
        else:
            desc_off = f"<![CDATA[<b>{html.escape(vessel_name)}</b> ({html.escape(code)})<br/>Operator: {html.escape(operator)}<br/>MMSI: {mmsi} | IMO: {imo} | Flag: {flag}<br/><b style=\"color:#cc0000\">AIS status: No current position received</b> (no Open Waters record within 30-min window)<br/>Permanent placemark retained — not moved to estimated position, not deleted, not substituted.<br/>Fetched: {html.escape(fetched_friendly)} | Source: Open Waters<br/><i>{html.escape(DISCLAIMER)}</i>]]>"
            kml_lines.append(f'  <Placemark id="{mmsi}"><name>{html.escape(vessel_name)} (offline)</name><styleUrl>#vesselOffline</styleUrl><description>{desc_off}</description><Point><coordinates>0,0,0</coordinates></Point><visibility>0</visibility><ExtendedData><Data name="mmsi"><value>{mmsi}</value></Data><Data name="imo"><value>{imo}</value></Data><Data name="status"><value>No current position</value></Data><Data name="fetched_at"><value>{html.escape(fetched_at)}</value></Data><Data name="source"><value>Open Waters (ais.openwaters.io)</value></Data></ExtendedData></Placemark>')
    kml_lines.append('</Document></kml>')
    return "\n".join(kml_lines)

async def snapshot_recovery():
    # NOTE: the vessels bbox endpoint requires a personal token
    # (anonymous bbox returns HTTP 400 "bbox not allowed for this key"),
    # so the key must be sent here — not just on the WebSocket.
    token = os.environ.get("OPENWATERS_TOKEN", "")
    snapshot_url = SNAPSHOT_URL + ("?key=" + token if token else "")
    print(f"Snapshot recovery: GET {SNAPSHOT_URL} ({'authenticated' if token else 'anonymous'} — token present: {'YES' if token else 'NO'})")
    if not token:
        print("Snapshot skipped: OPENWATERS_TOKEN not set, anonymous bbox is rejected (HTTP 400) — WebSocket will be the data source")
        return 0
    try:
        req = urllib.request.Request(snapshot_url, headers={"User-Agent": "OpenWatersFetch/1.0", "Accept": "application/json"})
        loop = asyncio.get_running_loop()
        def _do_fetch():
            with urllib.request.urlopen(req, timeout=20) as resp:
                import gzip
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode())
        data = await loop.run_in_executor(None, _do_fetch)
        features = data.get("features", [])
        print(f"Snapshot: {len(features)} vessels in bbox")
        matched = 0
        for f in features:
            props = f.get("properties", {})
            mmsi = str(props.get("mmsi"))
            if mmsi not in roster_mmsi_set:
                continue
            geom = f.get("geometry", {}).get("coordinates", [None, None])
            lon, lat = geom[0], geom[1] if len(geom) >= 2 else (None, None)
            if lat is None or lon is None:
                continue
            s = state[mmsi]
            s.lat = lat
            s.lon = lon
            if props.get("sog") is not None:
                s.sog = props.get("sog")
            if props.get("cog") is not None:
                s.cog = props.get("cog")
            if props.get("heading") is not None:
                s.heading = props.get("heading")
            s.source = props.get("source")
            s.station = props.get("station")
            s.seen = props.get("seen")
            s.last_update = time_module.time()
            s.status = "live"
            s.dirty = True
            matched += 1
        print(f"Snapshot matched {matched}/118 roster MMSIs")
        return matched
    except Exception as e:
        # Log the response body (token-redacted) so HTTP 400s are diagnosable
        # from the Actions log instead of guessing at the cause.
        body = ""
        try:
            # Duck-typed HTTPError check (avoids rebinding the `urllib` name
            # imported at module top level).
            if hasattr(e, "read") and hasattr(e, "code"):
                raw = e.read()
                try:
                    raw = raw.decode("utf-8", errors="replace")
                except Exception:
                    raw = repr(raw)
                body = raw[:300].replace(token, "***") if token else raw[:300]
        except Exception:
            pass
        print(f"Snapshot failed: {e} body={body!r}")
        return 0

async def websocket_loop(duration):
    token = os.environ.get("OPENWATERS_TOKEN", "")
    if not token:
        print("ERROR: OPENWATERS_TOKEN not set (use GitHub Secret OPENWATERS_TOKEN)", file=sys.stderr)
        print("token present: NO")
        return False
    print("token present: YES")
    # Do not print token value
    uri = "wss://ais.openwaters.io/v1/stream"
    backoff = 1
    max_backoff = 60
    start = time_module.time()
    end_time = start + duration if duration > 0 else start + 86400
    uri_with_key = uri + "?key=" + token
    while time_module.time() < end_time:
        try:
            print(f"Connecting to wss://ais.openwaters.io/v1/stream with personal token, bbox {BBOX} — token present: YES")
            async with websockets.connect(uri_with_key, ping_interval=20, ping_timeout=10, max_queue=1024) as ws:
                sub = {"type": "subscribe", "bbox": BBOX}
                await ws.send(json.dumps(sub))
                print(f"Sent subscribe {sub} with personal token")
                backoff = 1
                async for msg in ws:
                    if time_module.time() >= end_time:
                        break
                    if event_queue.qsize() >= 900:
                        stats["queue_depth_max"] = max(stats["queue_depth_max"], event_queue.qsize())
                    try:
                        data = json.loads(msg)
                    except:
                        continue
                    if data.get("type") == "error":
                        print(f"ERROR frame: {data}")
                        errors.append((datetime.now(timezone.utc).isoformat(), data))
                        break
                    if data.get("type") != "event":
                        continue
                    stats["events_received"] += 1
                    mmsi = str(data.get("mmsi"))
                    if mmsi not in roster_mmsi_set:
                        stats["unrostered_discarded"] += 1
                        continue
                    stats["roster_matched"] += 1
                    s = state[mmsi]
                    old_lat, old_lon = s.lat, s.lon
                    if data.get("lat") is not None and data.get("lon") is not None:
                        s.lat = data["lat"]
                        s.lon = data["lon"]
                        if old_lat != s.lat or old_lon != s.lon:
                            stats["position_changes"] += 1
                    if data.get("message") and isinstance(data["message"], dict):
                        msg_inner = data["message"]
                        if isinstance(msg_inner, dict) and len(msg_inner) == 1 and isinstance(list(msg_inner.values())[0], dict):
                            msg_inner = list(msg_inner.values())[0]
                        if isinstance(msg_inner, dict):
                            if "Sog" in msg_inner and msg_inner["Sog"] != 102.3:
                                s.sog = msg_inner["Sog"]
                            if "Cog" in msg_inner and msg_inner["Cog"] != 360:
                                s.cog = msg_inner["Cog"]
                            if "TrueHeading" in msg_inner and msg_inner["TrueHeading"] != 511:
                                s.heading = msg_inner["TrueHeading"]
                            if "heading" in msg_inner and msg_inner.get("heading") is not None and msg_inner.get("heading") != 511:
                                s.heading = msg_inner["heading"]
                            if "sog" in msg_inner and msg_inner.get("sog") is not None and msg_inner.get("sog") != 102.3:
                                s.sog = msg_inner["sog"]
                            if "cog" in msg_inner and msg_inner.get("cog") is not None and msg_inner.get("cog") != 360:
                                s.cog = msg_inner["cog"]
                    s.source = data.get("source", s.source)
                    s.station = data.get("station", s.station)
                    s.msg_type = data.get("msg_type", s.msg_type)
                    if data.get("time"):
                        s.seen = data["time"]
                    s.last_update = time_module.time()
                    s.status = "live"
                    s.dirty = True
                    try:
                        event_queue.put_nowait(1)
                    except:
                        pass
                    if stats["roster_matched"] <= 3 or stats["roster_matched"] % 50 == 0:
                        print(f"Roster match {stats['roster_matched']}: MMSI {mmsi} {s.lat},{s.lon} h {s.heading} cog {s.cog} at {s.seen} source {s.source}")
                    if time_module.time() >= end_time:
                        break
        except Exception as e:
            # Check for 429-like message
            msg = str(e)
            is_429 = "429" in msg or "rate" in msg.lower()
            print(f"WebSocket error: {e} — backoff {backoff}s")
            errors.append((datetime.now(timezone.utc).isoformat(), msg))
            if time_module.time() >= end_time:
                break
            # snapshot recovery on reconnect (once)
            try:
                await snapshot_recovery()
            except:
                pass
            await asyncio.sleep(backoff + random.uniform(0, 1))
            backoff = min(backoff * 2, max_backoff)
            stats["reconnects"] += 1
            if time_module.time() >= end_time:
                break
        if time_module.time() >= end_time:
            break
    return True

async def kml_writer(duration):
    # Run for duration + small buffer, flushing every 7 sec
    start = time_module.time()
    end_time = start + duration + 5  # allow final flush after websocket ends
    while time_module.time() < end_time:
        await asyncio.sleep(7)
        drained = 0
        while not event_queue.empty():
            try:
                event_queue.get_nowait()
                drained += 1
            except:
                break
        dirty = [m for m, s in state.items() if s.dirty]
        if not dirty:
            if time_module.time() >= start + duration:
                # final check after collection ended
                pass
            continue
        kml = build_kml()
        ids = re.findall(r'<Placemark id="(\d{9})">', kml)
        if len(ids) != 118 or len(set(ids)) != 118 or set(ids) != roster_mmsi_set:
            print(f"KML validation failed: {len(ids)} placemarks, expected 118 unique")
            continue
        # Validate no 511 heading written
        headings = re.findall(r'<heading>(\d+)</heading>', kml)
        if any(int(h) == 511 for h in headings):
            print("KML validation failed: invalid heading 511 found")
            continue
        # Atomic tmp->replace into final production path, but only if we have file handle
        # For workflow fail-safe, write to a staging tmp first, then replace
        tmp = OUTPUT_KML.with_suffix(".tmp")
        try:
            tmp.write_text(kml)
            # XML valid check
            import xml.etree.ElementTree as ET
            ET.fromstring(kml.encode())
            tmp.replace(OUTPUT_KML)
            for m in dirty:
                state[m].dirty = False
            stats["kml_flushes"] += 1
            print(f"KML flushed @ {datetime.now(timezone.utc).strftime('%H:%M:%S')} — {len(dirty)} dirty, flushes {stats['kml_flushes']}")
        except Exception as e:
            print(f"KML write failed: {e}")

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Open Waters AIS fetch — Great Lakes 118")
    parser.add_argument("--duration", type=int, default=75, help="WebSocket collection duration in seconds (default 75, 0= snapshot only)")
    parser.add_argument("--snapshot-only", action="store_true", help="Only do snapshot recovery, no WebSocket")
    args = parser.parse_args()
    duration = args.duration
    if args.snapshot_only:
        duration = 0
    print(f"=== Open Waters AIS fetch starting — bbox {BBOX} area 233.75 sq° (personal 400) rosters 118 ===")
    print(f"Duration: {duration}s, roster 118, snapshot recovery enabled, token present: {'YES' if os.environ.get('OPENWATERS_TOKEN') else 'NO'}")
    # Backup existing valid KML before any overwrite
    backup = None
    if OUTPUT_KML.exists():
        try:
            backup = pathlib.Path("/tmp/great_lakes_ais_openwaters.kml.bak")
            backup.write_text(OUTPUT_KML.read_text())
            print(f"Backed up existing KML to {backup}")
        except Exception as e:
            print(f"Backup failed: {e}")
    # Initial snapshot recovery (anonymous, before WebSocket)
    snapshot_matched = await snapshot_recovery()
    if duration == 0:
        print("Snapshot-only mode — building KML from snapshot")
        kml = build_kml()
        ids = re.findall(r'<Placemark id="(\d{9})">', kml)
        if len(ids) != 118 or len(set(ids)) != 118:
            print(f"KML validation failed: {len(ids)} placemarks — preserving previous valid KML")
            if backup and backup.exists():
                try:
                    pathlib.Path("/tmp/great_lakes_ais.kml.bak").write_text(backup.read_text())
                except:
                    pass
            sys.exit(1)
        # Check no 511
        headings = re.findall(r'<heading>(\d+)</heading>', kml)
        if any(int(h) == 511 for h in headings):
            print("KML validation failed: 511 heading present")
            sys.exit(1)
        tmp = OUTPUT_KML.with_suffix(".tmp")
        tmp.write_text(kml)
        import xml.etree.ElementTree as ET
        ET.fromstring(kml.encode())
        tmp.replace(OUTPUT_KML)
        print(f"Wrote KML {OUTPUT_KML} — snapshot matched {snapshot_matched}/118, live {sum(1 for s in state.values() if s.status=='live')}")
        print("token present: YES" if os.environ.get("OPENWATERS_TOKEN") else "token present: NO")
        return
    # Check token present before WebSocket
    if not os.environ.get("OPENWATERS_TOKEN"):
        print("ERROR: OPENWATERS_TOKEN not set — cannot open WebSocket (requires personal token for 233 sq°)", file=sys.stderr)
        # Preserve existing KML: do not overwrite with empty snapshot
        print("Preserving existing valid KML due to missing token")
        sys.exit(2)
    # Start concurrent tasks: websocket loop and kml writer
    writer_task = asyncio.create_task(kml_writer(duration))
    ws_task = asyncio.create_task(websocket_loop(duration))
    try:
        await asyncio.wait_for(asyncio.gather(writer_task, ws_task), timeout=duration + 30)
    except asyncio.TimeoutError:
        print(f"{duration}s collection timeout, finalizing")
        writer_task.cancel()
        ws_task.cancel()
        try:
            await writer_task
        except:
            pass
        try:
            await ws_task
        except:
            pass
    # Final KML flush if not yet done
    kml = build_kml()
    ids = re.findall(r'<Placemark id="(\d{9})">', kml)
    if len(ids) != 118 or len(set(ids)) != 118 or set(ids) != roster_mmsi_set:
        print(f"FINAL KML validation failed: {len(ids)} placemarks — preserving previous valid KML")
        if backup and backup.exists():
            # Restore will be handled by workflow; just ensure current file not corrupted
            print("Restoring backup due to validation failure")
            try:
                OUTPUT_KML.write_text(backup.read_text())
            except:
                pass
        sys.exit(1)
    headings = re.findall(r'<heading>(\d+)</heading>', kml)
    if any(int(h) == 511 for h in headings):
        print("FINAL KML validation failed: 511 heading present — preserving previous")
        sys.exit(1)
    # Atomic final write (kml_writer may have already flushed; this ensures final state)
    tmp = OUTPUT_KML.with_suffix(".tmp")
    tmp.write_text(kml)
    import xml.etree.ElementTree as ET
    ET.fromstring(kml.encode())
    # Only replace if we actually matched at least one roster vessel or had snapshot
    if stats["roster_matched"] == 0 and snapshot_matched == 0:
        print("No roster vessels matched in this window — preserving previous valid KML (not overwriting with empty)")
        # Remove tmp and exit without replacing? But we already would replace — prevent
        tmp.unlink(missing_ok=True)
        # Keep existing file (which we backed up) — ensure not overwritten
        if backup and backup.exists():
            OUTPUT_KML.write_text(backup.read_text())
        sys.exit(3)
    tmp.replace(OUTPUT_KML)
    print("\n=== FINAL REPORT ===")
    print(f"Reconnects: {stats['reconnects']}, events: {stats['events_received']}, roster_matched: {stats['roster_matched']}, unrostered: {stats['unrostered_discarded']}")
    print(f"Position changes: {stats['position_changes']}, KML flushes: {stats['kml_flushes']}, queue max: {stats['queue_depth_max']}, errors: {len(errors)}")
    live = sum(1 for s in state.values() if s.status == "live" and s.lat is not None and (time_module.time() - s.last_update) < 1800)
    print(f"Vessel state: live {live}, offline {118-live}")
    print(f"Snapshot matched {snapshot_matched}/118 initial")
    print("token present: YES" if os.environ.get("OPENWATERS_TOKEN") else "token present: NO")
    # Fail-safe: if no data, exit code signals workflow to preserve
    if stats["roster_matched"] == 0 and snapshot_matched == 0:
        sys.exit(3)

if __name__ == "__main__":
    asyncio.run(main())
