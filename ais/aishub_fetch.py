"""
AISHub Private Prototype Fetcher — $0 Production Path
- Single batch Great Lakes bbox + MMSI filter → JSON (format=1 human readable)
- MMSI is primary key — never name-match
- Updates existing permanent placemarks only (caller handles KML)
- Returns per-vessel: lat/lon, COG, SOG, HEADING, NAVSTAT, IMO, NAME, TYPE, TIME
- Handles offline / no-position correctly (no substitution)
- 5-min polling compatible (1 req/min limit → 5-min is 5x safe)
- Attribution preserved
Usage: USERNAME env var must be set (AISHub contributor username). For local test without account, script runs in mock mode with sample JSON.
"""
import os, json, sys, time, urllib.parse, urllib.request

AIS_HUB_URL = "https://data.aishub.net/ws.php"
# Verified Great Lakes bbox — covers all 5 lakes + Seaway to Montreal/Quebec approaches
BBOX = {"latmin": 41.0, "latmax": 49.5, "lonmin": -93.5, "lonmax": -66.0}
FORMAT = 1  # 1 = human readable (degrees, GMT time string)
OUTPUT = "json"
INTERVAL = 10  # max age minutes — returns positions updated within 10 min (use 15 for fallback)
ATTRIBUTION = "Source: AISHub (data.aishub.net) — Contributor network. Data may be delayed/incomplete/inaccurate. Not for navigation."
DISCLAIMER = "AIS data can be delayed, incomplete, or inaccurate and is not for navigation. Positions via AISHub aggregated terrestrial network."

def build_url(username, mmsi_list):
    mmsi_csv = ",".join([m for m in mmsi_list if m.isdigit() and len(m)==9])
    params = {
        "username": username,
        "format": str(FORMAT),
        "output": OUTPUT,
        "latmin": str(BBOX["latmin"]),
        "latmax": str(BBOX["latmax"]),
        "lonmin": str(BBOX["lonmin"]),
        "lonmax": str(BBOX["lonmax"]),
        "interval": str(INTERVAL),
    }
    if mmsi_csv:
        params["mmsi"] = mmsi_csv
    # Note: AISHub docs show duplicate imo param example bug — we send mmsi only, bbox covers
    qs = urllib.parse.urlencode(params)
    return f"{AIS_HUB_URL}?{qs}"

# Example mock response (format=1 human readable JSON) — structure per https://www.aishub.net/api JSON Samples
MOCK_JSON = {
    "ERROR": False, "USERNAME": "MOCK", "FORMAT": "HUMAN", "RECORDS": 2,
    "": [
        {"MMSI": 366904000, "TIME": "2026-08-27 12:08:05 GMT", "LONGITUDE": -83.04667, "LATITUDE": 42.01317, "COG": 48.7, "SOG": 12.3, "HEADING": 49, "ROT": 0, "NAVSTAT": 0, "IMO": 7723558, "NAME": "AMERICAN CENTURY", "CALLSIGN": "WDC123", "TYPE": 70, "A": 150, "B": 150, "C": 15, "D": 15, "DRAUGHT": 7.2, "DEST": "DETROIT", "ETA": "08-27 18:00"},
        {"MMSI": 316009090, "TIME": "2026-08-27 12:09:21 GMT", "LONGITUDE": -70.231, "LATITUDE": 46.123, "COG": 122.6, "SOG": 8.9, "HEADING": 119, "ROT": 0, "NAVSTAT": 0, "IMO": 9613927, "NAME": "ALGOMA EQUINOX", "CALLSIGN": "XJBH", "TYPE": 70, "A": 112, "B": 113, "C": 12, "D": 12, "DRAUGHT": 8.1, "DEST": "BAIE COMEAU", "ETA": "09-02 08:00"}
    ]
}

def parse_records(data):
    """Parse AISHub JSON — handle both [ERROR, RECORDS, array] and flat array quirks"""
    if isinstance(data, dict):
        # Newer format: first element is meta, second is list
        if "ERROR" in data:
            return []
    if isinstance(data, list) and len(data)==2 and isinstance(data[0], dict) and "ERROR" in data[0]:
        meta, vessels = data[0], data[1]
        return vessels if isinstance(vessels, list) else []
    if isinstance(data, dict) and "" in data:
        return data[""]
    if isinstance(data, list):
        # sometimes [meta, [vessels]]
        for el in data:
            if isinstance(el, list):
                return el
        return data
    return []

def fetch(username, mmsi_list, mock=False):
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if mock or not username or username=="USERNAME":
        print("[MOCK] No AISHub username — returning sample JSON (format=1 fields: TIME human GMT, LONGITUDE/LATITUDE degrees, COG degrees 360=NA, SOG knots 102.4=NA, HEADING 511=NA)")
        vessels = MOCK_JSON[""]
        source = "AISHub (mock) — attribution: AISHub "
        return {"fetched_at": fetched_at, "source": source, "bbox": BBOX, "interval_min": INTERVAL, "vessels": vessels, "record_count": len(vessels), "mock": True}
    url = build_url(username, mmsi_list)
    print(f"[LIVE] GET {url[:200]}...  (1 batch covering Great Lakes + {len(mmsi_list)} MMSIs, interval {INTERVAL}min, 1/5min = within 1/min limit)")
    req = urllib.request.Request(url, headers={"User-Agent": "GreatLakesAIS-prototype/1.0 (research)"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    vessels = parse_records(data)
    return {"fetched_at": fetched_at, "source": "AISHub (data.aishub.net)", "attribution": ATTRIBUTION, "disclaimer": DISCLAIMER, "bbox": BBOX, "interval_min": INTERVAL, "vessels": vessels, "record_count": len(vessels), "mock": False}

if __name__ == "__main__":
    # Load roster MMSIs — use production roster 118 MMSI list embedded in update_ais.py ROSTER_118
    # For portability, extract MMSIs from ais/update_ais.py
    import pathlib, json, re, importlib.util, sys
    # Load roster MMSIs from roster_118.json (no import side-effect)
    try:
        roster_data = json.load(open(pathlib.Path(__file__).parent / "roster_118.json"))
        mmsis = [str(r["mmsi"]) for r in roster_data]
        mmsis = [re.sub(r'\D','',m) for m in mmsis if len(re.sub(r'\D','',m))==9]
    except Exception as e:
        print(f"Warning: could not load roster_118.json ({e}), falling back to minimal list")
        mmsis=["366904000","316009090"]
    username=os.environ.get("AISHUB_USERNAME","USERNAME")
    # --live requires username, else fail for workflow fallback; --mock forces mock
    requested_live = "--live" in sys.argv
    requested_mock = "--mock" in sys.argv
    if requested_live and (not username or username=="USERNAME"):
        print("ERROR: --live requested but AISHUB_USERNAME not set (use GitHub Secret)", file=sys.stderr)
        sys.exit(2)
    mock = requested_mock or (not username or username=="USERNAME")
    # If no flag, auto-detect: live if username present else mock
    if not requested_live and not requested_mock:
        mock = not username or username=="USERNAME"
    result=fetch(username, mmsis, mock=mock)
    # Production snapshot path (repo-relative)
    out = pathlib.Path(__file__).parent / "latest_snapshot.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"Wrote {out} — {result['record_count']} vessels (mock={result['mock']}) for {len(mmsis)} MMSIs batched (Great Lakes bbox {BBOX})")
    for v in result["vessels"][:3]:
        print(json.dumps(v, indent=2))
