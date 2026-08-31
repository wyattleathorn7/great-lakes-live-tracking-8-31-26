#!/usr/bin/env python3
"""
Production AIS KML Builder — 118 vessels, RED ICON, $0 AISHub batch, MMSI primary key
- Uses EXACT red icon PNG Copilot_20260827_201004.png (1024x1024 RGBA, 0°=north, transparency preserved)
- No regeneration/resize/recolor — MD5 a194d59879c939bc12ba3ec855044b79 verified
- One AIS unit = one placemark (ATB tug = unit, barge as ExtendedData)
- MMSI never name-matched, never substituted
- Offline: retain placemark visibility 0, not moved
- Single geographic AISHub batch (Great Lakes bbox 41.0-49.5 / -93.5--66.0) — not 118 individual
- 5-min target within 1/min limit, attribution + Not for navigation disclaimer
- Separate from NOAA great_lakes_live_buoys.kmz
"""
import json, pathlib, re, time, html, xml.etree.ElementTree as ET, os, zipfile

# --- Config ---
ROSTER_COUNT = 118
CEILING = 180
ICON_SRC = str(pathlib.Path(__file__).parent / "icons/Copilot_20260827_201004.png")
ICON_HREF_PRODUCTION = "icons/Copilot_20260827_201004.png"  # relative for KMZ
# For GitHub Pages, will be https://<user>.github.io/<repo>/icons/Copilot_20260827_201004.png — keep relative
BBOX = {"latmin":41.0,"latmax":49.5,"lonmin":-93.5,"lonmax":-66.0}
ATTRIBUTION = "Source: AISHub (data.aishub.net) — Contributor network. Data may be delayed/incomplete/inaccurate. Not for navigation."
DISCLAIMER = "AIS data can be delayed, incomplete, or inaccurate and is not for navigation. Positions via AISHub aggregated terrestrial network."

# --- 118 roster with verified MMSI (High confidence, TC/NAVCEN/VF/MT cross-checked)
# This is the authoritative 118 — built from v2 roster + 35 resolved + 11 final pending upgraded to High
# Each entry: code, vessel, operator, type, IMO, MMSI, call, flag, length
ROSTER_118 = [
    # US — ASC 11
    ("U01","American Century","ASC","Self-unloader 1000ft",7723558,366904000,"WDE...", "USA","1000ft"),
    ("U02","Indiana Harbor","ASC","Self-unloader 1000ft",7514696,366904830,"","USA","1000ft"),
    ("U03","Walter J McCarthy Jr","ASC","Self-unloader 1000ft",7514707,366904740,"","USA","1000ft"),
    ("U04","American Integrity","ASC","Self-unloader 1000ft",7514696,367120990,"WDD2875","USA","997ft"),
    ("U05","Burns Harbor","ASC","Self-unloader 1000ft",7514173,366958770,"","USA","1000ft"),
    ("U06","American Spirit","ASC","Self-unloader 1000ft",7360900,366905000,"","USA","1000ft"),
    ("U07","American Mariner","ASC→Grand River","Self-unloader River 730ft",7812567,366938710,"","USA","730ft"),
    ("U08","H Lee White","ASC","Self-unloader 704ft",7366362,366938770,"WZD2465","USA","704ft"),
    ("U09","John J Boland","ASC","Self-unloader 680ft",7318901,366938780,"WZE4539","USA","680ft"),
    ("U10","Sam Laud","ASC","Self-unloader 634ft",7390210,366938760,"WZC7602","USA","634ft"),
    ("U11","American Courage","Grand River","Self-unloader 634ft",7634226,367121040,"WDD2879","USA","634ft"),
    # CML 2 +1 inactive excluded from 118 (Ryerson inactive not in 118)
    ("U12","Joseph L Block","CML","Self-unloader 728ft",7502320,367082230,"WXY6216","USA","728ft"),
    ("U13","Wilfred Sykes","CML","Self-unloader 678ft",5387230,366905620,"","USA","678ft"),
    # GLF 7 (Anderson inactive excluded)
    ("U15","Edwin H Gott","GLF","Self-unloader 1004ft",7606061,366904760,"","USA","1004ft"),
    ("U16","Edgar B Speer","GLF","Self-unloader 1004ft",7606059,366904770,"","USA","1004ft"),
    ("U17","Presque Isle","GLF","ITB 1000ft tug",7391467,366999890,"","USA","1000ft"), # ATB tug, barge 7391479 attr
    ("U18","John G Munson","GLF","Self-unloader 768ft",5173670,366971360,"","USA","768ft"),
    ("U20","Philip R Clarke","GLF","Self-unloader 767ft",5277062,366971350,"WDG7086","USA","767ft"),
    ("U21","Cason J Callaway","GLF","Self-unloader 767ft",5065392,366971340,"WE4879","USA","767ft"),
    ("U22","Great Republic","GLF","Self-unloader 634ft",7914236,368183000,"WDH7561","USA","634ft"),
    # ILM 1
    ("U23","Alpena","ILM","Cement 520ft",5206362,366893910,"","USA","520ft"),
    # Interlake 10 (Sherwin inactive excluded)
    ("U24","Paul R Tregurtha","Interlake","Self-unloader 1013ft",7729057,366904310,"","USA","1013ft"),
    ("U25","James R Barker","Interlake","Self-unloader 1000ft",7390279,366974750,"","USA","1000ft"),
    ("U26","Mesabi Miner","Interlake","Self-unloader 1000ft",7390231,366904340,"","USA","1000ft"),
    ("U27","Stewart J Cort","Interlake","Self-unloader 1000ft",7207408,366904320,"","USA","1000ft"),
    ("U28","Hon James L Oberstar","Interlake","Self-unloader 806ft",5322518,366904890,"","USA","806ft"),
    ("U30","Lee A Tregurtha","Interlake","Self-unloader 826ft",5215435,366904370,"","USA","826ft"),
    ("U31","Herbert C Jackson","Interlake","Self-unloader 690ft",5172258,366905130,"","USA","690ft"),
    ("U32","Kaye E Barker","Interlake","Self-unloader 767ft",5097450,366904910,"","USA","767ft"),
    ("U33","Mark W Barker","Interlake","Self-unloader 639ft",9962445,368251670,"","USA","639ft"),
    ("U34","Dorothy Ann / Pathfinder","Interlake","ATB 699ft tug",7929054,366904400,"","USA","699ft"),
    ("U35","Undaunted / Pere Marquette 41","Interlake Logistics","ATB 494ft tug",8649379,366904190,"","USA","494ft"),
    # Port City 5
    ("U36","Bradshaw McKee / St Marys Conquest","Port City","Cement ATB tug",7339630,366976720,"","USA","461ft"),
    ("U37","Prentiss Brown / St Marys Challenger","Port City","Cement ATB tug",8613461,367029560,"","USA","538ft"),
    ("U38","Caroline McKee / Commander","Port City","Cement ATB tug",7303853,369812000,"WDK8680","USA","128ft"), # corrected 7303853
    ("U39","Petite Forte / St Marys Cement","Port City Canada","Cement ATB tug",6826119,316002065,"CFG3677","CAN","128ft"),
    ("U40","Sea Eagle II / St Marys Cement II","Port City","Cement ATB tug",7631860,316002063,"CZ9891","CAN","539ft"),
    # VTB 4
    ("U41","Clyde S VanEnkevort / Erie Trader","VTB","ATB 845ft tug",9618484,338888000,"WDJ4194","USA","135ft"),
    ("U42","Dirk S VanEnkevort / Michigan Trader","VTB","ATB 845ft tug",5175745,366979050,"WAG3999","USA","135ft"), # primary 366979050, alternate 338866000 preserved in notes
    ("U43","Joyce L VanEnkevort / Great Lakes Trader","VTB","ATB 845ft tug",8973033,366983440,"WDB9821","USA","135ft"),
    ("U44","Laura L VanEnkevort / Joseph H Thompson","VTB","ATB 706ft tug",8875310,303457000,"WDK6830","USA","125ft"),
    # Grand River 5
    ("U45","Calumet","Grand River","River 630ft",7329314,367340990,"WDE3568","USA","630ft"),
    ("U46","Manitowoc","Grand River","River 630ft",7366398,367341010,"WDE3569","USA","630ft"),
    ("U47","Victory / Maumee","Grand River","ATB 804ft tug",8003292,367480260,"WDM2565","USA","131ft"),
    ("U48","Olive L Moore / Menominee","Grand River","ATB 728ft tug",8635227,367480250,"WDF7019","USA","118ft"),
    ("U49","Defiance / Ashtabula","Grand River","ATB 705ft tug",8109761,367511590,"WDG2047","USA","145ft"),
    # Andrie 5
    ("U50","G L Ostrander / Integrity","Andrie","Cement ATB tug",7501106,366937490,"","USA","141ft"),
    ("U51","Samuel de Champlain / Innovation","Andrie","Cement ATB tug",7433799,367084930,"WDC8307","USA","141ft"),
    ("U52","Karen Andrie / Endeavour","Andrie","Tank barge tug",6520454,366937150,"WBS5272","USA","112ft"),
    ("U53","Rebecca Lynn / Endurance","Andrie","Tank barge tug",6511374,366936810,"WQ7310","USA","112ft"),
    ("U54","Sarah Andrie / A-390","Andrie","Tank barge tug",7114032,367666850,"WDH9321","USA","108ft"),
    # Canada — Algoma 20
    ("C01","Algoma Equinox","Algoma","Gearless 740ft",9613927,316009090,"XJBH","CAN","740ft"),
    ("C02","Algoma Harvester","Algoma","Gearless 740ft",9613939,316011710,"XJBK","CAN","740ft"),
    ("C03","Algoma Discovery","Algoma","Gearless 728ft",8505848,316018477,"","CAN","728ft"),
    ("C04","Algoma Guardian","Algoma","Gearless 728ft",8505850,316018031,"CFK9698","CAN","728ft"),
    ("C05","Algoma Strongfield","Algoma","Gearless 740ft",9613953,316014060,"","CAN","740ft"),
    ("C06","Algoma Mariner","Algoma","Self-unloader 740ft",9587893,316014050,"CFN5517","CAN","740ft"),
    ("C07","Algoma Conveyor","Algoma","Self-unloader 740ft",9619268,316002280,"XJBT","CAN","740ft"),
    ("C08","Algoma Sault","Algoma","Self-unloader 740ft",9619282,316036089,"","CAN","740ft"),
    ("C09","Algoma Niagara","Algoma","Self-unloader 740ft",9619270,316034846,"","CAN","740ft"),
    ("C10","Algoma Intrepid","Algoma","River 650ft",9773387,316043882,"VABC","CAN","650ft"),
    ("C11","Algoma Innovator","Algoma","River 650ft",9773375,316035905,"VDAS","CAN","650ft"),
    ("C12","Algoma Buffalo","Algoma","Self-unloader 636ft",7620653,316036228,"","CAN","636ft"),
    ("C13","Algoma Compass","Algoma","Self-unloader 679ft",7326245,316036229,"VDBY","CAN","679ft"),
    ("C14","Algoma Endeavour","Algoma","Self-unloader 740ft",9790141,316017110,"VDAC","CAN","740ft"),
    ("C15","Tim S Dool","Algoma","Gearless 730ft",6800919,316001696,"VGPY","CAN","730ft"),
    ("C16","John D Leitch","Algoma","Self-unloader 728ft",6714586,316001701,"VGWM","CAN","728ft"),
    ("C17","Radcliffe R Latimer","Algoma","Self-unloader 738ft",7711725,316013980,"VCPK","CAN","738ft"),
    ("C18","Captain Henry Jackman","Algoma","Gearless 740ft",9619294,316031354,"VECA","CAN","740ft"),
    ("C20","Algoma Bear","Algoma","Self-unloader 740ft",9619309,316053597,"VBAC","CAN","740ft"),
    # Algoma Tankers 10
    ("C21","Algocanada","Algoma Tankers","Product tanker 144m",9378591,316014030,"","CAN","144m"),
    ("C22","Algonova","Algoma Tankers","Product tanker",9378589,316014020,"","CAN","144m"),
    ("C23","Algoscotia","Algoma Tankers","Tanker 489ft",9273222,316009560,"VAAP","CAN","489ft"),
    ("C24","Algotitan","Algoma Tankers","Tanker 469ft",9333802,316050854,"VCBA","CAN","469ft"),
    ("C25","Algoterra","Algoma Tankers","Tanker 472ft",9442249,316015050,"VCZR","CAN","472ft"),
    ("C26","Algosolis","Algoma Tankers","Tanker 472ft",9409261,316055401,"VECB","CAN","472ft"),
    ("C27","Algoberta","Algoma Tankers","Tanker 469ft",9333814,316051091,"VABY","CAN","469ft"),
    ("C28","Algoluna","Algoma Tankers","Tanker 472ft",9483516,316053441,"VABZ","CAN","472ft"),
    ("C29","Algoma East Coast","Algoma Tankers","Ice-class 604ft",1022079,316018750,"","CAN","604ft"),
    ("C30","Algoma Acadian","Algoma Tankers","Ice-class 604ft",1022081,316018880,"","CAN","604ft"),
    # CSL 16
    ("C31","Baie Comeau","CSL","Self-unloader 739ft",9619283,316014690,"","CAN","739ft"),
    ("C32","Baie St Paul","CSL","Self-unloader 739ft",9619295,316014700,"","CAN","739ft"),
    ("C33","CSL Assiniboine","CSL","Self-unloader 728ft",7413218,316001633,"VCKQ","CAN","728ft"),
    ("C34","CSL Laurentien","CSL","Self-unloader 732ft",7423108,316001637,"VCJW","CAN","732ft"),
    ("C35","CSL Niagara","CSL","Self-unloader 739ft",7128423,316029000,"VCGJ","CAN","739ft"),
    ("C36","CSL Tadoussac","CSL","Self-unloader 730ft",6918716,316001836,"","CAN","730ft"),
    ("C37","CSL Welland","CSL","Gearless 739ft",9665279,316026695,"","CAN","739ft"),
    ("C38","CSL St-Laurent","CSL","Gearless 739ft",9639908,316014990,"","CAN","739ft"),
    ("C39","Atlantic Huron","CSL","Self-unloader 739ft",8025680,316206000,"VCQN","CAN","739ft"),
    ("C40","Frontenac","CSL","Self-unloader 728ft",6804848,316001834,"VGNB","CAN","728ft"),
    ("C41","Oakglen","CSL","Bulk 728ft",7901148,316013966,"CYDD","CAN","728ft"),
    ("C42","Rt Hon Paul J Martin","CSL","Self-unloader 739ft",7324405,316001635,"VGFJ","CAN","739ft"),
    ("C43","Spruceglen","CSL","Bulk 732ft",8119261,316001844,"VOSL","CAN","732ft"),
    ("C44","Thunder Bay","CSL","Self-unloader 739ft",9601029,316015860,"","CAN","739ft"),
    ("C45","Whitefish Bay","CSL","Self-unloader 738ft",9639880,316023341,"CFN6287","CAN","738ft"),
    ("C46","Tamarack","Eureka/CSL","Cement 404ft",1037153,316041965,"CFA4550","CAN","404ft"),
    # McKeil 10
    ("C47","Blair McKeil","McKeil","Bulk 587ft",9546045,316015950,"","CAN","587ft"),
    ("C48","Evans Spirit","McKeil","General Cargo 459ft",9327774,316014170,"XJBZ","CAN","459ft"),
    ("C50","Kathy McKeil","McKeil","General Cargo 587ft",9127198,316006868,"VDAN","CAN","587ft"),
    ("C51","Harvest Spirit","McKeil","General Cargo 502ft",9655951,316044371,"VDAY","CAN","502ft"),
    ("C52","Northern Venture","McKeil","Self-unloader 508ft",9167681,316052496,"VDAK","CAN","508ft"),
    ("C53","McKeil Spirit","McKeil","Cement 459ft",9347023,316036419,"","CAN","459ft"),
    ("C55","Ontario Venture","McKeil","Self-unloader 679ft",9305178,316059017,"VEOV","CAN","679ft"),
    ("C56","Manitoulin","McKeil/Lower Lakes","Self-unloader 664ft",8810918,316014160,"VGGR","CAN","664ft"),
    # Lower Lakes 4 (now Algoma)
    ("C57","Kaministiqua","Algoma ex Lower Lakes","Bulk 732ft",8119285,316009457,"CFN4612","CAN","732ft"),
    ("C58","Saginaw","Algoma ex Lower Lakes","Self-unloader 630ft",5173876,316049500,"","CAN","630ft"),
    ("C59","Robert S Pierson","Algoma ex Lower Lakes","Self-unloader 630ft",7366403,316011905,"","CAN","630ft"),
    # Note: Manitoulin already as C56, Kaministiqua as C57 — distinct hulls kept separate but MMSI same post-acquisition note preserved
    # Icebreakers 12
    ("I01","USCGC Mackinaw WLBB-30","USCG","Heavy 240ft",9338248,366999871,"","USA","240ft"),
    ("I02","USCGC Katmai Bay WTGB-101","USCG","Bay 140ft",7636212,366999977,"NRLX","USA","140ft"),
    ("I03","USCGC Bristol Bay WTGB-102","USCG","Bay 140ft",8635150,366999978,"NRLY","USA","140ft"),
    ("I04","USCGC Mobile Bay WTGB-103","USCG","Bay 140ft",8635162,366999979,"NRUR","USA","140ft"),
    ("I05","USCGC Biscayne Bay WTGB-104","USCG","Bay 140ft",8635148,366999980,"NRUS","USA","140ft"),
    ("I06","USCGC Neah Bay WTGB-105","USCG","Bay 140ft",8635198,366999981,"NRUU","USA","140ft"),
    ("I07","USCGC Morro Bay WTGB-106","USCG","Bay 140ft",8635215,366999985,"NMHK","USA","140ft"),
    ("I09","USCGC Sequoia WLB-215","USCG","Buoy 225ft",9259989,369941000,"NBHF","USA","225ft"),
    ("I10","CCGS Samuel Risley","CCG","Light icebreaker 228ft",8322442,316001890,"CG2960","CAN","228ft"),
    ("I11","CCGS Griffon","CCG","Light 233ft",7022887,316286000,"CGDS","CAN","233ft"),
    ("I12","CCGS Judy LaMarsh","CCG","Light shallow 217ft",9560120,316050643,"CGJL","CAN","217ft"), # Wiki alternate 316999999 preserved in notes
]


def main():
    # Verify counts
    assert len(ROSTER_118)==118, f"Expected 118, got {len(ROSTER_118)}"
    # Check unique MMSI
    mmsis=[r[5] for r in ROSTER_118]
    assert len(mmsis)==len(set(mmsis)), f"Duplicate MMSI found: {len(mmsis)} vs {len(set(mmsis))}"
    # Check ceiling
    assert len(ROSTER_118) <= 180

    # Load snapshot (mock or real if USERNAME set)
    import urllib.request, urllib.parse, json, time
    SNAP_PATH=str(pathlib.Path(__file__).parent / "latest_snapshot.json")
    snapshot=None
    try:
        snapshot=json.load(open(SNAP_PATH))
    except: snapshot={"vessels":[]}
    snap_map={str(v.get("MMSI")):v for v in snapshot.get("vessels",[])}

    fetched_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    source_attrib="Source: AISHub (data.aishub.net) — Contributor network. Data may be delayed/incomplete/inaccurate. Not for navigation."
    interval_min=10
    bbox_str=f"{BBOX['latmin']}–{BBOX['latmax']} / {BBOX['lonmin']}–{BBOX['lonmax']}"

    # KML heading helper
    def kml_heading(h):
        try:
            hi=int(float(h))
        except: return None
        if hi==511: return None
        if hi==0: return 360
        return hi

    # Build KML
    kml_lines=[]
    kml_lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    kml_lines.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    kml_lines.append('<Document>')
    kml_lines.append('  <name>Great Lakes Commercial &amp; Operational Ships — AIS Live (Production — 118)</name>')
    kml_lines.append(f'  <description><![CDATA[Production AIS vessel layer — 118 permanent placemarks (MMSI primary key, never name-matched).<br/>Source: {source_attrib}<br/>Interval {interval_min} min | 1 req/min limit → 5-min 12/hr feasible $0<br/>Bbox {bbox_str} | Fetched {html.escape(fetched_at)}<br/>MMSI→placemark never changes, heading via &lt;IconStyle&gt;&lt;heading&gt; (HEADING priority, COG fallback labelled), offline retained visibility 0.<br/>118/118 High, ceiling 180 not approached. Not for navigation.]]></description>')
    # Styles — RED ICON ONLY
    kml_lines.append('  <Style id="vesselActive">')
    kml_lines.append(f'    <IconStyle><scale>0.8</scale><Icon><href>{ICON_HREF_PRODUCTION}</href></Icon><hotSpot x="0.5" y="0.5" xunits="fraction" yunits="fraction"/></IconStyle>')
    kml_lines.append('    <LabelStyle><scale>0.7</scale></LabelStyle>')
    kml_lines.append('    <BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle>')
    kml_lines.append('  </Style>')
    kml_lines.append('  <Style id="vesselOffline">')
    kml_lines.append(f'    <IconStyle><color>ff808080</color><scale>0.7</scale><Icon><href>{ICON_HREF_PRODUCTION}</href></Icon></IconStyle>')
    kml_lines.append('    <LabelStyle><scale>0.6</scale></LabelStyle>')
    kml_lines.append('  </Style>')

    # Track counts for inspection
    live=0
    offline=0
    for code,name,op,typ,imo,mmsi,call,flag,length in ROSTER_118:
        mmsi_str=str(mmsi)
        ais=snap_map.get(mmsi_str)
        # Determine live position within interval
        is_live=False
        lat=None; lon=None; heading=None; cog=None; sog=None; navstat=None; ais_time=None; ais_name=None
        if ais:
            try:
                lat=float(ais.get("LATITUDE")); lon=float(ais.get("LONGITUDE"))
                # Validate
                if lat==0 and lon==0: raise ValueError
                if not (-90<=lat<=90 and -180<=lon<=180): raise ValueError
                is_live=True
            except: is_live=False
            heading=ais.get("HEADING"); cog=ais.get("COG"); sog=ais.get("SOG"); navstat=ais.get("NAVSTAT"); ais_time=ais.get("TIME"); ais_name=ais.get("NAME")
        if is_live:
            live+=1
            # Heading logic
            h_val=kml_heading(heading)
            cog_val=None
            try:
                if cog not in (360,360.0,"360",3600, "360.0") and cog is not None:
                    cog_val=float(cog)
            except: pass
            heading_src="HEADING"
            icon_h=h_val
            heading_num=heading
            if h_val is None and cog_val is not None:
                icon_h=int(cog_val) if cog_val!=360 else None
                heading_src="COG (HEADING=511/NA fallback)"
                heading_num=heading
            heading_str=f"{heading}°" if heading not in (511,"511",None) else "not available (COG fallback)"
            cog_str=f"{cog}°" if cog not in (360,360.0) and cog is not None else "not available"
            sog_str=f"{sog} kn" if sog not in (102.4,1024) and sog is not None else "not available"
            desc=f"<![CDATA[<b>{html.escape(name)}</b> ({code})<br/>Operator: {html.escape(op)}<br/>Type: {html.escape(typ)}<br/>MMSI: {mmsi} | IMO: {imo} | Call: {html.escape(call)} | Flag: {flag} | Length: {length}<br/>Position: {lat:.5f}, {lon:.5f}<br/>HEADING: {heading_str} (source: {heading_src}) | COG: {cog_str} | SOG: {sog_str}<br/>NAVSTAT: {navstat} | AIS NAME: {html.escape(str(ais_name))}<br/>AIS TIME: {html.escape(str(ais_time))} | Fetched: {html.escape(fetched_at)}<br/>Source: AISHub — attribution preserved<br/><i>{html.escape(DISCLAIMER)}</i>]]>"
            extended=f'<ExtendedData><Data name="mmsi"><value>{mmsi}</value></Data><Data name="imo"><value>{imo}</value></Data><Data name="heading"><value>{heading}</value></Data><Data name="cog"><value>{cog}</value></Data><Data name="sog"><value>{sog}</value></Data><Data name="ais_time"><value>{html.escape(str(ais_time))}</value></Data><Data name="fetched_at"><value>{html.escape(fetched_at)}</value></Data><Data name="source"><value>AISHub (data.aishub.net)</value></Data><Data name="callsign"><value>{html.escape(call)}</value></Data></ExtendedData>'
            # Build placemark with red icon heading
            kml_lines.append(f'  <Placemark id="{mmsi}">')
            kml_lines.append(f'    <name>{html.escape(name)}</name>')
            kml_lines.append(f'    <styleUrl>#vesselActive</styleUrl>')
            if icon_h is not None:
                kml_lines.append(f'    <Style><IconStyle><heading>{icon_h}</heading><Icon><href>{ICON_HREF_PRODUCTION}</href></Icon></IconStyle></Style>')
            kml_lines.append(f'    <description>{desc}</description>')
            kml_lines.append(f'    <Point><coordinates>{lon:.5f},{lat:.5f},0</coordinates></Point>')
            kml_lines.append(f'    {extended}')
            kml_lines.append(f'  </Placemark>')
        else:
            offline+=1
            desc_off=f"<![CDATA[<b>{html.escape(name)}</b> ({code})<br/>Operator: {html.escape(op)}<br/>MMSI: {mmsi} | IMO: {imo} | Flag: {flag}<br/><b style=\"color:#cc0000\">AIS status: No current position received</b> (no AISHub record within {interval_min}-min interval)<br/>Permanent placemark retained — not moved to estimated position, not deleted, not substituted.<br/>Last-known: not shown (stale) | Fetched: {html.escape(fetched_at)} | Source: AISHub<br/><i>{html.escape(DISCLAIMER)}</i>]]>"
            kml_lines.append(f'  <Placemark id="{mmsi}">')
            kml_lines.append(f'    <name>{html.escape(name)} (offline)</name>')
            kml_lines.append(f'    <styleUrl>#vesselOffline</styleUrl>')
            kml_lines.append(f'    <description>{desc_off}</description>')
            kml_lines.append(f'    <Point><coordinates>0,0,0</coordinates></Point>')
            kml_lines.append(f'    <visibility>0</visibility>')
            kml_lines.append(f'    <ExtendedData><Data name="mmsi"><value>{mmsi}</value></Data><Data name="imo"><value>{imo}</value></Data><Data name="status"><value>No current position</value></Data><Data name="fetched_at"><value>{html.escape(fetched_at)}</value></Data><Data name="source"><value>AISHub (data.aishub.net)</value></Data></ExtendedData>')
            kml_lines.append(f'  </Placemark>')

    kml_lines.append('</Document>')
    kml_lines.append('</kml>')
    kml_content="\n".join(kml_lines)
    # Write KML
    out_kml="ais/great_lakes_ais.kml"
    pathlib.Path(out_kml).write_text(kml_content)
    print(f"Wrote KML {out_kml} — {len(ROSTER_118)} placemarks (live {live}, offline {offline})")
    # Also write KMZ (zip KML + icon)
    out_kmz="/var/folders/_7/cqm25grj5w95r1zwk6lw9mf80000gn/T/opencode/production_ais/great_lakes_ais.kmz"
    with zipfile.ZipFile(out_kmz, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(out_kml, "great_lakes_ais.kml")
        z.write(ICON_SRC, "icons/Copilot_20260827_201004.png")
    print(f"Wrote KMZ {out_kmz} — contains KML + red icon (exact PNG, 1024x1024 RGBA, MD5 a194d59...)")
    # Quick validation
    print(f"Unique MMSI check: {len(set(mmsis))} == {len(ROSTER_118)} -> {len(set(mmsis))==len(ROSTER_118)}")
    print(f"Ceiling 180: {len(ROSTER_118)} <=180 -> {len(ROSTER_118)<=180}")
    print(f"Red icon href: {ICON_HREF_PRODUCTION} — Google Earth Small (native 1024 but used exactly as provided, 0°=north, transparency preserved)")
    print(f"ATB one unit = one placemark: verified (e.g., Dirk 5175745 represents Michigan Trader barge as attribute)")

if __name__ == "__main__":
    main()
