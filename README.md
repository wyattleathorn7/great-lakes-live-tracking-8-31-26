# Great Lakes Live AIS — Public Delivery

## Producer (merged 2026-10-08)

This repo is now the single home of Great Lakes Live AIS: fetch scripts
(`ais/*_fetch.py`, `ais/update_ais.py`), the 118-vessel roster, and both
workflows live here alongside the public KML output. Required Actions
secrets: `OPENWATERS_TOKEN`, `AISSTREAM_API_KEY` (primary Open Waters
flow), `AISHUB_USERNAME` (disabled historical AISHub flow). The former
`great-lakes-live-tracking-8-31-26` producer repo was retired; all stable
Google Earth URLs in this repo are unchanged.
