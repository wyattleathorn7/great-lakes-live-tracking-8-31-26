import sys
import unittest
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
import openwaters_fetch as ais


class PositionFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.now = 2_000_000_000
        self.vessel = ais.VesselState(mmsi="123456789", roster_entry={})

    def stamp(self, age_seconds):
        return datetime.fromtimestamp(
            self.now - age_seconds, timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    def apply(self, **kwargs):
        defaults = dict(
            lat=43.0, lon=-82.0, seen=self.stamp(60),
            msg_type="PositionReport", sog=8.0, nav_status=0,
            now=self.now,
        )
        defaults.update(kwargs)
        return ais.apply_position_report(self.vessel, **defaults)

    def test_static_stream_message_does_not_refresh_position(self):
        self.assertFalse(self.apply(msg_type="StaticDataReport"))
        self.assertIsNone(self.vessel.lat)
        self.assertIsNone(self.vessel.position_seen)

    def test_snapshot_can_supply_cached_position_after_source_age_filter(self):
        self.assertTrue(self.apply(
            msg_type="StaticDataReport",
            seen=self.stamp(2 * 24 * 3600),
            sog=0.0, allow_snapshot_position=True,
        ))
        self.assertTrue(ais.is_live_state(self.vessel, now=self.now))

    def test_moving_position_expires_after_30_minutes(self):
        self.apply(seen=self.stamp(31 * 60), sog=8.0, nav_status=0)
        self.assertFalse(ais.is_live_state(self.vessel, now=self.now))

    def test_anchored_vessel_remains_live_for_up_to_seven_days(self):
        self.apply(seen=self.stamp(6 * 24 * 3600), sog=0.0, nav_status=1)
        self.assertTrue(ais.is_live_state(self.vessel, now=self.now))
        self.vessel.position_seen = self.stamp(8 * 24 * 3600)
        self.assertFalse(ais.is_live_state(self.vessel, now=self.now))

    def test_latest_report_clears_stale_stationary_speed_and_status(self):
        self.apply(sog=0.0, nav_status=5)
        self.apply(seen=self.stamp(31 * 60), sog=8.0, nav_status=0)
        self.assertFalse(ais.is_live_state(self.vessel, now=self.now))

    def test_positions_outside_great_lakes_bbox_are_rejected(self):
        self.assertFalse(self.apply(lat=33.7, lon=-118.2))
        self.assertIsNone(self.vessel.lat)


    def test_source_coverage_keeps_overlap_and_upstream_attribution(self):
        import json
        import tempfile
        from pathlib import Path

        old_path = ais.SOURCE_COVERAGE_PATH
        old_openwaters = set(ais.openwaters_mmsis)
        old_upstream = {k: set(v) for k, v in ais.openwaters_upstream_mmsis.items()}
        old_stations = {k: set(v) for k, v in ais.openwaters_station_mmsis.items()}
        old_aisstream = set(ais.aisstream_mmsis)
        try:
            ais.openwaters_mmsis.clear()
            ais.openwaters_mmsis.update({"123456789", "316009090"})
            ais.openwaters_upstream_mmsis.clear()
            ais.openwaters_upstream_mmsis["aishub"].update({"123456789"})
            ais.openwaters_upstream_mmsis["digitraffic"].update({"316009090"})
            ais.openwaters_station_mmsis.clear()
            ais.openwaters_station_mmsis["aishub|RX-01"].add("123456789")
            ais.aisstream_mmsis.clear()
            ais.aisstream_mmsis.update({"123456789", "366904310"})
            with tempfile.TemporaryDirectory() as tmp:
                ais.SOURCE_COVERAGE_PATH = Path(tmp) / "coverage.json"
                report = ais.write_source_coverage_report(180, 2)
                saved = json.loads(ais.SOURCE_COVERAGE_PATH.read_text())
                self.assertEqual(report["overlap_mmsi"], ["123456789"])
                self.assertEqual(saved["sources"]["Open Waters"]["unique_roster_vessels"], 2)
                self.assertEqual(saved["sources"]["AISStream"]["unique_roster_vessels"], 2)
                self.assertIn("123456789", saved["sources"]["Open Waters"]["mmsi"])
                self.assertIn("123456789", saved["sources"]["AISStream"]["mmsi"])
                self.assertEqual(
                    saved["sources"]["Open Waters"]["upstream_source_mmsi"]["aishub"],
                    ["123456789"],
                )
                self.assertEqual(saved["open_waters_only_mmsi"], ["316009090"])
                self.assertEqual(saved["aisstream_only_mmsi"], ["366904310"])
        finally:
            ais.SOURCE_COVERAGE_PATH = old_path
            ais.openwaters_mmsis.clear()
            ais.openwaters_mmsis.update(old_openwaters)
            ais.openwaters_upstream_mmsis.clear()
            for key, values in old_upstream.items():
                ais.openwaters_upstream_mmsis[key].update(values)
            ais.openwaters_station_mmsis.clear()
            for key, values in old_stations.items():
                ais.openwaters_station_mmsis[key].update(values)
            ais.aisstream_mmsis.clear()
            ais.aisstream_mmsis.update(old_aisstream)


if __name__ == "__main__":
    unittest.main()
