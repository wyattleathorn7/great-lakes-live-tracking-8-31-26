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


if __name__ == "__main__":
    unittest.main()
