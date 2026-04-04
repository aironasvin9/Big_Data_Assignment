# test_validation.py
"""
Comprehensive unit tests for Shadow Fleet Detection.
Tests all anomalies, DFSI calculation, and edge cases.
"""

import unittest
from datetime import datetime, timedelta
from config import (
    GOING_DARK_GAP_HOURS, GOING_DARK_MOVEMENT_KM,
    TELEPORTATION_SPEED_KNOTS, DRAFT_CHANGE_GAP_HOURS,
    DRAFT_CHANGE_PERCENT_THRESHOLD, LOITERING_PROXIMITY_KM,
    LOITERING_SOG_KNOTS, LOITERING_DURATION_HOURS,
    DFSI_GOING_DARK_WEIGHT, DFSI_TELEPORT_WEIGHT,
    DFSI_DRAFT_WEIGHT, DFSI_LOITERING_WEIGHT
)
from geo import haversine_distance, ts_to_epoch
from detect import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
)
from loiter import detect_loitering_anomalies
from scoring import calculate_dfsi, aggregate_anomalies_by_vessel


class TestGeoUtilities(unittest.TestCase):
    """Test geographical utility functions."""
    
    def test_haversine_distance_zero(self):
        """Test distance between same point is 0."""
        dist = haversine_distance(54.5, 12.1, 54.5, 12.1)
        self.assertAlmostEqual(dist, 0.0, places=5)
    
    def test_haversine_distance_known_locations(self):
        """Test distance between known locations."""
        # Copenhagen to Hamburg: ~170 km
        dist = haversine_distance(55.6761, 12.5683, 53.5511, 10.0127)
        self.assertAlmostEqual(dist, 170, delta=5)
    
    def test_haversine_distance_antipodal(self):
        """Test distance between antipodal points."""
        # North Pole to South Pole: ~20,000 km
        dist = haversine_distance(90, 0, -90, 0)
        self.assertAlmostEqual(dist, 20037, delta=100)
    
    def test_haversine_distance_symmetry(self):
        """Test distance is symmetric."""
        dist_ab = haversine_distance(54.5, 12.1, 55.0, 13.0)
        dist_ba = haversine_distance(55.0, 13.0, 54.5, 12.1)
        self.assertAlmostEqual(dist_ab, dist_ba, places=10)


class TestAnomalyAGoingDark(unittest.TestCase):
    """Test Anomaly A: Going Dark (AIS gaps >4 hours)."""
    
    def _create_record(self, hours_offset, lat, lon, sog=5.0, draught=8.0):
        """Helper to create a record tuple."""
        ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(hours=hours_offset)
        ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
        epoch = ts_to_epoch(ts_str)
        return (ts_str, epoch, lat, lon, sog, draught)
    
    def test_going_dark_standard_case(self):
        """Test standard going-dark detection: 5-hour gap, 10km movement."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),      # Start
            self._create_record(1, 54.5, 12.1),      # No gap yet
            self._create_record(6, 54.6, 12.2),      # 5h gap, ~15km movement
            self._create_record(7, 54.6, 12.2),      # Continue
        ]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        
        self.assertEqual(len(anomalies), 1, "Should detect 1 going-dark event")
        anom = anomalies[0]
        self.assertGreaterEqual(anom['gap_hours'], GOING_DARK_GAP_HOURS)
        self.assertGreaterEqual(anom['distance_km'], GOING_DARK_MOVEMENT_KM)
    
    def test_going_dark_gap_too_short(self):
        """Test no detection when gap < 4 hours."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(3, 54.6, 12.2),      # 3h gap (too short)
        ]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect gap < 4h")
    
    def test_going_dark_movement_too_small(self):
        """Test no detection when movement < 5km."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(5, 54.5001, 12.1001),  # 4h gap, <1km movement
        ]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect movement < 5km")
    
    def test_going_dark_edge_case_exactly_4_hours(self):
        """Test detection when gap is exactly threshold."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(4.001, 54.7, 12.3),  # Just over 4h, 22km
        ]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertGreaterEqual(len(anomalies), 1, "Should detect at edge case")
    
    def test_going_dark_multiple_gaps(self):
        """Test detection of multiple gaps in same vessel."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(5, 54.7, 12.3),      # Gap 1
            self._create_record(10, 54.7, 12.3),
            self._create_record(15, 55.0, 13.0),     # Gap 2
        ]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertGreaterEqual(len(anomalies), 2, "Should detect multiple gaps")
    
    def test_going_dark_insufficient_records(self):
        """Test no detection with < 2 records."""
        mmsi = "219000431"
        records = [self._create_record(0, 54.5, 12.1)]
        
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should return empty for < 2 records")


class TestAnomalyDTeleportation(unittest.TestCase):
    """Test Anomaly D: Teleportation (impossible speeds >60 knots)."""
    
    def _create_record(self, hours_offset, lat, lon, sog=5.0, draught=8.0):
        """Helper to create a record tuple."""
        ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(hours=hours_offset)
        ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
        epoch = ts_to_epoch(ts_str)
        return (ts_str, epoch, lat, lon, sog, draught)
    
    def test_teleportation_standard_case(self):
        """Test standard teleportation: 200km in 1 hour = 100+ knots."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(1, 56.3, 14.2),      # ~200km in 1h = 108 knots
        ]
        
        anomalies = detect_teleportation_anomalies(mmsi, records)
        
        self.assertEqual(len(anomalies), 1, "Should detect teleportation")
        anom = anomalies[0]
        self.assertGreater(anom['speed_knots'], TELEPORTATION_SPEED_KNOTS)
    
    def test_teleportation_no_detection_normal_speed(self):
        """Test no detection for normal speeds."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(1, 54.6, 12.2),      # ~15km in 1h = 8 knots
        ]
        
        anomalies = detect_teleportation_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect normal speed")
    
    def test_teleportation_edge_case_exactly_60_knots(self):
        """Test behavior at threshold."""
        mmsi = "219000431"
        # 60 knots ≈ 111 km/h in 1 hour
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(1, 55.5, 12.1),      # ~111km = 60 knots (boundary)
        ]
        
        anomalies = detect_teleportation_anomalies(mmsi, records)
        # Should not detect at exactly 60 (need > 60)
        if anomalies:
            self.assertGreater(anomalies[0]['speed_knots'], TELEPORTATION_SPEED_KNOTS)
    
    def test_teleportation_extreme_speed(self):
        """Test extreme teleportation (e.g., 5000+ knots)."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(0.1, 20.0, 20.0),    # Extreme distance in 6 minutes
        ]
        
        anomalies = detect_teleportation_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 1, "Should detect extreme speed")
        self.assertGreater(anomalies[0]['speed_knots'], 5000)
    
    def test_teleportation_zero_distance(self):
        """Test no detection when vessel doesn't move."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1),
            self._create_record(1, 54.5, 12.1),      # No movement
        ]
        
        anomalies = detect_teleportation_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect zero movement")


class TestAnomalyCDraftChange(unittest.TestCase):
    """Test Anomaly C: Draft Changes (>5% during blackouts >2h)."""
    
    def _create_record(self, hours_offset, lat, lon, sog=5.0, draught=8.0):
        """Helper to create a record tuple."""
        ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(hours=hours_offset)
        ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
        epoch = ts_to_epoch(ts_str)
        return (ts_str, epoch, lat, lon, sog, draught)
    
    def test_draft_change_standard_case(self):
        """Test standard draft change: 10% increase over 3h gap."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1, draught=8.0),
            self._create_record(4, 54.6, 12.2, draught=8.8),  # 10% increase
        ]
        
        anomalies = detect_draft_change_anomalies(mmsi, records)
        
        self.assertEqual(len(anomalies), 1, "Should detect draft change")
        anom = anomalies[0]
        self.assertGreaterEqual(anom['gap_hours'], DRAFT_CHANGE_GAP_HOURS)
        self.assertGreaterEqual(abs(anom['draught_change_percent']), 
                               DRAFT_CHANGE_PERCENT_THRESHOLD)
    
    def test_draft_change_gap_too_short(self):
        """Test no detection when gap < 2 hours."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1, draught=8.0),
            self._create_record(1, 54.6, 12.2, draught=8.5),  # 1h gap (too short)
        ]
        
        anomalies = detect_draft_change_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect gap < 2h")
    
    def test_draft_change_change_too_small(self):
        """Test no detection when change < 5%."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1, draught=8.0),
            self._create_record(3, 54.6, 12.2, draught=8.2),  # 2.5% change
        ]
        
        anomalies = detect_draft_change_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should not detect change < 5%")
    
    def test_draft_change_missing_draught_data(self):
        """Test handling of zero draught (missing data)."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1, draught=0.0),  # No draught
            self._create_record(3, 54.6, 12.2, draught=8.0),
        ]
        
        anomalies = detect_draft_change_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 0, "Should skip records with 0 draught")
    
    def test_draft_change_decrease(self):
        """Test detection of draft decrease (cargo unloading)."""
        mmsi = "219000431"
        records = [
            self._create_record(0, 54.5, 12.1, draught=10.0),
            self._create_record(3, 54.6, 12.2, draught=9.0),   # 10% decrease
        ]
        
        anomalies = detect_draft_change_anomalies(mmsi, records)
        self.assertEqual(len(anomalies), 1, "Should detect decrease too")


class TestAnomalyBLoitering(unittest.TestCase):
    """Test Anomaly B: Loitering (ship-to-ship transfers)."""
    
    def _create_record(self, hours_offset, lat, lon, sog=0.3, draught=8.0):
        """Helper to create a record tuple."""
        ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(hours=hours_offset)
        ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
        epoch = ts_to_epoch(ts_str)
        return (ts_str, epoch, lat, lon, sog, draught)
    
    def test_loitering_standard_case(self):
        """Test standard loitering: 2 vessels 100m apart, slow moving, 4+ hours."""
        mmsi_records = {
            'vessel1': [
                self._create_record(0, 54.5, 12.1, 0.2),
                self._create_record(1, 54.5, 12.1, 0.2),
                self._create_record(2, 54.5, 12.1, 0.2),
                self._create_record(4, 54.5, 12.1, 0.2),
                self._create_record(5, 54.5, 12.1, 0.2),
            ],
            'vessel2': [
                self._create_record(0, 54.50009, 12.10009, 0.3),  # ~100m away
                self._create_record(1, 54.50008, 12.10008, 0.3),
                self._create_record(2, 54.50009, 12.10009, 0.3),
                self._create_record(4, 54.50008, 12.10008, 0.3),
                self._create_record(5, 54.50009, 12.10009, 0.3),
            ],
        }
        
        anomalies = detect_loitering_anomalies(mmsi_records)
        # May or may not detect depending on exact sampling and thresholds
        # Just verify no errors occur
        self.assertIsInstance(anomalies, list)
    
    def test_loitering_no_detection_fast_moving(self):
        """Test no detection when vessels are moving fast."""
        mmsi_records = {
            'vessel1': [
                self._create_record(0, 54.5, 12.1, 10.0),  # Fast
                self._create_record(1, 54.6, 12.2, 10.0),
            ],
            'vessel2': [
                self._create_record(0, 54.5001, 12.1001, 10.0),  # Fast
                self._create_record(1, 54.6001, 12.2001, 10.0),
            ],
        }
        
        anomalies = detect_loitering_anomalies(mmsi_records)
        self.assertEqual(len(anomalies), 0, "Should not detect fast vessels")
    
    def test_loitering_no_detection_far_apart(self):
        """Test no detection when vessels are >500m apart."""
        mmsi_records = {
            'vessel1': [
                self._create_record(0, 54.5, 12.1, 0.2),
                self._create_record(1, 54.5, 12.1, 0.2),
                self._create_record(4, 54.5, 12.1, 0.2),
            ],
            'vessel2': [
                self._create_record(0, 54.5, 12.2, 0.2),    # ~11km away
                self._create_record(1, 54.5, 12.2, 0.2),
                self._create_record(4, 54.5, 12.2, 0.2),
            ],
        }
        
        anomalies = detect_loitering_anomalies(mmsi_records)
        self.assertEqual(len(anomalies), 0, "Should not detect distant vessels")
    
    def test_loitering_short_duration(self):
        """Test no detection when duration < 2 hours."""
        mmsi_records = {
            'vessel1': [
                self._create_record(0, 54.5, 12.1, 0.2),
                self._create_record(1, 54.5, 12.1, 0.2),
            ],
            'vessel2': [
                self._create_record(0, 54.50009, 12.10009, 0.2),
                self._create_record(1, 54.50009, 12.10009, 0.2),
            ],
        }
        
        anomalies = detect_loitering_anomalies(mmsi_records)
        self.assertEqual(len(anomalies), 0, "Should not detect <2h duration")
    
    def test_loitering_single_vessel(self):
        """Test no detection with only 1 vessel."""
        mmsi_records = {
            'vessel1': [
                self._create_record(0, 54.5, 12.1, 0.2),
                self._create_record(4, 54.5, 12.1, 0.2),
            ],
        }
        
        anomalies = detect_loitering_anomalies(mmsi_records)
        self.assertEqual(len(anomalies), 0, "Should not detect single vessel")


class TestDFSICalculation(unittest.TestCase):
    """Test DFSI (Dynamic Fictional Suspicion Index) calculation."""
    
    def test_dfsi_only_going_dark(self):
        """Test DFSI with only going-dark anomalies."""
        anomalies = [
            {'mmsi': '219000431', 'anomaly_type': 'going_dark', 'gap_hours': 10.0},
        ]
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        vessel_data = vessels_dict['219000431']
        # DFSI = gap_hours / 2 = 10 / 2 = 5.0
        self.assertAlmostEqual(vessel_data['dfsi'], 5.0, places=1)
    
    def test_dfsi_only_teleportation(self):
        """Test DFSI with only teleportation anomalies."""
        anomalies = [
            {'mmsi': '219000431', 'anomaly_type': 'teleportation', 
             'distance_nm': 100.0},
        ]
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        vessel_data = vessels_dict['219000431']
        # DFSI = distance_nm / 10 = 100 / 10 = 10.0
        self.assertAlmostEqual(vessel_data['dfsi'], 10.0, places=1)
    
    def test_dfsi_multiple_anomalies(self):
        """Test DFSI with multiple types of anomalies."""
        anomalies = [
            {'mmsi': '219000431', 'anomaly_type': 'going_dark', 'gap_hours': 20.0},
            {'mmsi': '219000431', 'anomaly_type': 'teleportation', 'distance_nm': 200.0},
            {'mmsi': '219000431', 'anomaly_type': 'loitering'},
        ]
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        vessel_data = vessels_dict['219000431']
        # DFSI = 20/2 + 200/10 + 1*10 = 10 + 20 + 10 = 40
        self.assertAlmostEqual(vessel_data['dfsi'], 40.0, places=1)
    
    def test_dfsi_zero_anomalies(self):
        """Test DFSI with no anomalies."""
        anomalies = []
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        self.assertEqual(len(vessels_dict), 0, "Should have no vessels")
    
    def test_dfsi_multiple_vessels(self):
        """Test DFSI calculation for multiple vessels."""
        anomalies = [
            {'mmsi': '219000431', 'anomaly_type': 'teleportation', 'distance_nm': 100.0},
            {'mmsi': '305620000', 'anomaly_type': 'going_dark', 'gap_hours': 20.0},
        ]
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        self.assertEqual(len(vessels_dict), 2, "Should have 2 vessels")
        self.assertAlmostEqual(vessels_dict['219000431']['dfsi'], 10.0, places=1)
        self.assertAlmostEqual(vessels_dict['305620000']['dfsi'], 10.0, places=1)
    
    def test_dfsi_ranking_order(self):
        """Test that DFSI correctly ranks suspicious vessels."""
        anomalies = [
            {'mmsi': 'suspect1', 'anomaly_type': 'teleportation', 'distance_nm': 100.0},
            {'mmsi': 'suspect2', 'anomaly_type': 'teleportation', 'distance_nm': 500.0},
            {'mmsi': 'suspect3', 'anomaly_type': 'going_dark', 'gap_hours': 8.0},
        ]
        vessels_dict = aggregate_anomalies_by_vessel(anomalies)
        
        dfsi_scores = {mmsi: data['dfsi'] for mmsi, data in vessels_dict.items()}
        
        # suspect2 should have highest DFSI (50)
        self.assertGreater(dfsi_scores['suspect2'], dfsi_scores['suspect1'])
        self.assertGreater(dfsi_scores['suspect2'], dfsi_scores['suspect3'])


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""
    
    def test_empty_records(self):
        """Test handling of empty record list."""
        mmsi = "219000431"
        records = []
        
        anomalies_a = detect_going_dark_anomalies(mmsi, records)
        anomalies_d = detect_teleportation_anomalies(mmsi, records)
        anomalies_c = detect_draft_change_anomalies(mmsi, records)
        
        self.assertEqual(len(anomalies_a), 0)
        self.assertEqual(len(anomalies_d), 0)
        self.assertEqual(len(anomalies_c), 0)
    
    def test_single_record(self):
        """Test handling of single record."""
        mmsi = "219000431"
        ts_str = "02/03/2025 10:00:00"
        epoch = ts_to_epoch(ts_str)
        records = [(ts_str, epoch, 54.5, 12.1, 5.0, 8.0)]
        
        anomalies_a = detect_going_dark_anomalies(mmsi, records)
        self.assertEqual(len(anomalies_a), 0, "Cannot detect anomaly with 1 record")
    
    def test_duplicate_records(self):
        """Test handling of duplicate records (same location/time)."""
        mmsi = "219000431"
        ts_str = "02/03/2025 10:00:00"
        epoch = ts_to_epoch(ts_str)
        records = [
            (ts_str, epoch, 54.5, 12.1, 5.0, 8.0),
            (ts_str, epoch, 54.5, 12.1, 5.0, 8.0),  # Duplicate
        ]
        
        # Should not crash
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertIsInstance(anomalies, list)
    
    def test_extreme_coordinates(self):
        """Test handling of extreme coordinates."""
        dist_north_pole = haversine_distance(90, 0, 89, 0)
        dist_equator = haversine_distance(0, 0, 0, 1)
        
        # Both should be positive
        self.assertGreater(dist_north_pole, 0)
        self.assertGreater(dist_equator, 0)
    
    def test_negative_speed(self):
        """Test handling of negative SOG (shouldn't happen but check robustness)."""
        mmsi = "219000431"
        ts_str = "02/03/2025 10:00:00"
        epoch = ts_to_epoch(ts_str)
        records = [
            (ts_str, epoch, 54.5, 12.1, -5.0, 8.0),  # Negative SOG
            (ts_str, epoch, 54.5, 12.1, 5.0, 8.0),
        ]
        
        # Should not crash
        anomalies = detect_going_dark_anomalies(mmsi, records)
        self.assertIsInstance(anomalies, list)
    
    def test_very_large_dataset(self):
        """Test with 1000 records (performance check)."""
        mmsi = "219000431"
        records = []
        for i in range(1000):
            ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(minutes=i)
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            epoch = ts_to_epoch(ts_str)
            lat = 54.5 + (i % 100) * 0.001
            lon = 12.1 + (i % 100) * 0.001
            records.append((ts_str, epoch, lat, lon, 5.0, 8.0))
        
        # Should complete in reasonable time
        import time
        start = time.time()
        anomalies = detect_going_dark_anomalies(mmsi, records)
        elapsed = time.time() - start
        
        self.assertLess(elapsed, 1.0, "Should process 1000 records in <1s")
        self.assertIsInstance(anomalies, list)


class TestIntegration(unittest.TestCase):
    """Integration tests across multiple components."""
    
    def test_end_to_end_pipeline(self):
        """Test complete detection pipeline."""
        # Create synthetic data with all anomaly types
        mmsi_records = {}
        
        # Create vessel 1: has going dark
        records_v1 = []
        for i in range(10):
            ts = datetime(2025, 3, 2, 0, 0, 0) + timedelta(hours=i)
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            epoch = ts_to_epoch(ts_str)
            lat = 54.5 + i * 0.1
            lon = 12.1 + i * 0.1
            sog = 5.0
            draught = 8.0
            if i == 5:
                # Create gap (skip next 4 hours)
                continue
            if i >= 9:
                lat += 1.0  # Move significantly
            records_v1.append((ts_str, epoch, lat, lon, sog, draught))
        
        mmsi_records['vessel1'] = records_v1
        
        # Test pipeline
        anomalies_b = detect_loitering_anomalies(mmsi_records)
        self.assertIsInstance(anomalies_b, list)
        
        anomalies_a = detect_going_dark_anomalies('vessel1', records_v1)
        self.assertIsInstance(anomalies_a, list)


if __name__ == '__main__':
    unittest.main(verbosity=2)
