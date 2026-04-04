# config.py
"""
Configuration file for Shadow Fleet Detection System.
All thresholds, constants, and parameters in one place.
"""

import multiprocessing as mp
import datetime

# ============================================================================
# MMSI VALIDATION CONSTANTS
# ============================================================================

INVALID_MMSI_PATTERNS = {
    '000000000', '111111111', '222222222', '333333333', '444444444',
    '555555555', '666666666', '777777777', '123456789', '999999999',
    '012345678', '987654321', '000000001', '888888888',
}

INVALID_MMSI_PREFIXES = ('0000', '1111', '9999',)
EXPECTED_MMSI_LENGTH = 9

# ============================================================================
# CSV COLUMN INDICES
# ============================================================================

COL_TIMESTAMP = 0
COL_TYPE_OF_MOBILE = 1
COL_MMSI = 2
COL_LATITUDE = 3
COL_LONGITUDE = 4
COL_NAV_STATUS = 5
COL_ROT = 6
COL_SOG = 7
COL_COG = 8
COL_HEADING = 9
COL_IMO = 10
COL_CALLSIGN = 11
COL_NAME = 12
COL_SHIP_TYPE = 13
COL_CARGO = 14
COL_DRAUGHT = 15

# ============================================================================
# PROCESSING PARAMETERS
# ============================================================================

CHUNK_SIZE = 10000
MAX_MMSI_PER_CHUNK = 100
NUM_WORKERS = max(1, mp.cpu_count() - 2)
MEMORY_LIMIT_MB = 1000.0

# ============================================================================
# ANOMALY DETECTION THRESHOLDS
# ============================================================================

# Anomaly A: Going Dark
GOING_DARK_GAP_HOURS = 4.0
GOING_DARK_MOVEMENT_KM = 5.0

# Anomaly B: Loitering
LOITERING_PROXIMITY_KM = 0.5  # 500 meters
LOITERING_SOG_KNOTS = 1.0
LOITERING_DURATION_HOURS = 2.0

# Anomaly C: Draft Changes
DRAFT_CHANGE_GAP_HOURS = 2.0
DRAFT_CHANGE_PERCENT_THRESHOLD = 5.0

# Anomaly D: Teleportation
TELEPORTATION_SPEED_KNOTS = 60.0

# ============================================================================
# DFSI SCORING WEIGHTS
# ============================================================================

DFSI_GOING_DARK_WEIGHT = 2.0
DFSI_TELEPORT_WEIGHT = 10.0
DFSI_DRAFT_WEIGHT = 15.0
DFSI_LOITERING_WEIGHT = 10.0

TOP_N_VESSELS = 50
TOP_N_GOING_DARK = 5

# ============================================================================
# OUTPUT PATHS
# ============================================================================

PARTITION_DIR = "./partitioned"
ANALYSIS_DIR = "./analysis"
LOITERING_DIR = "./loitering"
OUTPUT_DIRS = [PARTITION_DIR, ANALYSIS_DIR, LOITERING_DIR]

# ============================================================================
# CSV OUTPUT FORMAT
# ============================================================================

CSV_DELIMITER = ','

# ============================================================================
# REFERENCE EPOCH (for fast timestamp conversion)
# ============================================================================

EPOCH_2000 = datetime.datetime(2000, 1, 1)
