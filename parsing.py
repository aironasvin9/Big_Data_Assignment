# parsing.py
"""
Data validation and parsing for AIS records.
"""

import csv
from typing import Generator, List, Tuple
from config import (
    COL_MMSI, COL_LATITUDE, COL_LONGITUDE, COL_TIMESTAMP, COL_SOG, COL_DRAUGHT,
    INVALID_MMSI_PATTERNS, INVALID_MMSI_PREFIXES, EXPECTED_MMSI_LENGTH
)
from geo import is_valid_coordinate, ts_to_epoch


def is_valid_mmsi(mmsi: str) -> bool:
    """Check whether MMSI code is valid according to standard rules."""
    mmsi = mmsi.strip()
    
    if not mmsi or not mmsi.isdigit():
        return False
    if len(mmsi) != EXPECTED_MMSI_LENGTH:
        return False
    if mmsi in INVALID_MMSI_PATTERNS:
        return False
    if mmsi.startswith(INVALID_MMSI_PREFIXES):
        return False
    if len(set(mmsi)) == 1:
        return False
    
    return True


def validate_row(row: List[str]) -> bool:
    """Validate a complete row of AIS data."""
    mmsi = row[COL_MMSI] if len(row) > COL_MMSI else ""
    if not is_valid_mmsi(mmsi):
        return False
    
    lat = row[COL_LATITUDE] if len(row) > COL_LATITUDE else ""
    lon = row[COL_LONGITUDE] if len(row) > COL_LONGITUDE else ""
    if not is_valid_coordinate(lat, lon):
        return False
    
    return True


def stream_csv_rows(filepath: str, skip_header: bool = True) -> Generator[List[str], None, None]:
    """Generator that streams CSV rows one at a time without loading entire file."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        
        if skip_header:
            try:
                header = next(reader)
                yield header
            except StopIteration:
                return
        
        for row in reader:
            yield row


def stream_valid_rows(filepath: str) -> Generator[Tuple, None, None]:
    """
    Generator that streams valid AIS records with extended data.
    Yields: (mmsi, ts_str, epoch, lat, lon, sog, draught)
    """
    row_generator = stream_csv_rows(filepath, skip_header=True)

    try:
        _header = next(row_generator)
    except StopIteration:
        return

    for row in row_generator:
        if not validate_row(row):
            continue
        
        mmsi = row[COL_MMSI].strip()
        try:
            ts_str = row[COL_TIMESTAMP]
            epoch = ts_to_epoch(ts_str)
            lat = float(row[COL_LATITUDE])
            lon = float(row[COL_LONGITUDE])
            
            sog = 0.0
            if len(row) > COL_SOG:
                try:
                    sog = float(row[COL_SOG])
                except (ValueError, TypeError):
                    sog = 0.0
            
            draught = 0.0
            if len(row) > COL_DRAUGHT:
                try:
                    draught = float(row[COL_DRAUGHT])
                except (ValueError, TypeError):
                    draught = 0.0
            
        except (ValueError, IndexError):
            continue
        
        yield (mmsi, ts_str, epoch, lat, lon, sog, draught)
