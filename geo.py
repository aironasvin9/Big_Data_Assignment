# geo.py
"""
Geographical utilities for AIS data analysis.
Haversine distance calculations and coordinate validation.
"""

from math import radians, sin, cos, sqrt, atan2
import datetime
from config import EPOCH_2000


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Return the great-circle distance in kilometres between two points
    using the Haversine formula.
    """
    R = 6371.0  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def is_valid_coordinate(lat: str, lon: str) -> bool:
    """Validate latitude and longitude values."""
    try:
        lat_f = float(lat)
        lon_f = float(lon)
        
        if not (-90 <= lat_f <= 90):
            return False
        if not (-180 <= lon_f <= 180):
            return False
        if lat_f == 0.0 and lon_f == 0.0:
            return False
        
        return True
    except (ValueError, TypeError):
        return False


def ts_to_epoch(ts_str: str) -> int:
    """Parse AIS timestamp (DD/MM/YYYY HH:MM:SS) to integer seconds since 2000-01-01."""
    dt = datetime.datetime.strptime(ts_str.strip(), "%d/%m/%Y %H:%M:%S")
    return int((dt - EPOCH_2000).total_seconds())
