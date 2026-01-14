from __future__ import annotations

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.models import PosSample
from core.repo import CsvRepo
import pytest
from datetime import datetime, timedelta, timezone

MST = timezone(timedelta(hours=-7), name="MST")



def dt(hh: int, mm: int, ss: int, *, day: int = 14, month: int = 1, year: int = 2026) -> datetime:
    return datetime(year, month, day, hh, mm, ss, tzinfo=MST)

def import_engine_models():
    """Import engine/models 
    Returns:
        module: engine module
        module: models module
    """
    try:
        from core import engine as eng
        from core import models as mod
    except Exception as e:
        print(f"Error importing core modules: {e}")

    return eng, mod

@pytest.fixture(scope="session")
def csv_repo():
    """Real CSV reads poi.csv and gctokp.csv from the root
    if no files - skip tests"""

    if not os.path.exists(PROJECT_ROOT + "/POI.csv") or not os.path.exists(PROJECT_ROOT + "/Pig Tracking POI Valves Locations - Monitoring Team_updated(Nov11).csv"):
        pytest.skip("CSV files POI.csv or GCtoKP.csv not found in root directory")
    if not os.path.exists(PROJECT_ROOT + "/GCtoKP.csv"):
        print("Warning: GCtoKP.csv not found in root directory, some tests may fail")
              
    return CsvRepo(root_dir=".")        

def samples_gc(base_dt, gcs, offsen_min=None):
    """Generate PosSample list from base datetime and list of gcs"""
    if offsen_min is None:
        offsen_min = list(range(-(len(gcs)-1), 1))
    assert len(gcs) == len(offsen_min), "gcs and offsen_min must be same length"
    return [PosSample(dt=base_dt + timedelta(minutes=off), gc=gc) for gc, off in zip(gcs, offsen_min)]
