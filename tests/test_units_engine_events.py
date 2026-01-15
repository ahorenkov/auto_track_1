from datetime import datetime, timedelta, timezone
from core.engine import (
    EngineConfig,
    infer_pig_event,
    eta_from_to,
    infer_notification_type,
)

from core.models import PosSample, POI, GapPoint, PigState

from tests.conftest import dt

def _samples_kp(base_dt, kps, offset_sec):
    """make samples from kp"""
    assert len(kps) == len(offset_sec)
    return [PosSample(dt=base_dt + timedelta(seconds=off), kp=kp) for kp, off in zip(kps, offset_sec)]

def _poi(tag="V1", kp=10.0, gc=None, legacy="L1", valve_type="MAIN"):
    return POI(tag=tag, valve_type=valve_type, global_channel=gc, kp=kp, legacy_route=legacy)

def _cfg():
    return EngineConfig(
        meters_per_channel=25,
        poi_tol_meters=50,
        stopped_window_sec=300,
        prepoi_time_window_sec=60,
        eps_kp=1e-3,
        speed_window_sec=1500,
        speed_short_window_sec=300,
        moving_boost_sec=600,
        min_speed_dt_sec=120,
        speed_search_sec=2100,
    )

# ---------- infer pig event tests ----------

def test_infer_pig_event_no_samples_selected():
    cfg = _cfg()
    gc_to_kp = {}
    event = infer_pig_event(
        recent_samples=[],
        route_end_poi=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert event == "Not Detected"

def test_infer_pig_event_not_enough_valid_positions_not_detected():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(8, 0, 0)
    recent = [PosSample(dt=base, kp=10.0)]
    event = infer_pig_event(
        recent_samples=recent,
        route_end_poi=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert event == "Not Detected"


