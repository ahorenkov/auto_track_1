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

def test_infer_pig_event_stopped_when_span_within_tol():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(hh=8, mm=0)
    # span = 0.020, 20m (<= 50 tol meters) => stopped
    recent = _samples_kp(
        base,
        kps=[10.000, 10.010, 10, 10.020],
        offset_sec=[-240, -120, 0],
    )
    event = infer_pig_event(
        recent_samples=recent,
        route_end_poi=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert event == "Stopped"

def test_infer_pig_event_moving_when_span_exceeds_tol():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(hh=8, mm=0)
    # span = 0.200, 200m (> 50 tol meters) => moving
    recent = _samples_kp(
        base,
        kps=[10.000, 10.100, 10.200],
        offset_sec=[-240, -120, 0],
    )
    event = infer_pig_event(
        recent_samples=recent,
        route_end_poi=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert event == "Moving"    

def test_infer_pig_event_completed_if_close_to_end_poi_overrides():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(hh=8, mm=0)

    recent = _samples_kp(
        base,
        kps=[9.700, 9.900, 10.000],
        offset_sec=[-240, -120, 0],
    )
    end_poi = _poi(tag="END", kp=10.000, legacy="L1")
    event = infer_pig_event(
        recent_samples=recent,
        route_end_poi=end_poi,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert event == "Completed"

# eta_from_to

def test_eta_from_to_none_if_speed_zero():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(hh=8, mm=0)
    cur = PosSample(dt=base, kp=10.0)
    target = _poi(tag="N1", kp=10.5)
    assert eta_from_to(cur, target, speed=0.0, gc_to_kp=gc_to_kp, cfg=cfg) is None

def test_eta_from_to_none_if_target_behind():
    cfg = _cfg()
    gc_to_kp = {}
    base = dt(hh=8, mm=0)
    cur = PosSample(dt=base, kp=10.5)
    target = _poi(tag="B1", kp=9.9)
    assert eta_from_to(cur, target, speed=1.0, gc_to_kp=gc_to_kp, cfg=cfg) is None

def test_eta_from_to_computes_forward_eta():
    cfg = _cfg()
    gc_to_kp = {}
    cur_dt = dt(hh=8, mm=0)
    cur = PosSample(dt=cur_dt, kp=10.0)
    target = _poi(tag="N1", kp=10.1) # 100 m ahead
    # speed 2 m/s => 50 s
    eta = eta_from_to(cur, target, speed=2.0, gc_to_kp=gc_to_kp, cfg=cfg)
    assert eta == cur_dt + timedelta(seconds=50)

# infer_notification_type (priority order tests + dedup + cadence)
def test_notif_type_run_completion_has_top_priority():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}
    cur = PosSample(dt=dt(8, 0), kp=10.0)

    notif = infer_notification_type(
        state=state,
        pig_event="Completed",
        cur=cur,
        legacy_route="L1",
        route=[_poi("V1", 9.0), _poi("END", 10.0)],
        next_poi=None,
        end_poi=_poi("END", 10.0),
        gaps=[],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif == "Run Completion"

def test_notif_type_poi_passage_before_gap_and_updates():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}
    cur = PosSample(dt=dt(8, 0), kp=10.0)

    route = [_poi("V1", 9.0), _poi("V2", 11.0)]
    notif = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=route[1],
        end_poi=route[-1],
        gaps=[GapPoint(legacy_route="L1", kind="start", kp=10.0)],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif == "POI Passage"