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
        kps=[10.000, 10.010, 10.020],
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
    cur = PosSample(dt=dt(hh=8, mm=0), kp=10.0)

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

    cur = PosSample(dt=dt(hh=8, mm=0), kp=10.0)
    route = [_poi("V1", 10.0), _poi("V2", 11.0)]

    notif = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=route[1],
        end_poi=route[-1],
        gaps=[GapPoint(legacy_route="L1", kind="start", kp=99.0)],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif == "POI Passage"

def test_notif_type_gap_start_end():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}
    cur = PosSample(dt=dt(hh=8, mm=0), kp=10.0)

    notif = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=[_poi("V1", 9.0), _poi("V2", 11.0)],
        next_poi=_poi("V2", 11.0),
        end_poi=_poi("V2", 11.0),
        gaps=[GapPoint(legacy_route="L1", kind="start", kp=10.0)],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif == "Gap Start"

def test_notif_type_pre15_fires_ones_per_next_tag():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}

    cur_dt = dt(hh=8, mm=0)
    cur = PosSample(dt=cur_dt, kp=10.0)
    
    next_poi = _poi("NEXT", kp=10.5)
    eta_next = cur_dt + timedelta(minutes=15) # t15 = now

    route = [_poi("FAR1", kp=1.0), next_poi]
    end_poi = _poi("END_FAR", kp=99.0)

    # 1st time => fires
    notif1 = infer_notification_type(
        state = state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=next_poi,
        end_poi=end_poi,
        gaps=[],
        eta_next=eta_next,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif1 == "15 Min Upstream - Station"

    notif2 = infer_notification_type(
        state = state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=next_poi,
        end_poi=end_poi,
        gaps=[],
        eta_next=eta_next,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )   
    assert notif2 == "30 Min Update"

def test_notif_type_pre30_fires_within_window_and_dedups():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}

    # get into +/- 60 sec windown around t30
    cur_dt = dt(hh=8, mm=0, ss=30)
    cur = PosSample(dt=cur_dt, kp=10.0)

    next_poi = _poi("NEXT", kp=10.5)

    # eta next - 30 min - t30. t30  = now
    eta_next = cur_dt + timedelta(minutes=30)

    route = [_poi("FAR1", kp=1.0), next_poi]
    end_poi = _poi("END_FAR", kp=99.0)

    notif1 = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=next_poi,
        end_poi=end_poi,
        gaps=[],
        eta_next=eta_next,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif1 == "30 Min Upstream - Station"

    # 2nd time => suppressed
    notif2 = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=route,
        next_poi=next_poi,
        end_poi=end_poi,
        gaps=[],
        eta_next=eta_next,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )   
    assert notif2 == "30 Min Update"

def test_notif_type_30min_update_first_time_sets_state():
    cfg = _cfg()
    state = PigState()
    gc_to_kp = {}
    cur_dt = dt(hh=8, mm=0)
    cur = PosSample(dt=cur_dt, kp=10.0)

    notif = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur,
        legacy_route="L1",
        route=[_poi("V1", 9.0), _poi("V2", 11.0)],
        next_poi=_poi("V2", 11.0),
        end_poi=_poi("V2", 11.0),
        gaps=[],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif == "30 Min Update"
    assert state.first_notif_at == cur_dt
    assert state.last_notif_at == cur_dt

def test_notif_type_30_min_update_after_30_minutes():
    cfg = _cfg()
    gc_to_kp = {}

    base = dt(hh=8, mm=0)
    state = PigState(first_notif_at=base, last_notif_at=base)
    
    # after 29 minutes => empty

    cur1 = PosSample(dt=base + timedelta(minutes=29), kp=10.0)
    notif1 = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur1,
        legacy_route="L1",
        route=[_poi("V1", 9.0), _poi("V2", 11.0)],
        next_poi=_poi("V2", 11.0),
        end_poi=_poi("V2", 11.0),
        gaps=[],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif1 == ""
    # after 30 minutes => fires
    cur2 = PosSample(dt=base + timedelta(minutes=30), kp=10.0)
    notif2 = infer_notification_type(
        state=state,
        pig_event="Moving",
        cur=cur2,
        legacy_route="L1",
        route=[_poi("V1", 9.0), _poi("V2", 11.0)],
        next_poi=_poi("V2", 11.0),
        end_poi=_poi("V2", 11.0),
        gaps=[],
        eta_next=None,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
    )
    assert notif2 == "30 Min Update"
    assert state.last_notif_at == cur2.dt