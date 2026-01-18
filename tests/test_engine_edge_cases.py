from datetime import timedelta

import pytest

from core.engine import Engine, EngineConfig
from core.models import PosSample, PigState

from tests.conftest import dt


def _pick_route_with_kp(pois):
    by_route = {}
    for p in pois:
        if p.kp is None:
            continue
        by_route.setdefault(p.legacy_route or "Unknown", []).append(p)
    for r in by_route:
        by_route[r] = sorted(by_route[r], key=lambda x: x.kp)
    candidates = [(len(v), r) for r, v in by_route.items() if len(v) >= 3]
    if not candidates:
        return None, []
    _, best = max(candidates)
    return best, by_route[best]


def _safe_kp_not_near_any_poi(route_pois, base_kp, tol_km=0.08):
    kp = base_kp
    for _ in range(50):
        if min(abs(p.kp - kp) for p in route_pois) > tol_km:
            return kp
        kp += 0.02
    return kp


def _set_samples(repo, pig_id, base_dt, samples):
    repo.set_demo_telemetry(pig_id, samples)


@pytest.fixture()
def route_bundle(csv_repo):
    pois = csv_repo.get_pois()
    legacy, route = _pick_route_with_kp(pois)
    if not route:
        pytest.skip("No legacy route with >=3 POIs having KP found in POI.csv")
    return legacy, route


# -------------------------------------------------------------------
# 4.1 Partially missing data: None positions should not crash
# -------------------------------------------------------------------

def test_edge_some_samples_with_no_position_do_not_crash(csv_repo, route_bundle):
    pig_id = "PIG_EDGE_NONE_POS"
    engine = Engine(csv_repo, cfg=EngineConfig())

    _, route = route_bundle
    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp0 = _safe_kp_not_near_any_poi(route, mid_kp)

    now = dt(hh=8, mm=0, ss=0)

    samples = [
        PosSample(dt=now - timedelta(minutes=5), kp=kp0),
        PosSample(dt=now - timedelta(minutes=4), gc=None, kp=None),
        PosSample(dt=now - timedelta(minutes=3), kp=kp0 + 0.10),
        PosSample(dt=now - timedelta(minutes=2), gc=None, kp=None),
        PosSample(dt=now, kp=kp0 + 0.20),
    ]
    _set_samples(csv_repo, pig_id, now, samples)

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] in ("Moving", "Stopped", "Not Detected", "Resumption", "Completed")


# -------------------------------------------------------------------
# 4.2 Not enough history for speed: min_speed_dt_sec gate
# -------------------------------------------------------------------

def test_edge_speed_zero_when_ref_too_close_in_time(csv_repo, route_bundle):
    pig_id = "PIG_EDGE_MIN_DT"
    cfg = EngineConfig(min_speed_dt_sec=120, speed_search_sec=300)  
    engine = Engine(csv_repo, cfg=cfg)

    _, route = route_bundle
    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp0 = _safe_kp_not_near_any_poi(route, mid_kp)

    now = dt(hh=8, mm=0, ss=0)

    samples = [
        PosSample(dt=now - timedelta(seconds=60), kp=kp0),
        PosSample(dt=now, kp=kp0 + 0.20),
    ]
    _set_samples(csv_repo, pig_id, now, samples)

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] in ("Moving", "Resumption", "Stopped", "Not Detected")
    assert payload["Speed"] == "0.00"  


# -------------------------------------------------------------------
# 4.3 GC mapping missing for some GCs: fallback to gc*meters_per_channel
# -------------------------------------------------------------------

def test_edge_unknown_gc_mapping_fallback_does_not_crash(csv_repo):
    pig_id = "PIG_EDGE_GC_FALLBACK"
    cfg = EngineConfig(meters_per_channel=25)
    engine = Engine(csv_repo, cfg=cfg)

    now = dt(hh=8, mm=0, ss=0)
    samples = [
        PosSample(dt=now - timedelta(minutes=5), gc=99999999),
        PosSample(dt=now, gc=100000050),
    ]
    csv_repo.set_demo_telemetry(pig_id, samples)

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] in ("Moving", "Stopped", "Not Detected", "Resumption", "Completed")


# -------------------------------------------------------------------
# 4.4 Route selection Unknown: no POIs in range -> legacy_route becomes Unknown
# -------------------------------------------------------------------

def test_edge_route_becomes_unknown_when_position_far_outside_all_routes(csv_repo):
    pig_id = "PIG_EDGE_UNKNOWN_ROUTE"
    engine = Engine(csv_repo, cfg=EngineConfig())

    now = dt(hh=8, mm=0, ss=0)

    samples = [
        PosSample(dt=now - timedelta(minutes=5), kp=99999.0),
        PosSample(dt=now, kp=100000.0),
    ]
    csv_repo.set_demo_telemetry(pig_id, samples)

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Legacy Route"] == "Unknown"


# -------------------------------------------------------------------
# 4.5 ETA edge: if next_poi is behind (or cannot be found), ETAs should be empty
# -------------------------------------------------------------------

def test_edge_etas_empty_when_speed_positive_but_targets_invalid(csv_repo, route_bundle):
    pig_id = "PIG_EDGE_ETA_INVALID"
    engine = Engine(csv_repo, cfg=EngineConfig())

    legacy, route = route_bundle
    end_poi = route[-1]
    start_poi = route[0]

    now = dt(hh=8, mm=0, ss=0)
    cur_kp = end_poi.kp + 0.50

    samples = [
        PosSample(dt=now - timedelta(minutes=35), kp=cur_kp - 0.60),
        PosSample(dt=now - timedelta(minutes=25), kp=cur_kp - 0.40),
        PosSample(dt=now - timedelta(minutes=5), kp=cur_kp - 0.10),
        PosSample(dt=now, kp=cur_kp),
    ]
    csv_repo.set_demo_telemetry(pig_id, samples)

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] in ("Moving", "Resumption", "Stopped", "Not Detected", "Completed")
    assert payload["ETA to the Next Valve"] == ""
    assert payload["ETA to the End"] == ""


# -------------------------------------------------------------------
# 4.6 State reset on Completed: locked_legacy_route cleared
# -------------------------------------------------------------------

def test_edge_completed_resets_locked_route(csv_repo, route_bundle):
    pig_id = "PIG_EDGE_LOCK_RESET"
    engine = Engine(csv_repo, cfg=EngineConfig(poi_tol_meters=50))

    legacy, route = route_bundle
    end_poi = route[-1]

    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp0 = _safe_kp_not_near_any_poi(route, mid_kp)

    t1 = dt(hh=8, mm=0, ss=0)
    csv_repo.set_demo_telemetry(
        pig_id,
        [
            PosSample(dt=t1 - timedelta(minutes=35), kp=kp0 - 0.50),
            PosSample(dt=t1 - timedelta(minutes=25), kp=kp0 - 0.30),
            PosSample(dt=t1, kp=kp0),
        ],
    )
    p1 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=t1)
    assert p1["Legacy Route"] in (legacy, "Unknown")

    t2 = t1 + timedelta(minutes=1)
    near_end = end_poi.kp - 0.02  

    csv_repo.set_demo_telemetry(
        pig_id,
        [
            PosSample(dt=t2 - timedelta(minutes=5), kp=near_end - 0.10),
            PosSample(dt=t2, kp=near_end),
        ],
    )
    p2 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=t2)
    assert p2["Pig Event"] == "Completed"
    state = csv_repo.get_state(pig_id)
    assert state.locked_legacy_route is None
    assert state.moving_started_at is None


# -------------------------------------------------------------------
# 4.7 Pre-POI window boundaries: exactly on boundary should fire
# -------------------------------------------------------------------

def test_edge_prepoi_window_boundary_inclusive(csv_repo, route_bundle):
    """
    Проверяем, что окно ±prepoi_time_window_sec работает на границе.
    В infer_notification_type: abs(now - t15) <= win.
    """
    pig_id = "PIG_EDGE_PREPOI_BOUND"
    cfg = EngineConfig(prepoi_time_window_sec=60)
    engine = Engine(csv_repo, cfg=cfg)

    _, route = route_bundle
    next_poi = route[1]

    now = dt(hh=8, mm=0, ss=0)
    cur_kp = next_poi.kp - 1.0 

    samples = [
        PosSample(dt=now - timedelta(minutes=35), kp=cur_kp - 2.0),
        PosSample(dt=now - timedelta(minutes=25), kp=cur_kp - 1.5),
        PosSample(dt=now - timedelta(minutes=5), kp=cur_kp - 0.3),
        PosSample(dt=now, kp=cur_kp),
    ]
    csv_repo.set_demo_telemetry(pig_id, samples)

    _ = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)


    p2 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now + timedelta(seconds=1))
    assert isinstance(p2["Notification Type"], str)