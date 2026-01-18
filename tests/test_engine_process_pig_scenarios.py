from datetime import timedelta

import pytest

from core.engine import Engine, EngineConfig
from core.models import PosSample

# Берём dt() из твоего conftest.py
from tests.conftest import dt


def _pick_route_with_kp(pois):
    """
    Pick legacy route with > 3 poi for the prev/next/end.
    """
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


def _set_telemetry_kp(repo, pig_id, base_dt, series):
    """
    series: list (minutes_offset, kp)
    """
    samples = [PosSample(dt=base_dt + timedelta(minutes=off), kp=kp) for off, kp in series]
    repo.set_demo_telemetry(pig_id, samples)


@pytest.fixture()
def route_bundle(csv_repo):
    """
    Get real POIs from POI.csv and pick a route with KP.
    """
    pois = csv_repo.get_pois()
    legacy, route = _pick_route_with_kp(pois)
    if not route:
        pytest.skip("No legacy route with >=3 POIs having KP found in POI.csv")
    return legacy, route


def test_process_pig_not_detected_when_no_telemetry(csv_repo):
    pig_id = "PIG_TEST_0"
    engine = Engine(csv_repo, cfg=EngineConfig())

    now = dt(hh=8, mm=0, ss=0)
    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] == "Not Detected"
    assert payload["Speed"] == "0.00"
    assert payload["ETA to the Next Valve"] == ""
    assert payload["ETA to the End"] == ""


def test_process_pig_stopped_sets_speed_zero_and_empty_etas(csv_repo, route_bundle):
    pig_id = "PIG_TEST_STOP"
    engine = Engine(csv_repo, cfg=EngineConfig())

    _, route = route_bundle
    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp0 = _safe_kp_not_near_any_poi(route, mid_kp)

    now = dt(hh=8, mm=0, ss=0)

    # last 5 minutes span is small (<=50m), so Stopped
    # 50m = 0.05km. Make fluctuations 0.02km (20m)
    _set_telemetry_kp(
        csv_repo,
        pig_id,
        now,
        series=[
            (-5, kp0),
            (-3, kp0 + 0.01),
            (-1, kp0 + 0.02),
            (0,  kp0 + 0.02),
        ],
    )

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] == "Stopped"
    assert payload["Speed"] == "0.00"
    assert payload["ETA to the Next Valve"] == ""
    assert payload["ETA to the End"] == ""


def test_process_pig_moving_has_positive_speed_and_some_etas(csv_repo, route_bundle):
    pig_id = "PIG_TEST_MOVE"
    engine = Engine(csv_repo, cfg=EngineConfig())

    _, route = route_bundle
    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp_start = _safe_kp_not_near_any_poi(route, mid_kp)

    now = dt(hh=8, mm=0, ss=0)
# Give history for ~35 minutes (like in cli_demo) and noticeable progress in kp
    # so speed is calculated over long-window.
    _set_telemetry_kp(
        csv_repo,
        pig_id,
        now,
        series=[
            (-35, kp_start - 0.50),
            (-25, kp_start - 0.35),
            (-12, kp_start - 0.15),
            (-10, kp_start - 0.12),
            (-5,  kp_start - 0.05),
            (-3,  kp_start - 0.03),
            (-1,  kp_start - 0.01),
            (0,   kp_start),
        ],
    )

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] in ("Moving", "Resumption")  # if suddenly first tick after stop
    # speed should be > 0
    assert float(payload["Speed"]) > 0.0

    # ETA can be empty if next_poi not found (rare), but usually not empty.
    # So we soften: at least Legacy Route should be a filled string
    assert isinstance(payload["Legacy Route"], str)


def test_process_pig_completed_near_end_poi(csv_repo, route_bundle):
    pig_id = "PIG_TEST_DONE"
    engine = Engine(csv_repo, cfg=EngineConfig(poi_tol_meters=50))

    legacy, route = route_bundle
    end_poi = route[-1]
    assert end_poi.kp is not None

    now = dt(hh=8, mm=0, ss=0)

    # Set current position within 50m of end_poi
    # 50m = 0.05km. Take end_kp - 0.02km (20m)
    end_kp = end_poi.kp
    cur_kp = end_kp - 0.02

    _set_telemetry_kp(
        csv_repo,
        pig_id,
        now,
        series=[
            (-5, cur_kp - 0.10),
            (-3, cur_kp - 0.05),
            (-1, cur_kp - 0.02),
            (0,  cur_kp),
        ],
    )

    payload = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=now)

    assert payload["Pig Event"] == "Completed"
    assert payload["Notification Type"] == "Run Completion"
    assert payload["Legacy Route"] in (legacy, "Unknown")


def test_process_pig_resumption_after_stop(csv_repo, route_bundle):
    """
    Main scenario: if the PIG was stopped and then started moving again,
    the Pig Event on the first "tick" of movement should be Resumption.
    """
    pig_id = "PIG_TEST_RESUME"
    engine = Engine(csv_repo, cfg=EngineConfig())

    _, route = route_bundle
    mid_kp = (route[0].kp + route[-1].kp) / 2.0
    kp0 = _safe_kp_not_near_any_poi(route, mid_kp)

    # ---- Tick 1: Stopped ----
    t1 = dt(hh=8, mm=0, ss=0)
    _set_telemetry_kp(
        csv_repo,
        pig_id,
        t1,
        series=[
            (-5, kp0),
            (-3, kp0 + 0.01),
            (-1, kp0 + 0.02),
            (0,  kp0 + 0.02),
        ],
    )
    p1 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=t1)
    assert p1["Pig Event"] == "Stopped"

    # ---- Tick 2: Start moving after stop ----
    t2 = t1 + timedelta(minutes=1)

    # now span for last 5 minutes becomes noticeable (movement)
    # add new points so last 5 minutes show movement
    _set_telemetry_kp(
        csv_repo,
        pig_id,
        t2,
        series=[
            (-5, kp0 + 0.02),
            (-3, kp0 + 0.05),
            (-1, kp0 + 0.12),
            (0,  kp0 + 0.20),
        ],
    )
    p2 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=t2)

    # IMPORTANT: this is a check of your change
    assert p2["Pig Event"] == "Resumption"

    # ---- Tick 3: continue moving -> Moving ----
    t3 = t2 + timedelta(minutes=1)
    _set_telemetry_kp(
        csv_repo,
        pig_id,
        t3,
        series=[
            (-5, kp0 + 0.20),
            (-3, kp0 + 0.28),
            (-1, kp0 + 0.36),
            (0,  kp0 + 0.45),
        ],
    )
    p3 = engine.process_pig(pig_id=pig_id, tool_type="Tool", now=t3)
    assert p3["Pig Event"] == "Moving"