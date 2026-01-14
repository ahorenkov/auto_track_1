from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.engine import _pos_m, _current_sample, pick_ref_sample_at_or_before, speed_mps_by_ref, EngineConfig
from core.models import PosSample
from tests.conftest import dt, import_engine_models

MST = timezone(timedelta(hours=-7), name="MST")
def dt(hh, mm, ss=0):
    return datetime(2026, 1, 14, hh, mm, ss, tzinfo=MST)

def test_pos_m_prefers_kp():
    cfg = EngineConfig(meters_per_channel=25)
    gc_to_kp = {100: 1.0}
    s = PosSample(dt=dt(8, 0), gc=100, kp=2.5)
    assert _pos_m(s, gc_to_kp, cfg.meters_per_channel) == 2500.0

def test_pos_m_uses_gc_to_kp_when_no_kp():
    cfg = EngineConfig(meters_per_channel=25)
    gc_to_kp = {100: 1.23}
    s = PosSample(dt=dt(8, 0), gc=100, kp=None)
    assert _pos_m(s, gc_to_kp, cfg.meters_per_channel) == 1230.0

def test_current_sample_picks_latest():
    s1 = PosSample(dt=dt(8, 0), gc=100)
    s2 = PosSample(dt=dt(8, 5), gc=101)
    assert _current_sample([s1, s2]) == s2

def test_pick_ref_sample_at_or_before_prefers_left_side():
    target = dt(8, 10)
    s_old = PosSample(dt=dt(8, 0), gc=100)
    s_near_left = PosSample(dt=dt(8, 9), gc=101)
    s_right = PosSample(dt=dt(8, 11), gc=102)
    ref = pick_ref_sample_at_or_before([s_old, s_near_left, s_right], target)
    assert ref == s_near_left

def test_speed_mps_by_ref_basic():
    cfg = EngineConfig(meters_per_channel=25)
    gc_to_kp = {100: 1.0, 101: 1.1}
    cur = PosSample(dt=dt(8, 10), gc=101)
    ref = PosSample(dt=dt(8, 0), gc=100)
    spd = speed_mps_by_ref(cur, ref, gc_to_kp, cfg)
    assert abs(spd - (100.0 / 600.0)) < 1e-9

    