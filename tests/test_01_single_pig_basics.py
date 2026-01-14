from datetime import datetime, timedelta, timezone
from core.engine import Engine
from core.repo import CsvRepo

def test_not_detected_when_no_telemetry(tmp_path):
    '''If no samples are provided, pig should not be detected.'''
    (tmp_path / "POI.csv").write_text("Valve Tag, Valve Type, Global Channel, mathed_kp, Legacy Route Name\n"
                                     "T001, Test Valve 001, 100, 0.100, Route_Test\n"
                                     "T002, Test Valve 002, 200, 0.200, Route_Test\n",
                                     encoding="utf-8")


    (tmp_path / "GCtoKP.csv").write_text("Global Channel, mathed_kp\n"
                                        "100, 0.100\n"
                                        "200, 0.200\n",
                                        encoding="utf-8")

    repo = CsvRepo(root_dir=tmp_path)
    engine = Engine(repo)

    now = datetime(2025, 1, 1, 12, 0, 0)
    payload = engine.process_pig(pig_id="PIG_001", tool_type="TestTool", now=now)

    assert payload["Pig Event"] == "Not Detected"
    assert payload["Notification Type"] == ""
    assert payload["Speed"] == "0.00"