from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class EngineConfig:
    """Configuration for the engine."""
pass

class Engine:
    def __init__(self, repo:object, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()

    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> dict:
        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Notification Type": "",
        }
        
