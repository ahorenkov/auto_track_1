from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from core.state import InMemoryStateStore
from core.models import PigState

@dataclass
class EngineConfig:
    """Configuration for the engine."""
pass

class Engine:
    def __init__(self, repo:object, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()
        self.state_store = InMemoryStateStore()

    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> dict:

        state: PigState = self.state_store.get(pig_id)

        # temporarily imitate a state change
        if state.first_notif_at is None:
            state.first_notif_at = now

        self.state_store.save(pig_id, state)

        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Now": now.isoformat(),
            "First Notification At": (
                state.first_notif_at.isoformat()
                if state.first_notif_at
                else None
            ),
        }
        
