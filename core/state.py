from __future__ import annotations

from typing import Dict
from core.models import PigState


class InMemoryStateStore:
    """Temporary state store (development). Replace with Postgres later."""

    def __init__(self) -> None:
        self._by_pig: Dict[str, PigState] = {}

    def get(self, pig_id: str) -> PigState:
        if pig_id not in self._by_pig:
            self._by_pig[pig_id] = PigState()
        return self._by_pig[pig_id]

    def upsert(self, pig_id: str, state: PigState) -> None:
        self._by_pig[pig_id] = state
