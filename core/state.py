from typing import Dict
from core.models import PigState

class InMemoryStateStore:
    '''In memory state storage
    Lives only for the lifetime of the Python process'''

    def __init__(self) -> None:
        self._states: Dict[str, PigState] = {}

    def get(self, pig_id: str) -> PigState:
        '''Get state for a PIG
        If not exists - create a new one'''
        if pig_id not in self._states:
            self._states[pig_id] = PigState()
        
        return self._states[pig_id]
    
    def upsert(self, pig_id: str, state: PigState) -> None:
        '''Persist state for a PIG'''
        self._states[pig_id] = state
