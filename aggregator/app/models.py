from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator

class Event(BaseModel):
    topic:     str
    event_id:  str
    timestamp: datetime
    source:    str
    payload:   Dict[str, Any] = Field(default_factory=dict)

class EventBatch(BaseModel):
    events: List[Event]

    @model_validator(mode="after")
    def check_not_empty(self):
        if not self.events:
            raise ValueError("batch tidak boleh kosong")
        return self

class PublishResponse(BaseModel):
    accepted:  int
    duplicated: int
    errors:    List[str] = []

class StatsResponse(BaseModel):
    received:          int
    unique_processed:  int
    duplicate_dropped: int
    topics:            int
    uptime_seconds:    float
    duplicate_rate:    float
