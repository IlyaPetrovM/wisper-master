from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pydantic import BaseModel


class TranscribeRequest(BaseModel):
    file_id: int
    url: str
    max_duration: int = 60
    model_size: str = "small"
    format: str = "json"
    min_mark_duration_ms: int = 60000


class LoadModelRequest(BaseModel):
    model_size: str


@dataclass
class TaskState:
    task_id: str
    file_id: str
    url: str
    max_duration: int = 60
    split_files: Dict[int, str] = field(default_factory=dict)
    split_metadata: List[Dict] = field(default_factory=list)
    transcriptions: Dict[int, str] = field(default_factory=dict)
    failed_parts: List[int] = field(default_factory=list)
    expected_parts: int = 0
    splitted_file_id: Optional[str] = None
