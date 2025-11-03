from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class CompiledObj:
    name: str
    obj: Any
    file: str
    errors: Optional[List[Exception]]
    obj_type: str
    tjson: str
