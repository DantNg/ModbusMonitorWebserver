"""Protocol-style runtime store interfaces and state models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Protocol


@dataclass(frozen=True)
class TagRuntimeState:
    """Canonical runtime view of a tag's latest value."""

    tag_id: int
    value: Optional[float]
    timestamp: Optional[datetime]


@dataclass(frozen=True)
class DeviceRuntimeState:
    """Canonical runtime view of a device connection state."""

    device_id: int
    is_online: Optional[bool]
    updated_at: Optional[datetime]
    name: Optional[str] = None


class RuntimeStore(Protocol):
    """Facade over the current runtime persistence layer."""

    def get_tag_state(self, tag_id: int) -> Optional[TagRuntimeState]:
        ...

    def get_tag_states(self, tag_ids: list[int]) -> Dict[int, TagRuntimeState]:
        ...

    def upsert_tag_state(self, tag_id: int, value: float, timestamp: datetime) -> bool:
        ...

    def get_device_state(self, device_id: int) -> Optional[DeviceRuntimeState]:
        ...

    def upsert_device_state(self, device_id: int, is_online: bool, updated_at: Optional[datetime] = None) -> bool:
        ...