"""MySQL-backed runtime store facade built on existing db.py helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from .interfaces import DeviceRuntimeState, RuntimeStore, TagRuntimeState


class MySQLRuntimeStore(RuntimeStore):
    """Facade around the existing runtime tables.

    This class intentionally reuses current helper functions in db.py so the
    first migration step adds an abstraction layer without changing behavior.
    """

    def __init__(self, db_module):
        if db_module is None:
            raise ValueError("db_module is required")
        self._db = db_module

    def get_tag_state(self, tag_id: int) -> Optional[TagRuntimeState]:
        result = self._db.get_latest_tag_value(tag_id)
        if not result:
            return None

        value = None
        timestamp = None

        if isinstance(result, tuple):
            if len(result) >= 1:
                value = result[0]
            if len(result) >= 2:
                timestamp = result[1]
        elif isinstance(result, dict):
            value = result.get("value")
            timestamp = result.get("ts") or result.get("timestamp")
        else:
            value = result

        if value is not None:
            try:
                value = float(value)
            except Exception:
                value = None

        return TagRuntimeState(tag_id=tag_id, value=value, timestamp=timestamp)

    def get_tag_states(self, tag_ids: list[int]) -> Dict[int, TagRuntimeState]:
        if not tag_ids:
            return {}

        rows = self._db.get_latest_tag_values_batch(tag_ids)
        result: Dict[int, TagRuntimeState] = {}
        for tag_id, row in rows.items():
            value = None
            timestamp = None
            if isinstance(row, tuple):
                if len(row) >= 1:
                    value = row[0]
                if len(row) >= 2:
                    timestamp = row[1]
            elif isinstance(row, dict):
                value = row.get("value")
                timestamp = row.get("ts") or row.get("timestamp")
            else:
                value = row

            if value is not None:
                try:
                    value = float(value)
                except Exception:
                    value = None

            result[int(tag_id)] = TagRuntimeState(tag_id=int(tag_id), value=value, timestamp=timestamp)

        return result

    def upsert_tag_state(self, tag_id: int, value: float, timestamp: datetime) -> bool:
        try:
            self._db.update_tag_latest_value(tag_id, value, timestamp)
            return True
        except Exception:
            return False

    def get_device_state(self, device_id: int) -> Optional[DeviceRuntimeState]:
        row = self._db.get_device(device_id)
        if not row:
            return None

        return DeviceRuntimeState(
            device_id=int(row.get("id", device_id)),
            is_online=row.get("is_online"),
            updated_at=row.get("updated_at"),
            name=row.get("name"),
        )

    def upsert_device_state(self, device_id: int, is_online: bool, updated_at: Optional[datetime] = None) -> bool:
        payload = {"is_online": bool(is_online)}
        if updated_at is not None:
            payload["updated_at"] = updated_at

        try:
            affected = self._db.update_device_row(device_id, payload)
            return affected >= 0
        except Exception:
            return False