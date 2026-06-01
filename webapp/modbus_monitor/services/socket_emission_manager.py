"""Compatibility Publisher — centralises all Socket.IO emissions.

This module is the single canonical place that knows:
  - which event names to use (never change them!)
  - how to build each payload
  - which Socket.IO rooms to route each event to

Current status (PR4): Publisher is wired in but only takes effect when
  USE_COMPATIBILITY_PUBLISHER=true in web_config.txt.
  Default is false — existing emit calls in app.py keep working.

Legacy Socket.IO event names (must NOT be renamed):
  modbus_update, tag_update, alarm_event, card_alarm_event, quad_alarm_event

Room routing policy (must stay the same):
  subdashboard_{subdash_id}   — per-subdashboard room
  dashboard_device_{device_id} — per-device room
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class CompatibilityPublisher:
    """Wraps a Flask-SocketIO instance and provides typed publish methods.

    Inject the SocketIO instance via `set_socketio()` after app creation:

        from webapp.modbus_monitor.services.socket_emission_manager import publisher
        publisher.set_socketio(socketio)
    """

    def __init__(self) -> None:
        self._socketio: Any = None

    def set_socketio(self, socketio_instance: Any) -> None:
        """Register the SocketIO instance. Must be called before any publish."""
        self._socketio = socketio_instance

    # ------------------------------------------------------------------
    # Tag / Modbus update events
    # ------------------------------------------------------------------

    def publish_modbus_update(self, data: dict, rooms: list[str] | None = None) -> None:
        """Emit `modbus_update` to the given rooms (or broadcast if none).

        Data contract (do NOT change field names):
            tags: list of {id, name, value, timestamp}
            device_id, device_name, worker_id, worker_type, ok
        """
        if not self._socketio:
            return
        if rooms:
            for room in rooms:
                try:
                    self._socketio.emit('modbus_update', data, room=room)
                except Exception as exc:
                    logger.warning("publish_modbus_update room=%s error: %s", room, exc)
        else:
            try:
                self._socketio.emit('modbus_update', data)
            except Exception as exc:
                logger.warning("publish_modbus_update broadcast error: %s", exc)

    def publish_tag_update(self, data: dict) -> None:
        """Emit `tag_update` as broadcast (all connected clients).

        Data contract (do NOT change field names):
            tag_id, value, timestamp, worker_id, worker_type, device_id, device_name
        """
        if not self._socketio:
            return
        try:
            self._socketio.emit('tag_update', data, broadcast=True)
        except Exception as exc:
            logger.warning("publish_tag_update error: %s", exc)

    # ------------------------------------------------------------------
    # Simple tag alarm events
    # ------------------------------------------------------------------

    def publish_alarm_event(self, data: dict) -> None:
        """Emit `alarm_event` (simple per-tag alarm, broadcast).

        Data contract (do NOT change field names):
            title, message, level, tag_id, tag_name, value,
            status (INCOMING/OUTGOING), created_at, alarm_event_id
        """
        if not self._socketio:
            return
        try:
            self._socketio.emit('alarm_event', data)
        except Exception as exc:
            logger.warning("publish_alarm_event error: %s", exc)

    # ------------------------------------------------------------------
    # Card alarm events (qtag6/qtag4/qtag3/qtag2/single3/pv_only/pv_dual)
    # ------------------------------------------------------------------

    def publish_card_alarm_event(self, data: dict) -> None:
        """Emit `card_alarm_event` (broadcast).

        Data contract comes from `strategy.build_event()` in alarm_strategies.py.
        Key fields: card_type, card_id, column, alarm_type, status,
                    pv_tag_id, pv_value, sv_value, threshold, operator, timestamp
        """
        if not self._socketio:
            return
        try:
            self._socketio.emit('card_alarm_event', data)
        except Exception as exc:
            logger.warning("publish_card_alarm_event error: %s", exc)

    # ------------------------------------------------------------------
    # Quad tag alarm events (legacy quad card)
    # ------------------------------------------------------------------

    def publish_quad_alarm_event(self, data: dict) -> None:
        """Emit `quad_alarm_event` (broadcast).

        Data contract comes from `strategy.build_event()` for quad cards.
        Key fields: quad_id, column, alarm_type, status,
                    pv_tag_id, pv_value, sv_value, threshold, operator, timestamp
        """
        if not self._socketio:
            return
        try:
            self._socketio.emit('quad_alarm_event', data)
        except Exception as exc:
            logger.warning("publish_quad_alarm_event error: %s", exc)

    # ------------------------------------------------------------------
    # Generic dispatch (used by alarm_worker when event name is resolved
    # from strategy.socket_event_name)
    # ------------------------------------------------------------------

    def publish_generic(self, event_name: str, data: dict) -> None:
        """Emit an arbitrary event. Event name must be one of the known constants.

        Allowed: modbus_update, tag_update, alarm_event,
                 card_alarm_event, quad_alarm_event
        """
        _ALLOWED = {
            'modbus_update', 'tag_update', 'alarm_event',
            'card_alarm_event', 'quad_alarm_event',
        }
        if event_name not in _ALLOWED:
            logger.error(
                "publish_generic: unknown event '%s' — refusing to emit. "
                "Allowed: %s", event_name, _ALLOWED
            )
            return
        if not self._socketio:
            return
        try:
            self._socketio.emit(event_name, data)
        except Exception as exc:
            logger.warning("publish_generic %s error: %s", event_name, exc)


# ---------------------------------------------------------------------------
# Module-level singleton — imported by app.py and alarm_worker.py
# ---------------------------------------------------------------------------
publisher = CompatibilityPublisher()


# ---------------------------------------------------------------------------
# Legacy stub kept for backward compatibility (nothing depended on it,
# but preserve the import surface so old import-time code doesn't break)
# ---------------------------------------------------------------------------
class EmissionManager:
    """Legacy stub — use `publisher` (CompatibilityPublisher) for new code."""

    def __init__(self) -> None:
        self.enabled = False

    def enable_emission(self) -> None:
        self.enabled = True
        logger.info("EmissionManager enabled")

    def disable_emission(self) -> None:
        self.enabled = False
        logger.info("EmissionManager disabled")


_emission_manager = EmissionManager()


def get_emission_manager() -> EmissionManager:
    """Return legacy stub instance for backward-compat imports."""
    return _emission_manager
