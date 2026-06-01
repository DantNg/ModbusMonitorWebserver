"""Feature flags for incremental rollout of refactored components.

Flags are read from web_config.txt (JSON) at the project root.
Each flag defaults to a safe value matching the current deployed behavior
so that a missing or empty web_config.txt never breaks anything.

Usage (workers / services):
    from shared.feature_flags import flags

    if flags.USE_RUNTIME_STORE:
        ...new path...
    else:
        ...old path...
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class FeatureFlags:
    # --- Already rolled out (default ON) ---
    # Shared FormattingService used by Jinja filters and logger worker
    USE_FORMATTING_SERVICE: bool = True

    # Runtime Store facade in tcp/rtu/alarm/logger workers
    USE_RUNTIME_STORE: bool = True

    # --- Pending rollout (default OFF) ---
    # Compatibility Publisher as single emit point for all Socket.IO events
    USE_COMPATIBILITY_PUBLISHER: bool = False

    # Card Schema Registry + Presenter layer in subdashboard route
    USE_CARD_SCHEMA_PRESENTERS: bool = False

    # Generic canonical alarm rule adapters
    USE_GENERIC_ALARM_RULES: bool = False

    # Generic alarm engine (shadow mode first, then replace)
    USE_GENERIC_ALARM_ENGINE: bool = False


def _find_web_config() -> str | None:
    """Locate web_config.txt from several candidate directories."""
    candidates = []

    # 1. Environment override
    env = os.environ.get("WEB_CONFIG_PATH")
    if env:
        candidates.append(env)

    # 2. Directory of the frozen EXE (PyInstaller)
    try:
        import sys
        if getattr(sys, "frozen", False):
            candidates.append(os.path.join(os.path.dirname(sys.executable), "web_config.txt"))
    except Exception:
        pass

    # 3. Current working directory
    candidates.append(os.path.join(os.getcwd(), "web_config.txt"))

    # 4. Two levels up from this file (project root when running from source)
    _here = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.join(os.path.dirname(_here), "web_config.txt"))

    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def _load_flags() -> FeatureFlags:
    """Load feature flags from web_config.txt, fall back to safe defaults."""
    cfg_path = _find_web_config()
    if not cfg_path:
        return FeatureFlags()

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return FeatureFlags()

        loaded: dict = {}
        bool_keys = [
            "USE_FORMATTING_SERVICE",
            "USE_RUNTIME_STORE",
            "USE_COMPATIBILITY_PUBLISHER",
            "USE_CARD_SCHEMA_PRESENTERS",
            "USE_GENERIC_ALARM_RULES",
            "USE_GENERIC_ALARM_ENGINE",
        ]
        for key in bool_keys:
            if key in data:
                # Accept: true/false (JSON bool), 1/0 (int), "1"/"0" (string)
                raw = data[key]
                if isinstance(raw, bool):
                    loaded[key] = raw
                else:
                    loaded[key] = bool(int(raw))

        return FeatureFlags(**loaded)
    except Exception as exc:
        logger.warning("feature_flags: failed to read %s: %s — using defaults", cfg_path, exc)
        return FeatureFlags()


# Module-level singleton — imported once per process
flags: FeatureFlags = _load_flags()
