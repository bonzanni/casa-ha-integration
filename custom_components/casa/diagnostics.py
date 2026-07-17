"""Strictly redacted diagnostics for the Casa integration."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return only non-sensitive parent and child health state."""
    runtime = entry.runtime_data
    return {
        "catalog_healthy": runtime.catalog_healthy,
        "agents": [
            {
                "subentry_id": child.subentry_id,
                "role": child.role,
                "catalog_present": child.catalog_present,
                "transport": child.transport,
                "connection_state": child.connection_state,
                "background_capable": (
                    child.client.background_capable if child.client else False
                ),
            }
            for child in runtime.agents.values()
        ],
    }
