"""Constants for Casa integration."""

from __future__ import annotations

import json
from pathlib import Path

DOMAIN = "casa"

INTEGRATION_VERSION = json.loads(
    (Path(__file__).parent / "manifest.json").read_text()
)["version"]

CONF_HOST = "host"
CONF_PORT = "port"
CONF_WEBHOOK_SECRET = "webhook_secret"
CONF_ROLE = "role"
CONF_AGENT_NAME = "name"

CONF_IDLE_STABILITY_MS = "idle_stability_ms"
CONF_SATELLITE_ENTITY_OVERRIDES = "satellite_entity_overrides"
CONF_SESSION_MODE = "session_mode"
CONF_TRANSPORT = "transport"

SESSION_MODE_DEVICE = "device"
SESSION_MODE_USER = "user"
SESSION_MODE_CONVERSATION = "conversation"

TRANSPORT_WS = "ws"
TRANSPORT_SSE = "sse"

DEFAULT_PORT = 18065
DEFAULT_IDLE_STABILITY_MS = 750
DEFAULT_SATELLITE_ENTITY_OVERRIDES = "{}"
DEFAULT_SESSION_MODE = SESSION_MODE_DEVICE
DEFAULT_TRANSPORT = TRANSPORT_WS

TIMEOUT_CONNECT = 3
TIMEOUT_TOTAL = 30
TIMEOUT_HEALTH = 5
WS_RECONNECT_MIN = 1
WS_RECONNECT_MAX = 30
VOICE_ROUTE_PROTOCOL = 2
VOICE_ROUTE_CAPABILITIES = (
    "background_jobs", "satellite_announce", "voice_handoff",
)
VOICE_AGENT_CATALOG_SCHEMA_VERSION = 1
MAX_VOICE_AGENTS = 20
MAX_VOICE_AGENT_NAME_LENGTH = 128
SUBENTRY_TYPE_AGENT = "voice_agent"

SSE_PATH = "/api/converse"
WS_PATH = "/api/converse/ws"
HEALTH_PATH = "/healthz"
VOICE_AGENTS_PATH = "/api/voice/agents"

FALLBACK = "Sorry, I'm having trouble. Please try again."
SILENT_STREAM_FALLBACK = "I'm here — could you say that again?"
