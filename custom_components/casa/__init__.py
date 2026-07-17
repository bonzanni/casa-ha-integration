"""Casa integration for Home Assistant."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import AuthenticationError, CasaApiClient, ConnectionState
from .catalog import CatalogValidationError, VoiceAgentCatalog, reconcile_catalog
from .const import (
    CONF_AGENT_NAME,
    CONF_HOST,
    CONF_IDLE_STABILITY_MS,
    CONF_PORT,
    CONF_ROLE,
    CONF_SATELLITE_ENTITY_OVERRIDES,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    CONF_WEBHOOK_SECRET,
    DEFAULT_IDLE_STABILITY_MS,
    DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    DEFAULT_SESSION_MODE,
    DEFAULT_TRANSPORT,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
)
from .delivery import BackgroundDeliveryManager, SatelliteDirectory
from .registration import SessionRegistrationListener

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.CONVERSATION]


class CasaAgentRuntime:
    """Lifecycle owner for one persisted Casa voice-agent child."""

    def __init__(
        self,
        *,
        parent_entry_id: str,
        subentry_id: str,
        role: str,
        name: str,
        session_mode: str,
        transport: str,
        idle_stability_ms: int,
        catalog_present: bool,
        client: CasaApiClient | None,
        manager: BackgroundDeliveryManager | None,
    ) -> None:
        self.parent_entry_id = parent_entry_id
        self.subentry_id = subentry_id
        self.role = role
        self.name = name
        self.session_mode = session_mode
        self.transport = transport
        self.idle_stability_ms = idle_stability_ms
        self.catalog_present = catalog_present
        self.client = client
        self.manager = manager
        self.connection_state = ConnectionState.DISCONNECTED
        self._availability_subscribers: set[Callable[[], None]] = set()
        self._registration_tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    @property
    def route_id(self) -> str:
        """Stable delivery route for this parent and role."""
        return f"{self.parent_entry_id}:{self.role}"

    @property
    def entity_unique_id(self) -> str:
        """Stable entity identity for this parent and role."""
        return f"{self.parent_entry_id}:{self.role}"

    @property
    def available(self) -> bool:
        """Whether this exact catalog child is currently usable."""
        return bool(
            self.catalog_present
            and (
                self.transport == TRANSPORT_SSE
                or self.connection_state is ConnectionState.CONNECTED
            )
        )

    def async_add_availability_listener(
        self,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        """Subscribe one entity callback to this child's state only."""
        self._availability_subscribers.add(listener)

        def unsubscribe() -> None:
            self._availability_subscribers.discard(listener)

        return unsubscribe

    def set_connection_state(self, state: ConnectionState) -> None:
        """Update this child's connection state and notify its subscribers."""
        if self.connection_state is state:
            return
        self.connection_state = state
        for listener in tuple(self._availability_subscribers):
            try:
                listener()
            except Exception:
                _LOGGER.warning(
                    "Casa availability callback failed role=%s reason=callback",
                    self.role,
                )

    def register_session(self, device_id: str) -> asyncio.Task[None] | None:
        """Register one listening device through this present WS child."""
        if (
            self._closed
            or not self.catalog_present
            or self.transport != TRANSPORT_WS
            or self.client is None
        ):
            return None
        task = asyncio.create_task(self.client.register_session(
            scope_id=device_id,
            transport=TRANSPORT_WS,
            agent_role=self.role,
        ))
        self._registration_tasks.add(task)
        task.add_done_callback(self._registration_done)
        return task

    def _registration_done(self, task: asyncio.Task[None]) -> None:
        self._registration_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            _LOGGER.warning(
                "Casa session registration failed role=%s reason=connection",
                self.role,
            )

    @property
    def registration_task_count_for_test(self) -> int:
        """Expose owned registration task count for lifecycle assertions."""
        return len(self._registration_tasks)

    async def async_close(self) -> None:
        """Cancel registration and close every owned child resource."""
        if self._closed:
            return
        self._closed = True
        tasks = [task for task in self._registration_tasks if not task.done()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._registration_tasks.clear()
        self._availability_subscribers.clear()

        first_error: BaseException | None = None
        for owner in (self.manager, self.client):
            if owner is None:
                continue
            try:
                await owner.close()
            except BaseException as err:
                if first_error is None:
                    first_error = err
        if first_error is not None:
            raise first_error


@dataclass
class CasaRuntimeData:
    directory: SatelliteDirectory
    listener: SessionRegistrationListener
    agents: dict[str, CasaAgentRuntime]
    catalog_healthy: bool


def _configured_overrides(entry: ConfigEntry) -> dict[str, str]:
    raw = entry.options.get(
        CONF_SATELLITE_ENTITY_OVERRIDES,
        DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    )
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(parsed, dict) or not all(
        isinstance(device_id, str) and isinstance(entity_id, str)
        for device_id, entity_id in parsed.items()
    ):
        return {}
    return parsed


def _persisted_agent_subentries(entry: ConfigEntry) -> list:
    """Return every retained catalog-owned child in stable insertion order."""
    return [
        subentry
        for subentry in entry.subentries.values()
        if subentry.subentry_type == SUBENTRY_TYPE_AGENT
    ]


async def _fetch_catalog(
    hass: HomeAssistant,
    entry: ConfigEntry,
    session,
) -> VoiceAgentCatalog:
    """Fetch the parent catalog and always release the temporary client."""
    client = CasaApiClient(
        session=session,
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        webhook_secret=entry.data[CONF_WEBHOOK_SECRET],
    )
    try:
        return await client.fetch_voice_agents()
    finally:
        try:
            await client.close()
        except Exception:
            _LOGGER.warning("Casa catalog client cleanup failed reason=close")


async def _async_cleanup_children(
    listener: SessionRegistrationListener | None,
    agents: dict[str, CasaAgentRuntime],
) -> None:
    """Detach once and close every child before raising the first failure."""
    first_error: BaseException | None = None
    if listener is not None:
        try:
            listener.detach()
        except BaseException as err:
            first_error = err
    for runtime in agents.values():
        try:
            await runtime.async_close()
        except BaseException as err:
            if first_error is None:
                first_error = err
    if first_error is not None:
        raise first_error


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    persisted = _persisted_agent_subentries(entry)
    session = async_get_clientsession(hass)
    try:
        catalog = await _fetch_catalog(hass, entry, session)
    except AuthenticationError as err:
        raise ConfigEntryAuthFailed("Invalid Casa webhook secret") from err
    except (
        aiohttp.ClientError,
        OSError,
        asyncio.TimeoutError,
        CatalogValidationError,
    ) as err:
        if not persisted:
            raise ConfigEntryNotReady("Casa voice-agent catalog unavailable") from err
        catalog = None

    catalog_healthy = catalog is not None
    if catalog is not None:
        reconcile_catalog(hass, entry, catalog)
        catalog_roles = {agent.role for agent in catalog.agents}
    else:
        catalog_roles = {
            subentry.unique_id
            for subentry in persisted
            if subentry.unique_id is not None
        }

    directory = SatelliteDirectory(overrides=_configured_overrides(entry))
    agents: dict[str, CasaAgentRuntime] = {}
    listener: SessionRegistrationListener | None = None
    parent_runtime: CasaRuntimeData | None = None
    runtime_active = False
    reauth_started = False

    def connection_state_changed(
        runtime: CasaAgentRuntime,
        state: ConnectionState,
    ) -> None:
        nonlocal reauth_started
        runtime.set_connection_state(state)
        if (
            state is ConnectionState.AUTH_FAILED
            and runtime_active
            and parent_runtime is not None
            and entry.runtime_data is parent_runtime
            and not reauth_started
        ):
            reauth_started = True
            entry.async_start_reauth(hass)

    try:
        for subentry in _persisted_agent_subentries(entry):
            role = subentry.unique_id or subentry.data[CONF_ROLE]
            present = role in catalog_roles
            runtime = CasaAgentRuntime(
                parent_entry_id=entry.entry_id,
                subentry_id=subentry.subentry_id,
                role=role,
                name=subentry.data.get(CONF_AGENT_NAME, subentry.title),
                session_mode=subentry.data.get(
                    CONF_SESSION_MODE,
                    DEFAULT_SESSION_MODE,
                ),
                transport=subentry.data.get(CONF_TRANSPORT, DEFAULT_TRANSPORT),
                idle_stability_ms=subentry.data.get(
                    CONF_IDLE_STABILITY_MS,
                    DEFAULT_IDLE_STABILITY_MS,
                ),
                catalog_present=present,
                client=None,
                manager=None,
            )
            agents[subentry.subentry_id] = runtime
            if not present:
                continue
            client = CasaApiClient(
                session=session,
                host=entry.data[CONF_HOST],
                port=entry.data[CONF_PORT],
                webhook_secret=entry.data[CONF_WEBHOOK_SECRET],
                state_callback=lambda state, child=runtime: connection_state_changed(
                    child,
                    state,
                ),
            )
            runtime.client = client
            runtime.manager = BackgroundDeliveryManager(
                hass,
                client,
                route_id=runtime.route_id,
                directory=directory,
                idle_stability_ms=runtime.idle_stability_ms,
            )

        for runtime in agents.values():
            if (
                not runtime.catalog_present
                or runtime.transport != TRANSPORT_WS
                or runtime.client is None
                or runtime.manager is None
            ):
                continue
            try:
                await runtime.client.start_background(
                    route_id=runtime.route_id,
                    agent_role=runtime.role,
                    job_handler=runtime.manager.handle_frame,
                )
            except AuthenticationError as err:
                raise ConfigEntryAuthFailed(
                    "Invalid Casa webhook secret",
                ) from err
            except (aiohttp.ClientError, OSError, asyncio.TimeoutError):
                _LOGGER.warning(
                    "Casa child startup deferred role=%s reason=connection",
                    runtime.role,
                )

        def register_listening_device(device_id: str) -> None:
            for runtime in agents.values():
                runtime.register_session(device_id)

        listener = SessionRegistrationListener(
            hass,
            directory=directory,
            on_listening=register_listening_device,
        )
        listener.attach()
        parent_runtime = CasaRuntimeData(
            directory=directory,
            listener=listener,
            agents=agents,
            catalog_healthy=catalog_healthy,
        )
        entry.runtime_data = parent_runtime
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        update_unsubscribe = entry.add_update_listener(_async_options_updated)
        try:
            entry.async_on_unload(update_unsubscribe)
        except BaseException:
            try:
                update_unsubscribe()
            except BaseException:
                pass
            raise
        runtime_active = True
    except BaseException:
        entry.runtime_data = None
        try:
            await _async_cleanup_children(listener, agents)
        except BaseException:
            pass
        raise

    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not ok:
        return False
    runtime = entry.runtime_data
    if runtime is not None:
        entry.runtime_data = None
        await _async_cleanup_children(runtime.listener, runtime.agents)
    return True
