"""Tests for Casa parent and per-agent runtime wiring."""

from __future__ import annotations

import asyncio
import logging
from types import MappingProxyType
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import aiohttp
import pytest

from homeassistant.config_entries import ConfigSubentry
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady

from custom_components.casa import (
    CasaAgentRuntime,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.casa.api import AuthenticationError, ConnectionState
from custom_components.casa.catalog import (
    CatalogValidationError,
    VoiceAgent,
    VoiceAgentCatalog,
)
from custom_components.casa.const import (
    CONF_AGENT_NAME,
    CONF_HOST,
    CONF_IDLE_STABILITY_MS,
    CONF_PORT,
    CONF_ROLE,
    CONF_SATELLITE_ENTITY_OVERRIDES,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    CONF_WEBHOOK_SECRET,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
)


def _catalog(*role_names: str) -> VoiceAgentCatalog:
    return VoiceAgentCatalog(
        schema_version=1,
        agents=tuple(
            VoiceAgent(role=role_names[index], name=role_names[index + 1])
            for index in range(0, len(role_names), 2)
        ),
    )


def _subentry(
    role: str,
    name: str,
    *,
    subentry_id: str | None = None,
    transport: str = TRANSPORT_WS,
    session_mode: str = "device",
    idle_stability_ms: int = 750,
) -> ConfigSubentry:
    kwargs = {}
    if subentry_id is not None:
        kwargs["subentry_id"] = subentry_id
    return ConfigSubentry(
        data=MappingProxyType({
            CONF_ROLE: role,
            CONF_AGENT_NAME: name,
            CONF_SESSION_MODE: session_mode,
            CONF_TRANSPORT: transport,
            CONF_IDLE_STABILITY_MS: idle_stability_ms,
        }),
        subentry_type=SUBENTRY_TYPE_AGENT,
        title=name,
        unique_id=role,
        **kwargs,
    )


def _entry(*children: ConfigSubentry):
    entry = MagicMock()
    entry.entry_id = "entry-1"
    entry.data = {
        CONF_HOST: "127.0.0.1",
        CONF_PORT: 18065,
        CONF_WEBHOOK_SECRET: "PRIVATE_PARENT_SECRET",
    }
    entry.options = {
        CONF_SATELLITE_ENTITY_OVERRIDES: (
            '{"dev-k":"assist_satellite.kitchen"}'
        ),
    }
    entry.subentries = {child.subentry_id: child for child in children}
    entry.runtime_data = None
    entry.async_on_unload = MagicMock()
    entry.async_start_reauth = MagicMock()
    entry.add_update_listener = MagicMock(return_value=lambda: None)
    return entry


def _hass(order: list[str] | None = None):
    hass = MagicMock()
    order = order if order is not None else []

    def add_subentry(entry, subentry):
        order.append("reconcile_add")
        entry.subentries = {**entry.subentries, subentry.subentry_id: subentry}

    def update_subentry(entry, subentry, **changes):
        order.append("reconcile_update")
        for key, value in changes.items():
            object.__setattr__(subentry, key, value)

    async def forward(*_args):
        order.append("forward")

    hass.config_entries.async_add_subentry = MagicMock(side_effect=add_subentry)
    hass.config_entries.async_update_subentry = MagicMock(
        side_effect=update_subentry,
    )
    hass.config_entries.async_forward_entry_setups = AsyncMock(
        side_effect=forward,
    )
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
    return hass


class _ClientHarness:
    def __init__(
        self,
        catalog: VoiceAgentCatalog | None = None,
        *,
        fetch_error: BaseException | None = None,
        start_effects: tuple[BaseException | None, ...] = (),
    ) -> None:
        self.temp = MagicMock()
        self.temp.fetch_voice_agents = AsyncMock(
            return_value=catalog,
            side_effect=fetch_error,
        )
        self.temp.close = AsyncMock()
        self.children: list[MagicMock] = []
        self.start_effects = start_effects

    def __call__(self, **kwargs):
        if "state_callback" not in kwargs:
            return self.temp
        index = len(self.children)
        client = MagicMock()
        client.state_callback = kwargs["state_callback"]
        effect = self.start_effects[index] if index < len(self.start_effects) else None
        client.start_background = AsyncMock(side_effect=effect)
        client.register_session = AsyncMock()
        client.close = AsyncMock()
        self.children.append(client)
        return client


class _ManagerHarness:
    def __init__(self) -> None:
        self.items: list[MagicMock] = []
        self.calls: list[dict] = []

    def __call__(
        self,
        hass,
        client,
        *,
        route_id,
        directory,
        idle_stability_ms,
    ):
        manager = MagicMock()
        manager.handle_frame = AsyncMock()
        manager.close = AsyncMock()
        self.items.append(manager)
        self.calls.append({
            "hass": hass,
            "client": client,
            "route_id": route_id,
            "directory": directory,
            "idle_stability_ms": idle_stability_ms,
        })
        return manager


def _agent_runtime(
    *,
    transport: str = TRANSPORT_WS,
    catalog_present: bool = True,
    client=None,
    manager=None,
) -> CasaAgentRuntime:
    if client is None and catalog_present:
        client = MagicMock()
        client.register_session = AsyncMock()
        client.close = AsyncMock()
    if manager is None and catalog_present:
        manager = MagicMock()
        manager.close = AsyncMock()
    return CasaAgentRuntime(
        parent_entry_id="entry-1",
        subentry_id="child-butler",
        role="butler",
        name="Tina",
        session_mode="device",
        transport=transport,
        idle_stability_ms=750,
        catalog_present=catalog_present,
        client=client,
        manager=manager,
    )


class TestAgentRuntime:
    def test_stable_ids_and_exact_availability_notifications(self):
        runtime = _agent_runtime()
        changed = MagicMock()
        unsubscribe = runtime.async_add_availability_listener(changed)

        assert runtime.route_id == "entry-1:butler"
        assert runtime.entity_unique_id == "entry-1:butler"
        assert runtime.available is False

        runtime.set_connection_state(ConnectionState.CONNECTED)
        assert runtime.available is True
        changed.assert_called_once_with()

        runtime.set_connection_state(ConnectionState.CONNECTED)
        changed.assert_called_once_with()

        runtime.set_connection_state(ConnectionState.DISCONNECTED)
        assert runtime.available is False
        assert changed.call_count == 2

        unsubscribe()
        runtime.set_connection_state(ConnectionState.CONNECTED)
        assert changed.call_count == 2

        assert _agent_runtime(transport=TRANSPORT_SSE).available is True
        assert _agent_runtime(
            transport=TRANSPORT_SSE,
            catalog_present=False,
            client=None,
            manager=None,
        ).available is False

    @pytest.mark.asyncio
    async def test_register_session_tracks_only_present_ws_client_task(self):
        runtime = _agent_runtime()

        task = runtime.register_session("dev-kitchen")
        assert task is not None
        assert runtime.registration_task_count_for_test == 1
        await task
        await asyncio.sleep(0)

        runtime.client.register_session.assert_awaited_once_with(
            scope_id="dev-kitchen",
            transport="ws",
            agent_role="butler",
        )
        assert runtime.registration_task_count_for_test == 0

        sse = _agent_runtime(transport=TRANSPORT_SSE)
        missing = _agent_runtime(
            catalog_present=False,
            client=None,
            manager=None,
        )
        assert sse.register_session("dev-kitchen") is None
        assert missing.register_session("dev-kitchen") is None
        sse.client.register_session.assert_not_awaited()

        await runtime.async_close()
        await sse.async_close()
        await missing.async_close()

    @pytest.mark.asyncio
    async def test_registration_failure_log_is_role_only_and_generic(self, caplog):
        exception_canary = "PRIVATE_REGISTRATION_EXCEPTION"
        device_canary = "PRIVATE_DEVICE_ID"
        client = MagicMock()
        client.register_session = AsyncMock(
            side_effect=RuntimeError(exception_canary),
        )
        client.close = AsyncMock()
        runtime = _agent_runtime(client=client)

        with caplog.at_level(logging.WARNING, logger="custom_components.casa"):
            task = runtime.register_session(device_canary)
            assert task is not None
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)

        assert "role=butler reason=connection" in caplog.text
        assert exception_canary not in caplog.text
        assert device_canary not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
        await runtime.async_close()

    @pytest.mark.asyncio
    async def test_close_cancels_registration_and_closes_all_owners(self):
        registration_started = asyncio.Event()
        registration_cancelled = asyncio.Event()
        manager_error = RuntimeError("manager close failed")
        order: list[str] = []

        async def register_session(**_kwargs):
            registration_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                registration_cancelled.set()
                raise

        async def close_manager():
            order.append("manager")
            raise manager_error

        async def close_client():
            order.append("client")

        client = MagicMock()
        client.register_session = AsyncMock(side_effect=register_session)
        client.close = AsyncMock(side_effect=close_client)
        manager = MagicMock()
        manager.close = AsyncMock(side_effect=close_manager)
        runtime = _agent_runtime(client=client, manager=manager)
        runtime.register_session("dev-kitchen")
        await asyncio.wait_for(registration_started.wait(), timeout=1)

        with pytest.raises(RuntimeError) as raised:
            await runtime.async_close()

        assert raised.value is manager_error
        assert registration_cancelled.is_set()
        assert runtime.registration_task_count_for_test == 0
        assert order == ["manager", "client"]


class TestParentSetup:
    @pytest.mark.asyncio
    async def test_two_roles_get_isolated_owners_and_one_shared_listener(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry(
            "concierge",
            "Gary",
            subentry_id="child-concierge",
            idle_stability_ms=1250,
        )
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            assert await async_setup_entry(hass, entry) is True

        runtime = entry.runtime_data
        tina = runtime.agents["child-butler"]
        gary = runtime.agents["child-concierge"]
        assert runtime.catalog_healthy is True
        assert tina.client is not gary.client
        assert tina.manager is not gary.manager
        assert tina.manager is managers.items[0]
        assert gary.manager is managers.items[1]
        assert {call["route_id"] for call in managers.calls} == {
            "entry-1:butler",
            "entry-1:concierge",
        }
        assert all(call["directory"] is runtime.directory for call in managers.calls)
        assert {call["client"] for call in managers.calls} == set(clients.children)

        listener_cls.assert_called_once_with(
            hass,
            directory=runtime.directory,
            on_listening=ANY,
        )
        assert runtime.listener is listener_cls.return_value
        listener_cls.return_value.attach.assert_called_once_with()
        for child in (tina, gary):
            child.client.start_background.assert_awaited_once_with(
                route_id=child.route_id,
                agent_role=child.role,
                job_handler=child.manager.handle_frame,
            )
        hass.config_entries.async_forward_entry_setups.assert_awaited_once()
        clients.temp.close.assert_awaited_once_with()

        on_listening = listener_cls.call_args.kwargs["on_listening"]
        on_listening("dev-kitchen")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        tina.client.register_session.assert_awaited_once_with(
            scope_id="dev-kitchen",
            transport="ws",
            agent_role="butler",
        )
        gary.client.register_session.assert_awaited_once_with(
            scope_id="dev-kitchen",
            transport="ws",
            agent_role="concierge",
        )
        assert tina.registration_task_count_for_test == 0
        assert gary.registration_task_count_for_test == 0

    @pytest.mark.asyncio
    async def test_missing_catalog_role_is_retained_without_transport(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        missing = _subentry("legacy", "Old", subentry_id="child-legacy")
        entry = _entry(butler, missing)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina"))
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        runtime = entry.runtime_data
        legacy = runtime.agents["child-legacy"]
        assert legacy.catalog_present is False
        assert legacy.client is None
        assert legacy.manager is None
        assert legacy.available is False
        assert len(clients.children) == 1
        assert len(managers.items) == 1

    @pytest.mark.asyncio
    async def test_present_sse_child_has_owners_without_background_socket(self):
        child = _subentry(
            "concierge",
            "Gary",
            subentry_id="child-concierge",
            transport=TRANSPORT_SSE,
        )
        entry = _entry(child)
        hass = _hass()
        clients = _ClientHarness(_catalog("concierge", "Gary"))
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        runtime = entry.runtime_data.agents["child-concierge"]
        assert runtime.client is clients.children[0]
        assert runtime.manager is managers.items[0]
        assert runtime.available is True
        runtime.client.start_background.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_valid_catalog_reconciles_add_and_rename_before_forward(self):
        order: list[str] = []
        butler = _subentry("butler", "Old Tina", subentry_id="child-butler")
        entry = _entry(butler)
        original_add_listener = entry.add_update_listener

        def add_update_listener(callback):
            order.append("update_listener")
            return original_add_listener(callback)

        entry.add_update_listener = MagicMock(side_effect=add_update_listener)
        hass = _hass(order)
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            listener_cls.return_value.attach.side_effect = lambda: order.append(
                "listener_attach",
            )
            assert await async_setup_entry(hass, entry) is True

        hass.config_entries.async_update_subentry.assert_called_once()
        hass.config_entries.async_add_subentry.assert_called_once()
        assert {child.role for child in entry.runtime_data.agents.values()} == {
            "butler",
            "concierge",
        }
        assert order.index("reconcile_update") < order.index("forward")
        assert order.index("reconcile_add") < order.index("forward")
        assert order.index("listener_attach") < order.index("forward")
        assert order[-1] == "update_listener"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fetch_error",
        [
            aiohttp.ClientError("offline"),
            OSError("socket failed"),
            CatalogValidationError("invalid"),
        ],
    )
    async def test_known_child_catalog_failure_loads_degraded(self, fetch_error):
        child = _subentry("butler", "Tina", subentry_id="child-butler")
        entry = _entry(child)
        hass = _hass()
        clients = _ClientHarness(fetch_error=fetch_error)
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        runtime = entry.runtime_data
        assert runtime.catalog_healthy is False
        assert runtime.agents["child-butler"].catalog_present is True
        assert runtime.agents["child-butler"].client is clients.children[0]
        hass.config_entries.async_add_subentry.assert_not_called()
        hass.config_entries.async_update_subentry.assert_not_called()
        clients.temp.close.assert_awaited_once_with()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fetch_error",
        [aiohttp.ClientError("offline"), OSError("socket failed")],
    )
    async def test_first_catalog_failure_without_child_raises_not_ready(
        self,
        fetch_error,
    ):
        entry = _entry()
        hass = _hass()
        clients = _ClientHarness(fetch_error=fetch_error)

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch("custom_components.casa.BackgroundDeliveryManager") as manager_cls, \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            with pytest.raises(ConfigEntryNotReady):
                await async_setup_entry(hass, entry)

        clients.temp.close.assert_awaited_once_with()
        manager_cls.assert_not_called()
        listener_cls.assert_not_called()
        hass.config_entries.async_forward_entry_setups.assert_not_awaited()
        assert entry.runtime_data is None

    @pytest.mark.asyncio
    async def test_catalog_auth_always_raises_auth_failed(self):
        child = _subentry("butler", "Tina", subentry_id="child-butler")
        entry = _entry(child)
        hass = _hass()
        clients = _ClientHarness(
            fetch_error=AuthenticationError("PRIVATE_AUTH_FAILURE"),
        )

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch("custom_components.casa.BackgroundDeliveryManager") as manager_cls, \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            with pytest.raises(ConfigEntryAuthFailed):
                await async_setup_entry(hass, entry)

        clients.temp.close.assert_awaited_once_with()
        manager_cls.assert_not_called()
        listener_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_one_child_network_start_failure_does_not_stop_sibling(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(
            _catalog("butler", "Tina", "concierge", "Gary"),
            start_effects=(ConnectionError("offline"), None),
        )
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        assert len(entry.runtime_data.agents) == 2
        assert all(client.start_background.await_count == 1 for client in clients.children)
        hass.config_entries.async_forward_entry_setups.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_child_network_failure_log_contains_no_connection_details(
        self,
        caplog,
    ):
        exception_canary = "PRIVATE_CHILD_CONNECTION_FAILURE"
        child = _subentry("butler", "Tina", subentry_id="child-butler")
        entry = _entry(child)
        hass = _hass()
        clients = _ClientHarness(
            _catalog("butler", "Tina"),
            start_effects=(ConnectionError(exception_canary),),
        )
        managers = _ManagerHarness()

        with caplog.at_level(logging.WARNING, logger="custom_components.casa"), \
             patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        assert "role=butler reason=connection" in caplog.text
        assert exception_canary not in caplog.text
        assert entry.data[CONF_WEBHOOK_SECRET] not in caplog.text
        assert entry.data[CONF_HOST] not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)

    @pytest.mark.asyncio
    async def test_connection_state_is_child_local_and_reauth_is_parent_deduped(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True

        tina = entry.runtime_data.agents["child-butler"]
        gary = entry.runtime_data.agents["child-concierge"]
        tina_changed = MagicMock()
        gary_changed = MagicMock()
        tina.async_add_availability_listener(tina_changed)
        gary.async_add_availability_listener(gary_changed)

        tina.client.state_callback(ConnectionState.CONNECTED)
        assert tina.available is True
        assert gary.available is False
        tina_changed.assert_called_once_with()
        gary_changed.assert_not_called()

        tina.client.state_callback(ConnectionState.AUTH_FAILED)
        gary.client.state_callback(ConnectionState.AUTH_FAILED)
        entry.async_start_reauth.assert_called_once_with(hass)
        assert tina.connection_state is ConnectionState.AUTH_FAILED
        assert gary.connection_state is ConnectionState.AUTH_FAILED
        assert tina_changed.call_count == 2
        assert gary_changed.call_count == 1

    @pytest.mark.asyncio
    async def test_auth_callback_during_sibling_cleanup_does_not_start_reauth(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()
        first_close_started = asyncio.Event()
        release_first_close = asyncio.Event()

        async def block_first_close():
            first_close_started.set()
            await release_first_close.wait()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            assert await async_setup_entry(hass, entry) is True
            managers.items[0].close.side_effect = block_first_close

            unload_task = asyncio.create_task(async_unload_entry(hass, entry))
            await asyncio.wait_for(first_close_started.wait(), timeout=1)
            assert entry.runtime_data is None

            clients.children[1].state_callback(ConnectionState.AUTH_FAILED)
            entry.async_start_reauth.assert_not_called()

            release_first_close.set()
            assert await asyncio.wait_for(unload_task, timeout=1) is True

    @pytest.mark.asyncio
    async def test_child_auth_rolls_back_every_sibling_without_masking_auth(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(
            _catalog("butler", "Tina", "concierge", "Gary"),
            start_effects=(AuthenticationError("PRIVATE_AUTH_FAILURE"), None),
        )
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            with pytest.raises(ConfigEntryAuthFailed):
                await async_setup_entry(hass, entry)

        assert entry.runtime_data is None
        assert len(clients.children) == 2
        assert all(client.close.await_count == 1 for client in clients.children)
        assert all(manager.close.await_count == 1 for manager in managers.items)
        hass.config_entries.async_forward_entry_setups.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_setup_auth_callback_relies_on_auth_failed_exception_only(self):
        child = _subentry("butler", "Tina", subentry_id="child-butler")
        entry = _entry(child)
        hass = _hass()

        async def authenticate_then_fail(**_kwargs):
            clients.children[0].state_callback(ConnectionState.AUTH_FAILED)
            raise AuthenticationError("PRIVATE_AUTH_FAILURE")

        clients = _ClientHarness(
            _catalog("butler", "Tina"),
            start_effects=(authenticate_then_fail,),
        )
        managers = _ManagerHarness()

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch("custom_components.casa.SessionRegistrationListener"):
            with pytest.raises(ConfigEntryAuthFailed):
                await async_setup_entry(hass, entry)

        entry.async_start_reauth.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure_seam",
        [
            "listener",
            "platform",
            "add_update_listener",
            "async_on_unload",
        ],
    )
    async def test_setup_rollback_detaches_and_closes_every_child(
        self,
        failure_seam,
    ):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()
        setup_error = RuntimeError(f"{failure_seam} failed")
        cleanup_error = RuntimeError("first cleanup failed")
        update_unsubscribe = MagicMock()

        async def fail_platform(*_args):
            managers.items[0].close.side_effect = cleanup_error
            raise setup_error

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            if failure_seam == "listener":
                listener_cls.return_value.attach.side_effect = setup_error
            elif failure_seam == "add_update_listener":
                entry.add_update_listener.side_effect = setup_error
            elif failure_seam == "async_on_unload":
                entry.add_update_listener.return_value = update_unsubscribe
                entry.async_on_unload.side_effect = setup_error
            else:
                hass.config_entries.async_forward_entry_setups.side_effect = fail_platform

            with pytest.raises(RuntimeError) as raised:
                await async_setup_entry(hass, entry)

        assert raised.value is setup_error
        assert entry.runtime_data is None
        listener_cls.return_value.detach.assert_called_once_with()
        assert all(manager.close.await_count == 1 for manager in managers.items)
        assert all(client.close.await_count == 1 for client in clients.children)
        if failure_seam == "add_update_listener":
            entry.add_update_listener.assert_called_once_with(ANY)
            entry.async_on_unload.assert_not_called()
        elif failure_seam == "async_on_unload":
            entry.add_update_listener.assert_called_once_with(ANY)
            entry.async_on_unload.assert_called_once_with(
                update_unsubscribe,
            )
            update_unsubscribe.assert_called_once_with()
        else:
            entry.add_update_listener.assert_not_called()


class TestParentUnload:
    @pytest.mark.asyncio
    async def test_platform_unload_failure_keeps_runtime_active(self):
        entry = _entry()
        runtime = MagicMock()
        entry.runtime_data = runtime
        hass = _hass()
        hass.config_entries.async_unload_platforms = AsyncMock(return_value=False)

        assert await async_unload_entry(hass, entry) is False

        assert entry.runtime_data is runtime
        runtime.listener.detach.assert_not_called()
        for child in runtime.agents.values():
            child.async_close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unload_detaches_once_and_closes_every_child_before_raising(self):
        entry = _entry()
        hass = _hass()
        cleanup_error = RuntimeError("first child failed")
        first = MagicMock()
        first.async_close = AsyncMock(side_effect=cleanup_error)
        second = MagicMock()
        second.async_close = AsyncMock()
        listener = MagicMock()
        entry.runtime_data = MagicMock(
            listener=listener,
            agents={"first": first, "second": second},
        )

        with pytest.raises(RuntimeError) as raised:
            await async_unload_entry(hass, entry)

        assert raised.value is cleanup_error
        listener.detach.assert_called_once_with()
        first.async_close.assert_awaited_once_with()
        second.async_close.assert_awaited_once_with()
        assert entry.runtime_data is None

    @pytest.mark.asyncio
    async def test_unload_cancels_all_registration_tasks_and_listener(self):
        butler = _subentry("butler", "Tina", subentry_id="child-butler")
        concierge = _subentry("concierge", "Gary", subentry_id="child-concierge")
        entry = _entry(butler, concierge)
        hass = _hass()
        clients = _ClientHarness(_catalog("butler", "Tina", "concierge", "Gary"))
        managers = _ManagerHarness()
        started = [asyncio.Event(), asyncio.Event()]
        cancelled = [asyncio.Event(), asyncio.Event()]

        with patch("custom_components.casa.CasaApiClient", side_effect=clients), \
             patch(
                 "custom_components.casa.BackgroundDeliveryManager",
                 side_effect=managers,
             ), \
             patch(
                 "custom_components.casa.SessionRegistrationListener",
             ) as listener_cls:
            assert await async_setup_entry(hass, entry) is True

            for index, client in enumerate(clients.children):
                async def blocked_registration(_index=index, **_kwargs):
                    started[_index].set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cancelled[_index].set()
                        raise

                client.register_session = AsyncMock(side_effect=blocked_registration)

            listener_cls.call_args.kwargs["on_listening"]("dev-kitchen")
            await asyncio.gather(*(event.wait() for event in started))
            runtime = entry.runtime_data

            assert await async_unload_entry(hass, entry) is True

        listener_cls.return_value.detach.assert_called_once_with()
        assert all(event.is_set() for event in cancelled)
        assert all(
            child.registration_task_count_for_test == 0
            for child in runtime.agents.values()
        )
        assert entry.runtime_data is None
