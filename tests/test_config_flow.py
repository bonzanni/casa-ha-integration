"""Tests for catalog-backed Casa parent and agent-subentry config flows."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from homeassistant.config_entries import ConfigSubentry

from custom_components.casa.api import AuthenticationError
from custom_components.casa.catalog import (
    CatalogValidationError,
    VoiceAgent,
    VoiceAgentCatalog,
)
from custom_components.casa.config_flow import (
    CasaAgentSubentryFlow,
    CasaConfigFlow,
    CasaOptionsFlow,
    _fetch_catalog,
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
    DEFAULT_IDLE_STABILITY_MS,
    DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
)


CATALOG = VoiceAgentCatalog(
    schema_version=1,
    agents=(
        VoiceAgent(role="butler", name="Tina"),
        VoiceAgent(role="concierge", name="Gary"),
    ),
)
MANUAL_INPUT = {
    CONF_HOST: "1.1.1.1",
    CONF_PORT: 18065,
    CONF_WEBHOOK_SECRET: "secret",
}


def _schema_keys(result: dict) -> set[str]:
    return {
        getattr(key, "schema", key)
        for key in result["data_schema"].schema
    }


def _assert_catalog_children(result: dict) -> None:
    children = result["subentries"]
    assert [(child["unique_id"], child["title"]) for child in children] == [
        ("butler", "Butler"),
        ("concierge", "Concierge"),
    ]
    assert children[0]["data"][CONF_SESSION_MODE] == SESSION_MODE_DEVICE
    assert (
        children[1]["data"][CONF_SESSION_MODE]
        == SESSION_MODE_CONVERSATION
    )


def _agent_subentry() -> ConfigSubentry:
    return ConfigSubentry(
        data=MappingProxyType({
            CONF_ROLE: "concierge",
            CONF_AGENT_NAME: "Gary",
            CONF_SESSION_MODE: SESSION_MODE_CONVERSATION,
            CONF_TRANSPORT: TRANSPORT_WS,
            CONF_IDLE_STABILITY_MS: DEFAULT_IDLE_STABILITY_MS,
        }),
        subentry_type=SUBENTRY_TYPE_AGENT,
        title="Gary",
        unique_id="concierge",
        subentry_id="gary-subentry",
    )


class TestFetchCatalog:
    @pytest.mark.asyncio
    async def test_fetches_with_shared_session_and_always_closes(self):
        hass = MagicMock()
        session = MagicMock()
        with (
            patch(
                "custom_components.casa.config_flow.async_get_clientsession",
                return_value=session,
            ),
            patch("custom_components.casa.config_flow.CasaApiClient") as client_cls,
        ):
            client = client_cls.return_value
            client.fetch_voice_agents = AsyncMock(return_value=CATALOG)
            client.close = AsyncMock()

            result = await _fetch_catalog(hass, MANUAL_INPUT)

        assert result is CATALOG
        client_cls.assert_called_once_with(
            session=session,
            host="1.1.1.1",
            port=18065,
            webhook_secret="secret",
        )
        client.close.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_closes_when_fetch_fails(self):
        with patch("custom_components.casa.config_flow.CasaApiClient") as client_cls:
            client = client_cls.return_value
            client.fetch_voice_agents = AsyncMock(
                side_effect=aiohttp.ClientError("unavailable"),
            )
            client.close = AsyncMock()

            with pytest.raises(aiohttp.ClientError):
                await _fetch_catalog(MagicMock(), MANUAL_INPUT)

        client.close.assert_awaited_once_with()


class TestUserStep:
    @pytest.mark.asyncio
    async def test_form_shown_without_agent_role(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_user(None)

        assert result["type"] == "form"
        assert result["step_id"] == "user"
        assert _schema_keys(result) == {
            CONF_HOST,
            CONF_PORT,
            CONF_WEBHOOK_SECRET,
        }

    @pytest.mark.asyncio
    async def test_creates_parent_with_catalog_children(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(return_value=CATALOG),
        ):
            result = await flow.async_step_user(dict(MANUAL_INPUT))

        assert CasaConfigFlow.VERSION == 2
        assert result["type"] == "create_entry"
        assert result["title"] == "Casa"
        assert result["data"] == MANUAL_INPUT
        _assert_catalog_children(result)
        assert flow.abort_entries_match_calls == [
            {CONF_HOST: "1.1.1.1", CONF_PORT: 18065},
        ]

    @pytest.mark.asyncio
    async def test_exact_host_port_duplicate_aborts_before_fetch(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        duplicate_abort = RuntimeError("duplicate endpoint")
        flow._async_abort_entries_match = MagicMock(side_effect=duplicate_abort)

        with (
            patch(
                "custom_components.casa.config_flow._fetch_catalog",
                AsyncMock(),
            ) as fetch,
            pytest.raises(RuntimeError, match="duplicate endpoint"),
        ):
            await flow.async_step_user(dict(MANUAL_INPUT))

        flow._async_abort_entries_match.assert_called_once_with({
            CONF_HOST: "1.1.1.1",
            CONF_PORT: 18065,
        })
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("failure", "error"),
        [
            (AuthenticationError("bad"), "invalid_auth"),
            (CatalogValidationError("bad catalog"), "invalid_catalog"),
            (aiohttp.ClientError("offline"), "cannot_connect"),
        ],
    )
    async def test_actionable_fetch_errors_are_distinct(self, failure, error):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(side_effect=failure),
        ):
            result = await flow.async_step_user(dict(MANUAL_INPUT))

        assert result["type"] == "form"
        assert result["errors"]["base"] == error

    @pytest.mark.asyncio
    async def test_unexpected_setup_error_log_omits_exception_details(self, caplog):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        canary = "PRIVATE_SETUP_EXCEPTION_CANARY"
        with (
            patch(
                "custom_components.casa.config_flow._fetch_catalog",
                AsyncMock(side_effect=RuntimeError(canary)),
            ),
            caplog.at_level(
                logging.ERROR,
                logger="custom_components.casa.config_flow",
            ),
        ):
            result = await flow.async_step_user(dict(MANUAL_INPUT))

        assert result["errors"]["base"] == "unknown"
        assert canary not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)


class TestHassio:
    @staticmethod
    def _configured_flow() -> CasaConfigFlow:
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._host = "casa.local"
        flow._port = 18065
        flow._secret = "secret"
        flow._discovery_name = "Casa Add-on"
        return flow

    @pytest.mark.asyncio
    async def test_discovery_checks_exact_endpoint_and_shows_confirmation(self):
        from homeassistant.helpers.service_info.hassio import HassioServiceInfo

        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        info = HassioServiceInfo(
            config={
                "schema_version": 1,
                "host": "casa.local",
                "port": 18065,
                "webhook_secret": "secret",
                "addon": "casa",
            },
            name="Casa Add-on",
            slug="casa",
            uuid="uuid-1",
        )

        result = await flow.async_step_hassio(info)

        assert result["type"] == "form"
        assert result["step_id"] == "hassio_confirm"
        assert result["description_placeholders"] == {
            "name": "Casa Add-on",
            "host": "casa.local",
            "port": "18065",
        }
        assert flow.abort_entries_match_calls == [
            {CONF_HOST: "casa.local", CONF_PORT: 18065},
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"schema_version": 1, "host": "casa", "port": 18065},
            {"schema_version": 1, "host": "casa", "port": True,
             "webhook_secret": "secret"},
            {"schema_version": 1, "host": "casa", "port": 65536,
             "webhook_secret": "secret"},
            {"schema_version": True, "host": "casa", "port": 18065,
             "webhook_secret": "secret"},
            {"schema_version": 2, "host": "casa", "port": 18065,
             "webhook_secret": "secret"},
            {"schema_version": 1, "host": "", "port": 18065,
             "webhook_secret": "secret"},
            {"schema_version": 1, "host": "casa", "port": 18065,
             "webhook_secret": ""},
            {"schema_version": 1, "host": "casa", "port": 18065,
             "token": "legacy-secret"},
            {"schema_version": 1, "host": "casa", "port": 18065,
             "webhook_secret": "secret", "unexpected": "value"},
        ],
    )
    async def test_rejects_malformed_or_legacy_discovery_payload(self, config):
        from homeassistant.helpers.service_info.hassio import HassioServiceInfo

        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        info = HassioServiceInfo(
            config=config,
            name="Casa Add-on",
            slug="casa",
            uuid="uuid-1",
        )

        result = await flow.async_step_hassio(info)

        assert result == {"type": "abort", "reason": "invalid_discovery"}
        assert flow.abort_entries_match_calls == []
        assert flow.unique_id is None

    @pytest.mark.asyncio
    async def test_new_discovery_preserves_exact_endpoint_duplicate_protection(self):
        from homeassistant.helpers.service_info.hassio import HassioServiceInfo

        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._async_abort_entries_match = MagicMock(
            side_effect=RuntimeError("duplicate endpoint"),
        )
        info = HassioServiceInfo(
            config={
                "schema_version": 1,
                "host": "casa.local",
                "port": 18065,
                "webhook_secret": "secret",
                "addon": "casa",
            },
            name="Casa Add-on",
            slug="casa",
            uuid="uuid-1",
        )

        with pytest.raises(RuntimeError, match="duplicate endpoint"):
            await flow.async_step_hassio(info)

        flow._async_abort_entries_match.assert_called_once_with({
            CONF_HOST: "casa.local",
            CONF_PORT: 18065,
        })

    @pytest.mark.asyncio
    async def test_rediscovery_updates_existing_parent_and_reloads_without_catalog(self):
        from homeassistant.helpers.service_info.hassio import HassioServiceInfo

        child = _agent_subentry()
        children = {child.subentry_id: child}
        entry = SimpleNamespace(
            entry_id="entry-1",
            data={
                CONF_HOST: "old-casa.local",
                CONF_PORT: 18065,
                CONF_WEBHOOK_SECRET: "old-secret",
            },
            subentries=children,
        )
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow.async_set_unique_id = AsyncMock(return_value=entry)
        update = MagicMock(return_value={
            "type": "abort", "reason": "discovery_updated",
        })
        flow.async_update_reload_and_abort = update
        info = HassioServiceInfo(
            config={
                "schema_version": 1,
                "host": "casa.local",
                "port": 18066,
                "webhook_secret": "rotated-secret",
                "addon": "casa",
            },
            name="Casa Add-on",
            slug="casa",
            uuid="uuid-1",
        )

        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(side_effect=AssertionError("must not fetch catalog")),
        ):
            result = await flow.async_step_hassio(info)

        assert result == {"type": "abort", "reason": "discovery_updated"}
        update.assert_called_once_with(
            entry,
            data_updates={
                CONF_HOST: "casa.local",
                CONF_PORT: 18066,
                CONF_WEBHOOK_SECRET: "rotated-secret",
            },
            reason="discovery_updated",
        )
        assert entry.subentries is children
        assert entry.subentries[child.subentry_id] is child
        assert flow.abort_entries_match_calls == []

    @pytest.mark.asyncio
    async def test_confirmation_creates_parent_with_catalog_children(self):
        flow = self._configured_flow()
        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(return_value=CATALOG),
        ):
            result = await flow.async_step_hassio_confirm({"confirm": True})

        assert result["type"] == "create_entry"
        assert result["title"] == "Casa"
        _assert_catalog_children(result)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("failure", "error", "attempts"),
        [
            (AuthenticationError("bad"), "invalid_auth", 1),
            (CatalogValidationError("bad catalog"), "invalid_catalog", 1),
            (aiohttp.ClientError("offline"), "cannot_connect", 3),
        ],
    )
    async def test_confirmation_reports_distinct_errors(
        self,
        failure,
        error,
        attempts,
        monkeypatch,
    ):
        flow = self._configured_flow()
        fetch = AsyncMock(side_effect=failure)

        async def no_sleep(*args, **kwargs):
            return None

        monkeypatch.setattr("custom_components.casa.config_flow._fetch_catalog", fetch)
        monkeypatch.setattr("custom_components.casa.config_flow._sleep", no_sleep)

        result = await flow.async_step_hassio_confirm({"confirm": True})

        assert fetch.await_count == attempts
        assert result["type"] == "form"
        assert result["errors"]["base"] == error


class TestReauth:
    @pytest.mark.asyncio
    async def test_confirm_form(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_reauth_confirm(None)

        assert result["type"] == "form"

    @pytest.mark.asyncio
    async def test_loaded_entry_replaces_secret_and_uses_listener_reload(self):
        child = _agent_subentry()
        children = {child.subentry_id: child}
        entry = SimpleNamespace(
            entry_id="entry-1",
            data={
                CONF_HOST: "casa.local",
                CONF_PORT: 18065,
                CONF_WEBHOOK_SECRET: "old-secret",
            },
            subentries=children,
            update_listeners=(MagicMock(),),
        )
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._get_reauth_entry = MagicMock(return_value=entry)
        flow.async_update_reload_and_abort = MagicMock(
            side_effect=AssertionError("the parent update listener reloads"),
        )
        fetch = AsyncMock(return_value=CATALOG)

        with patch("custom_components.casa.config_flow._fetch_catalog", fetch):
            result = await flow.async_step_reauth_confirm({
                CONF_WEBHOOK_SECRET: "new-secret",
            })

        assert result == {"type": "abort", "reason": "reauth_successful"}
        fetch.assert_awaited_once_with(flow.hass, {
            CONF_HOST: "casa.local",
            CONF_PORT: 18065,
            CONF_WEBHOOK_SECRET: "new-secret",
        })
        assert entry.data[CONF_WEBHOOK_SECRET] == "new-secret"
        assert entry.subentries is children
        assert entry.subentries[child.subentry_id] is child
        flow.hass.config_entries.async_schedule_reload.assert_not_called()

    @pytest.mark.asyncio
    async def test_loaded_entry_same_secret_schedules_one_explicit_reload(self):
        child = _agent_subentry()
        children = {child.subentry_id: child}
        entry = SimpleNamespace(
            entry_id="entry-1",
            data={
                CONF_HOST: "casa.local",
                CONF_PORT: 18065,
                CONF_WEBHOOK_SECRET: "restored-secret",
            },
            subentries=children,
            update_listeners=(MagicMock(),),
        )
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._get_reauth_entry = MagicMock(return_value=entry)
        update_and_abort = flow.async_update_and_abort
        flow.async_update_and_abort = MagicMock(wraps=update_and_abort)
        flow.async_update_reload_and_abort = MagicMock(
            side_effect=AssertionError("loaded entry already has a listener"),
        )

        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(return_value=CATALOG),
        ):
            result = await flow.async_step_reauth_confirm({
                CONF_WEBHOOK_SECRET: "restored-secret",
            })

        assert result == {"type": "abort", "reason": "reauth_successful"}
        flow.async_update_and_abort.assert_called_once_with(
            entry,
            data_updates={CONF_WEBHOOK_SECRET: "restored-secret"},
        )
        flow.hass.config_entries.async_schedule_reload.assert_called_once_with(
            "entry-1",
        )
        assert entry.subentries is children
        assert entry.subentries[child.subentry_id] is child

    @pytest.mark.asyncio
    async def test_setup_failed_entry_replaces_secret_and_schedules_reload(self):
        child = _agent_subentry()
        children = {child.subentry_id: child}
        entry = SimpleNamespace(
            entry_id="entry-1",
            data={
                CONF_HOST: "casa.local",
                CONF_PORT: 18065,
                CONF_WEBHOOK_SECRET: "old-secret",
            },
            subentries=children,
            update_listeners=(),
        )
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._get_reauth_entry = MagicMock(return_value=entry)
        flow.async_update_and_abort = MagicMock(
            side_effect=AssertionError("no listener can reload this entry"),
        )
        reload_and_abort = flow.async_update_reload_and_abort
        flow.async_update_reload_and_abort = MagicMock(wraps=reload_and_abort)

        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(return_value=CATALOG),
        ):
            result = await flow.async_step_reauth_confirm({
                CONF_WEBHOOK_SECRET: "new-secret",
            })

        assert result == {"type": "abort", "reason": "reauth_successful"}
        flow.async_update_reload_and_abort.assert_called_once_with(
            entry,
            data_updates={CONF_WEBHOOK_SECRET: "new-secret"},
        )
        assert entry.data[CONF_WEBHOOK_SECRET] == "new-secret"
        assert entry.subentries is children
        assert entry.subentries[child.subentry_id] is child

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("failure", "error"),
        [
            (AuthenticationError("bad"), "invalid_auth"),
            (CatalogValidationError("bad catalog"), "invalid_catalog"),
            (aiohttp.ClientError("offline"), "cannot_connect"),
        ],
    )
    async def test_reports_distinct_errors(self, failure, error):
        entry = SimpleNamespace(
            data={CONF_HOST: "casa.local", CONF_PORT: 18065},
            subentries={},
        )
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._get_reauth_entry = MagicMock(return_value=entry)

        with patch(
            "custom_components.casa.config_flow._fetch_catalog",
            AsyncMock(side_effect=failure),
        ):
            result = await flow.async_step_reauth_confirm({
                CONF_WEBHOOK_SECRET: "new-secret",
            })

        assert result["type"] == "form"
        assert result["errors"]["base"] == error


class TestParentOptions:
    @pytest.mark.asyncio
    async def test_form_contains_only_satellite_overrides(self):
        flow = CasaOptionsFlow()

        result = await flow.async_step_init(None)

        assert result["type"] == "form"
        assert _schema_keys(result) == {CONF_SATELLITE_ENTITY_OVERRIDES}

    @pytest.mark.asyncio
    async def test_saved_options_strip_unrelated_agent_values(self):
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init({
            CONF_SATELLITE_ENTITY_OVERRIDES: "{}",
            "agent_role": "legacy",
            CONF_TRANSPORT: TRANSPORT_SSE,
        })

        assert result == {
            "type": "create_entry",
            "data": {
                CONF_SATELLITE_ENTITY_OVERRIDES:
                    DEFAULT_SATELLITE_ENTITY_OVERRIDES,
            },
        }

    @pytest.mark.asyncio
    async def test_valid_override_is_registry_checked_and_canonicalized(self):
        from homeassistant.helpers import entity_registry as er

        registry = MagicMock()
        registry.async_get.return_value = MagicMock(device_id="dev-k")
        er.async_get = MagicMock(return_value=registry)
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init({
            CONF_SATELLITE_ENTITY_OVERRIDES: (
                '{ "dev-k" : "assist_satellite.kitchen" }'
            ),
        })

        assert result["type"] == "create_entry"
        assert result["data"][CONF_SATELLITE_ENTITY_OVERRIDES] == (
            '{"dev-k":"assist_satellite.kitchen"}'
        )
        registry.async_get.assert_called_once_with("assist_satellite.kitchen")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raw", ["not-json", "[]", '{"dev-k":7}'])
    async def test_invalid_override_json_is_rejected(self, raw):
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init({
            CONF_SATELLITE_ENTITY_OVERRIDES: raw,
        })

        assert result["type"] == "form"
        assert result["errors"][CONF_SATELLITE_ENTITY_OVERRIDES] == (
            "invalid_satellite_entity_overrides"
        )


class TestAgentSubentryFlow:
    @pytest.mark.asyncio
    async def test_supported_subentry_type_is_registered(self):
        supported = CasaConfigFlow.async_get_supported_subentry_types(
            MagicMock(),
        )

        assert supported == {SUBENTRY_TYPE_AGENT: CasaAgentSubentryFlow}

    @pytest.mark.asyncio
    async def test_user_creation_aborts_as_catalog_managed(self):
        flow = CasaAgentSubentryFlow()

        result = await flow.async_step_user()

        assert result == {"type": "abort", "reason": "catalog_managed"}

    @pytest.mark.asyncio
    async def test_reconfigure_form_exposes_only_mutable_fields(self, hass):
        child = _agent_subentry()
        flow = CasaAgentSubentryFlow()
        flow.hass = hass
        flow._entry = SimpleNamespace(subentries={child.subentry_id: child})
        flow._reconfigure_subentry = child

        result = await flow.async_step_reconfigure(None)

        assert result["type"] == "form"
        assert _schema_keys(result) == {
            CONF_SESSION_MODE,
            CONF_TRANSPORT,
            CONF_IDLE_STABILITY_MS,
        }

    @pytest.mark.asyncio
    async def test_reconfigure_preserves_role_and_name(self, hass):
        child = _agent_subentry()
        entry = SimpleNamespace(subentries={child.subentry_id: child})
        flow = CasaAgentSubentryFlow()
        flow.hass = hass
        flow._entry = entry
        flow._reconfigure_subentry = child

        result = await flow.async_step_reconfigure({
            CONF_SESSION_MODE: SESSION_MODE_DEVICE,
            CONF_TRANSPORT: TRANSPORT_SSE,
            CONF_IDLE_STABILITY_MS: 1250,
        })

        assert result == {
            "type": "abort",
            "reason": "reconfigure_successful",
        }
        assert child.data == {
            CONF_ROLE: "concierge",
            CONF_AGENT_NAME: "Gary",
            CONF_SESSION_MODE: SESSION_MODE_DEVICE,
            CONF_TRANSPORT: TRANSPORT_SSE,
            CONF_IDLE_STABILITY_MS: 1250,
        }
        assert child.unique_id == "concierge"
        assert child.title == "Gary"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stability_ms", [True, -1, 5001])
    async def test_invalid_idle_stability_is_rejected(self, hass, stability_ms):
        child = _agent_subentry()
        flow = CasaAgentSubentryFlow()
        flow.hass = hass
        flow._entry = SimpleNamespace(subentries={child.subentry_id: child})
        flow._reconfigure_subentry = child

        result = await flow.async_step_reconfigure({
            CONF_SESSION_MODE: SESSION_MODE_CONVERSATION,
            CONF_TRANSPORT: TRANSPORT_WS,
            CONF_IDLE_STABILITY_MS: stability_ms,
        })

        assert result["type"] == "form"
        assert result["errors"][CONF_IDLE_STABILITY_MS] == (
            "invalid_idle_stability"
        )
        assert hass.config_entries.updated_subentries == []


PARENT_OVERRIDE_DESCRIPTION = (
    "Usually leave this empty. Configure it only when one Home Assistant device "
    "has multiple assist_satellite entities and Casa cannot determine where to "
    "speak a queued result. For example, map device ID abc123 to "
    "assist_satellite.kitchen_voice; results originating from abc123 will then "
    "play on that kitchen satellite. Without an override, Casa safely declines "
    "an ambiguous announcement rather than speaking on the wrong device."
)
CHILD_IDLE_DESCRIPTION = (
    "How long Casa waits after the voice assistant becomes idle before speaking "
    "a queued specialist result. This prevents Gary from interrupting an active "
    "conversation or follow-up. If the device is already idle, the result is "
    "spoken immediately. Default: 750 ms."
)


@pytest.mark.parametrize("filename", ["strings.json", "translations/en.json"])
def test_catalog_and_subentry_copy(filename):
    path = Path(__file__).parents[1] / "custom_components" / "casa" / filename
    content = json.loads(path.read_text())

    assert content["options"]["step"]["init"]["data_description"][
        CONF_SATELLITE_ENTITY_OVERRIDES
    ] == PARENT_OVERRIDE_DESCRIPTION
    subentry_copy = content["config_subentries"][SUBENTRY_TYPE_AGENT]
    assert subentry_copy["entry_type"] == "Casa voice agent"
    assert subentry_copy["initiate_flow"] == {
        "user": "Voice agents are discovered from Casa automatically",
    }
    assert subentry_copy["step"]["reconfigure"]["data_description"][
        CONF_IDLE_STABILITY_MS
    ] == CHILD_IDLE_DESCRIPTION
    assert "ha_voice" in content["config"]["error"]["invalid_catalog"]
    assert content["config"]["abort"]["discovery_updated"] == (
        "Casa connection details updated."
    )
    assert "catalog_managed" in subentry_copy["abort"]
    assert "reconfigure_successful" in subentry_copy["abort"]
    assert "agent_role" not in json.dumps(content)
