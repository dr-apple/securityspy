"""SecuritySpy Platform."""
from __future__ import annotations

import logging

from aiohttp.client_exceptions import ServerDisconnectedError
from awesomeversion import AwesomeVersion
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import (
    CONF_ID,
    CONF_HOST,
    CONF_PORT,
    CONF_USERNAME,
    CONF_PASSWORD,
)
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    ServiceValidationError,
)
from homeassistant.helpers.aiohttp_client import async_create_clientsession
from homeassistant.helpers.typing import ConfigType
import homeassistant.helpers.device_registry as dr
from pysecspy.errors import InvalidCredentials, RequestError
from pysecspy.secspy_server import SecSpyServer
from pysecspy.const import SERVER_ID

from .const import (
    CONF_DISABLE_RTSP,
    CONF_CONFIG_ENTRY_ID,
    CONF_MIN_SCORE,
    CONFIG_OPTIONS,
    DEFAULT_BRAND,
    DEFAULT_MIN_SCORE,
    DOMAIN,
    SECURITYSPY_PLATFORMS,
    SERVICE_ENABLE_SCHEDULE_PRESET,
    ENABLE_SCHEDULE_PRESET_SCHEMA,
    MIN_SECSPY_VERSION,
    ATTR_PRESET_ID,
)
from .data import SecuritySpyData, SecuritySpyRuntimeData

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up SecuritySpy services."""

    async def async_enable_schedule_preset(call: ServiceCall) -> None:
        """Enable a SecuritySpy schedule preset."""
        await async_handle_enable_schedule_preset(hass, call)

    hass.services.async_register(
        DOMAIN,
        SERVICE_ENABLE_SCHEDULE_PRESET,
        async_enable_schedule_preset,
        schema=ENABLE_SCHEDULE_PRESET_SCHEMA,
    )

    return True


@callback
def _async_import_options_from_data_if_missing(hass: HomeAssistant, entry: ConfigEntry):
    options = dict(entry.options)
    data = dict(entry.data)
    modified = False
    for importable_option in CONFIG_OPTIONS:
        if importable_option not in entry.options and importable_option in entry.data:
            options[importable_option] = entry.data[importable_option]
            del data[importable_option]
            modified = True

    if modified:
        hass.config_entries.async_update_entry(entry, data=data, options=options)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the SecuritySpy config entries."""
    _async_import_options_from_data_if_missing(hass, entry)

    session = async_create_clientsession(hass)
    securityspyserver = SecSpyServer(
        session,
        entry.data[CONF_HOST],
        entry.data[CONF_PORT],
        entry.data[CONF_USERNAME],
        entry.data[CONF_PASSWORD],
        entry.options.get(CONF_MIN_SCORE, DEFAULT_MIN_SCORE),
    )

    secspy_data = SecuritySpyData(hass, securityspyserver)

    try:
        server_info = await securityspyserver.get_server_information()
    except InvalidCredentials as unauthex:
        raise ConfigEntryAuthFailed from unauthex
    except (RequestError, ServerDisconnectedError) as notreadyerror:
        raise ConfigEntryNotReady from notreadyerror

    if AwesomeVersion(server_info["server_version"]) < AwesomeVersion(MIN_SECSPY_VERSION):
        _LOGGER.error(
            "This version of SecuritySpy is too old. Please upgrade to minimum V%s and try again.",
            MIN_SECSPY_VERSION,
        )
        return False

    if entry.unique_id is None:
        hass.config_entries.async_update_entry(entry, unique_id=server_info[SERVER_ID])

    await secspy_data.async_setup()
    if not secspy_data.last_update_success:
        raise ConfigEntryNotReady

    entry.runtime_data = SecuritySpyRuntimeData(
        secspy_data=secspy_data,
        nvr=securityspyserver,
        server_info=server_info,
        disable_stream=entry.options.get(CONF_DISABLE_RTSP, False),
    )
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = entry.runtime_data

    await _async_get_or_create_nvr_device_in_registry(hass, entry, server_info)
    await hass.config_entries.async_forward_entry_setups(entry, SECURITYSPY_PLATFORMS)

    return True


async def _async_get_or_create_nvr_device_in_registry(
    hass: HomeAssistant, entry: ConfigEntry, nvr
) -> None:
    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, nvr["server_id"])},
        manufacturer=DEFAULT_BRAND,
        name=entry.data[CONF_ID],
        model="macOS Computer",
        sw_version=nvr["server_version"],
    )


@callback
def _async_loaded_runtime_data(
    hass: HomeAssistant, entry_id: str | None = None
) -> list[SecuritySpyRuntimeData]:
    """Return runtime data for loaded SecuritySpy entries."""
    runtime_entries: list[SecuritySpyRuntimeData] = []
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry_id is not None and entry.entry_id != entry_id:
            continue
        if entry.state is ConfigEntryState.LOADED:
            runtime_entries.append(entry.runtime_data)
    return runtime_entries


async def async_handle_enable_schedule_preset(hass: HomeAssistant, call: ServiceCall):
    """Enable Schedule Preset."""

    preset_id = call.data[ATTR_PRESET_ID]
    entry_id = call.data.get(CONF_CONFIG_ENTRY_ID)
    runtime_entries = _async_loaded_runtime_data(hass, entry_id)

    if not runtime_entries:
        raise ServiceValidationError("No matching loaded SecuritySpy config entry")
    if len(runtime_entries) > 1:
        raise ServiceValidationError(
            "Schedule preset service requires exactly one loaded SecuritySpy entry"
        )

    _LOGGER.debug("Setting Schedule Preset ID: %s", preset_id)
    await runtime_entries[0].nvr.enable_schedule_preset(preset_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload SecuritySpy config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, SECURITYSPY_PLATFORMS
    )

    if unload_ok:
        await entry.runtime_data.secspy_data.async_stop()
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
