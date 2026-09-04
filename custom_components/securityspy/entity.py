"""Shared Entity definition for SecurotySpy Integration."""
from __future__ import annotations

import logging

from homeassistant.const import ATTR_ATTRIBUTION
from homeassistant.core import callback
from homeassistant.helpers.entity import Entity, DeviceInfo
import homeassistant.helpers.device_registry as dr

from .const import (
    ATTR_BRAND,
    DEFAULT_ATTRIBUTION,
    DEFAULT_BRAND,
    DOMAIN,
)
from .data import SecuritySpyData

_LOGGER = logging.getLogger(__name__)


class SecuritySpyEntity(Entity):
    """Base class for SecuritySpy entities."""

    def __init__(
        self,
        secspy,
        secspy_data: SecuritySpyData,
        server_info,
        device_id,
        sensor_type,
    ):
        """Initialize the entity."""
        super().__init__()
        self.secspy = secspy
        self.secspy_data = secspy_data
        self._device_id = device_id
        self._sensor_type = sensor_type

        self._device_data = self.secspy_data.data[self._device_id]
        self._device_name = self._device_data["name"]
        self._firmware_version = server_info["server_version"]
        self._server_id = server_info["server_id"]
        self._schedule_presets = server_info["schedule_presets"]
        self._device_type = self._device_data["type"]
        self._model = self._device_data["model"]
        self._server_ip = server_info["server_ip_address"]
        self._server_port = server_info["server_port"]

        self._attr_available = self.secspy_data.last_update_success
        if self._sensor_type is None:
            self._attr_unique_id = f"{self._device_id}_{self._server_id}"
        else:
            self._attr_unique_id = (
                f"{self._sensor_type}_{self._server_id}_{self._device_id}"
            )
        self._attr_has_entity_name = True
        self._attr_should_poll = False

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information, resolving the NVR's registry ID for via_device_id.

        via_device (identifier tuple) is deprecated by HA in favor of via_device_id
        (the parent device's registry ID); the NVR device is already registered by
        __init__.py before entity platforms are set up, so it's always resolvable here.
        async_get_device_by_identifier (not the also-deprecated async_get_device) is
        used since identifiers are only guaranteed unique within a config entry.
        """
        via_device_id = None
        config_entry = getattr(self.platform, "config_entry", None)
        if config_entry is not None:
            via_device_entry = dr.async_get(self.hass).async_get_device_by_identifier(
                (DOMAIN, self._server_id), config_entry.entry_id
            )
            if via_device_entry is not None:
                via_device_id = via_device_entry.id
        return DeviceInfo(
            identifiers={(DOMAIN, f"{self._server_id}_{self._device_id}")},
            name=self._device_name,
            manufacturer=DEFAULT_BRAND,
            model=self._model,
            sw_version=self._firmware_version,
            via_device_id=via_device_id,
            configuration_url=f"http://{self._server_ip}:{self._server_port}/camerasettings?cameraNum={self._device_id}",
        )

    @property
    def extra_state_attributes(self):
        """Return the device state attributes."""
        return {
            ATTR_ATTRIBUTION: DEFAULT_ATTRIBUTION,
            ATTR_BRAND: DEFAULT_BRAND,
        }

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        self.async_on_remove(
            self.secspy_data.async_subscribe_device_id(
                self._device_id, self._handle_device_update
            )
        )

    @callback
    def _handle_device_update(self) -> None:
        """Handle pushed updates from SecuritySpy."""
        self._device_data = self.secspy_data.data[self._device_id]
        self._attr_available = self.secspy_data.last_update_success
        self.async_schedule_update_ha_state()
