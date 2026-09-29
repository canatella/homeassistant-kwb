"""Manage the shared asynchronous pykwb listener and connection."""

import asyncio
import logging
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

from homeassistant.const import CONF_DEVICE, CONF_HOST, CONF_PORT, CONF_TYPE
from homeassistant.core import HomeAssistant
from pykwb import kwb

from .const import CONF_CONTROLLER, CONF_HEATER_MODEL, SIGNAL_MAP_SOURCES

_LOGGER = logging.getLogger(__name__)


class KWBClient(kwb.KWBEasyfire):
    """Adapt pykwb's listener to Home Assistant's task lifecycle."""

    _listener_task: asyncio.Task[None] | None = None

    async def _async_listen(self, hass: HomeAssistant) -> None:
        """Let pykwb manage reads, connection failures, and retries."""
        try:
            await self.listen_forever()
        except (OSError, EOFError):
            _LOGGER.exception("KWB connection closed while reading")
        finally:
            for sensor in self.get_sensors():
                sensor.value = None
            await hass.async_add_executor_job(self._close_connection)

    def async_start(self, hass: HomeAssistant) -> None:
        """Start a single listener without a pykwb reader thread."""
        self._listener_task = hass.async_create_background_task(
            self._async_listen(hass), "KWB listener"
        )

    async def async_stop(self, hass: HomeAssistant) -> None:
        """Cancel reads before closing the transport in the executor."""
        if self._listener_task is not None:
            self._listener_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None
        # Also handles a task cancelled before its coroutine first ran.
        await hass.async_add_executor_job(self._close_connection)


def _signal_map_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Select the signal map matching the configured heater and controller."""
    source = SIGNAL_MAP_SOURCES.get(
        (config.get(CONF_HEATER_MODEL), config.get(CONF_CONTROLLER))
    )
    return {} if source is None else {"source": source}


def create_client(config: Mapping[str, Any], *, reconnect: bool = True) -> KWBClient:
    """Open the configured connection (must run in an executor)."""
    signal_map = _signal_map_config(config)
    if config[CONF_TYPE] == "serial":
        return KWBClient(
            kwb.PROP_MODE_SERIAL,
            _serial_device=config[CONF_DEVICE],
            _config=signal_map,
        )
    if config[CONF_TYPE] == "tcp_server":
        # We listen; the serial server connects to us. CONF_PORT is the local
        # port and CONF_HOST the only peer accepted. Its own client-mode
        # reconnect-and-reboot then handles recovery.
        return KWBClient(
            kwb.PROP_MODE_TCP_SERVER,
            config[CONF_HOST],
            config[CONF_PORT],
            _config=signal_map,
        )
    if config[CONF_TYPE] == "udp":
        # CONF_PORT is the local port we bind; CONF_HOST is the serial
        # server's address, used only to reject datagrams from anyone else.
        # There is no connection, so reconnect has nothing to act on.
        return KWBClient(
            kwb.PROP_MODE_UDP,
            config[CONF_HOST],
            config[CONF_PORT],
            _config=signal_map,
        )
    return KWBClient(
        kwb.PROP_MODE_TCP,
        config[CONF_HOST],
        config[CONF_PORT],
        _config={**signal_map, "connection": {"reconnect": reconnect}},
    )


def validate_connection(config: Mapping[str, Any]) -> None:
    """Check that the transport can be opened, without starting a reader."""
    # A failed setup probe must raise instead of waiting for background retries.
    client = create_client(config, reconnect=False)
    client._close_connection()
