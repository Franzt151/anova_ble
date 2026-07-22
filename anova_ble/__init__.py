import asyncio
import logging
import time

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak_retry_connector import BleakClient, BLEDevice, establish_connection, BleakClientWithServiceCache
from enum import Enum
from .const import READ_DEVICE_STATUS, READ_CURRENT_TEMP, READ_TARGET_TEMP, READ_TIMER, READ_UNIT, DEVICE_NOTIFICATION_CHAR_UUID
from .const import CTL_START, CTL_STOP, CTL_TIMER_START, CTL_TIMER_STOP
from .const import SET_TARGET_TEMP, SET_TEMP_UNIT, SET_TIMER

_LOGGER = logging.getLogger(__name__)

from dataclasses import dataclass

class AnovaStatus(Enum):
    Running = "running"
    Stopped = "stopped"
    LowWater = "low water"

class AnovaTemperatureUnit(Enum):
    C = "c"
    F = "f"

class AnovaException(Exception):
    pass

class AnovaConnectionException(AnovaException):
    pass

class AnovaUpdateStateException(AnovaException):
    pass

@dataclass
class AnovaState():
    status: AnovaStatus
    current_temp: float # [5.0C, 99.9C], [41.0F, 211.8F]
    target_temp: float
    timer: tuple[int, AnovaStatus] # [0, 6000]
    unit: AnovaTemperatureUnit

class AnovaBLEPrecisionCooker():
    """Connects to Anova, gets information, and write state info."""
    state: AnovaState | None
    _client: BleakClient | None
    ble_device = BLEDevice | None

    def __init__(self, *, ble_device: BLEDevice = None):
        self.ble_device = ble_device
        self.name = "Anova"
        self.state = None
        self._cached_services = None
        self._client = None
        self._cmd_lock = asyncio.Lock()
        # Responses arrive on a single notify characteristic. We subscribe
        # once per connection and funnel every packet into this queue,
        # instead of subscribing/unsubscribing around each command.
        self._notify_queue: asyncio.Queue[bytearray] = asyncio.Queue()

    def set_ble_device(self, device: BLEDevice):
        # NOTE: do NOT clear self._client here. This callback fires on every
        # BLE advertisement. Dropping the client reference without
        # disconnecting leaks the existing connection and forces a reconnect
        # on the next poll, which single-connection devices like the Anova
        # cannot survive. establish_connection() already refreshes the
        # device via ble_device_callback.
        self.ble_device = device

    async def connect(self) -> None:
        if self._client and self._client.is_connected:
            return

        _LOGGER.debug(f"{self.name}: Attempting a connection to {self.ble_device.address}")

        try:
            self._client = await establish_connection(
                BleakClientWithServiceCache,
                self.ble_device,
                self.name,
                self._disconnected,
                cached_services=self._cached_services,
                ble_device_callback=lambda: self.ble_device
            )
            # Cache the resolved GATT services so subsequent reconnects can
            # skip service discovery, which is one of the slowest parts of
            # establishing a BLE connection. Without this the cache passed
            # above is always None and discovery runs every single time.
            self._cached_services = self._client.services
            await self._client.start_notify(
                DEVICE_NOTIFICATION_CHAR_UUID, self._handle_notify
            )
            _LOGGER.debug(f"{self.name}: Connected; MAC: {self.ble_device.address}")
        except Exception as e:
            raise AnovaConnectionException() from e

    def _handle_notify(self, _char: BleakGATTCharacteristic, data: bytearray) -> None:
        """Receive a response packet from the cooker."""
        self._notify_queue.put_nowait(data)

    def _disconnected(self, client: BleakClient):
        _LOGGER.debug(f"${self.name}: Disconnected from device; MAC {self.ble_device.address}")
        self._client = None

    async def disconnect(self) -> None:
        await self._client.disconnect()
        self._client = None
        self.ble_device = None

    async def update_state(self) -> AnovaState:
        if self.ble_device is None:
            return self.state

        if not self._client.is_connected:
            return self.state

        try:
            commands = [
                READ_DEVICE_STATUS,
                READ_CURRENT_TEMP,
                READ_TARGET_TEMP,
                READ_TIMER,
            ]
            # The unit can't change by itself, so read it only on the first
            # successful poll and reuse it after that. Saves one full command
            # round-trip on every subsequent update.
            if self.state is None:
                commands.append(READ_UNIT)

            # These serialize on _cmd_lock anyway, so run them in order
            # rather than pretending to gather them concurrently.
            res: list[str] = []
            for command in commands:
                res.append(await self._do_command(command))

            status = AnovaStatus(res[0])
            current_temp = float(res[1])
            target_temp = float(res[2])
            timer = (int(res[3].split()[0]), AnovaStatus(res[3].split()[1]))
            unit = AnovaTemperatureUnit(res[4]) if len(res) > 4 else self.state.unit

            self.state = AnovaState(
                status,
                current_temp,
                target_temp,
                timer,
                unit
            )

            return self.state

        except Exception as e:
            _LOGGER.debug(f"${self.name}: Failed to read data from device; MAC: {self.ble_device.address}; Error: {e}")
            raise AnovaUpdateStateException() from e
    
    async def set_timer(self, minutes: int):
        return await self._do_command(SET_TIMER.format(minutes))
    
    async def set_temp_unit(self, unit: AnovaTemperatureUnit):
        return await self._do_command(SET_TEMP_UNIT.format(unit.value))
    
    async def set_temp(self, temp: float):
        return await self._do_command(SET_TARGET_TEMP.format(temp))

    async def start(self):
        return await self._do_command(CTL_START)
    
    async def stop(self):
        return await self._do_command(CTL_STOP)
    
    async def start_timer(self):
        return await self._do_command(CTL_TIMER_START)
    
    async def stop_timer(self):
        return await self._do_command(CTL_TIMER_STOP)
            

    async def _do_command(self, cmd: str) -> str:
        await self.connect()
        if not self._client:
            raise Exception("No connected device.")
        
        cmd_bytes = cmd.encode() + ('\r'.encode())

        async with self._cmd_lock:
            # Drop any late response left over from a previous command so we
            # don't mistake it for the reply to this one.
            while not self._notify_queue.empty():
                self._notify_queue.get_nowait()

            await self._client.write_gatt_char(DEVICE_NOTIFICATION_CHAR_UUID, cmd_bytes)
            res = await asyncio.wait_for(self._notify_queue.get(), timeout=10)

        return res.decode().strip()
