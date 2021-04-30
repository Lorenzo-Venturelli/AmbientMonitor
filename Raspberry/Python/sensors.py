import time, copy, threading, logging
from interfaces import Data, Event, System
try:
    import board, adafruit_dht as DHT22, adafruit_bh1750 as BH1750, Adafruit_BMP.BMP085 as BMP185
except Exception as e:
    print("Fatal error: sensors' libraries are missing!")
    print(e)
    exit()


class Sensors(threading.Thread):
    
    _TEMP_SENSOR_PIN = board.D26

    def __init__(self, data: object, event: object, system: object, logger: object):
        if isinstance(event, Event) != True  or isinstance(data, Data) != True or isinstance(system, System) != True:
            raise TypeError

        # Interfaces
        self._data = data
        self._event = event
        self._system = system
        self._logger = logger

        # Sensors' list
        self._sensorsList = {"T&H" : DHT22.DHT22(self._TEMP_SENSOR_PIN), "P" : BMP185.BMP085(), "L" : BH1750.BH1750(board.I2C())}
        self._periodicClb = None                                                            # Periodic callback
        self._isRunning = True
        self._event.createEvent(eventName = "readSensors")                                  # Polling event
        super().__init__(daemon = False)

    
    def _executePolling(self) -> None:
        '''Periodic callback to set the polling event'''

        if self._event.isPresent(eventName = "readSensors") == False:                       # If the event has been removed, recreate it
            self._event.createEvent(eventName = "readSensors")

        self._event.post(eventName = "readSensors")                                         # Post the event to trigger the polling
        self._periodicClb = threading.Timer(interval = (self._system.settings["samplingSpeed"] * 60), function = self._executePolling)
        self._periodicClb.start()
        self._logger.debug("Sensors polling")
        return

    def stopThread(self) -> None:
        '''Stop and delete this thread'''
        self._isRunning = False                                                             # Set the flag
        self._periodicClb.cancel()                                                          # Interrupt the periodic callback
        self._event.post(eventName = "readSensors")                                         # Set the event to unlock this thread
        return

    def run(self) -> None:
        '''Periodically read sensors and prepare data'''

        self._periodicClb = threading.Timer(interval = (self._system.settings["samplingSpeed"] * 60), function = self._executePolling)
        self._periodicClb.start()
        self._logger.debug("Sensors' thread started")

        while True:
            self._event.pend(eventName = "readSensors")                                     # Wait the periodic event

            if self._isRunning == False:                                                    # If the thread is not running anymore, return
                self._logger.debug("Sensors' thread closed")
                return

            newData = dict()   
            
            try:
                # Read data from sensors
                newData["pressure"] = self._sensorsList["P"].read_pressure()
                newData["temperature"] = self._sensorsList["T&H"].temperature
                newData["humidity"] = self._sensorsList["T&H"].humidity
                newData["Ligth"] = self._sensorsList["L"].lux
            except Exception:                                                               # In case of errors abort this reading and go back waitig
                self._logger.error("Sensors read failed", exc_info = True)
                continue

            # Try to save these data. If something happen, recreate the item into Data interface
            if self._data.insertDict(itemName = "sampledData", key = int(time.time()), element = newData) == False:
                self._data.remove(itemName = "sampledData")
                self._data.store(itemName = "sampledData", item = newData, itemType = "dict")

            del newData                                                                     # Delete this temporary dict, it's useless


if __name__ == "__main__":
    print("Fatal error: This program has to be used as a module")
    exit()