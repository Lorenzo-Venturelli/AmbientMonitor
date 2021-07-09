import time, copy, threading, logging, subprocess
from interfaces import Data, Event, System
try:
    import board, adafruit_dht as DHT22, adafruit_bh1750 as BH1750, Adafruit_BMP.BMP085 as BMP185
except Exception as e:
    print("Fatal error: sensors' libraries are missing!")
    print(e)
    exit()


class Sensors(threading.Thread):
    
    _TEMP_SENSOR_PIN = board.D26
    _MAX_SENSOR_FAILURE = 5

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
        self._consecutiveFailure = 0                                                        # Keep track of consecutive sensors' reading failure
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
        self._consecutiveFailure = 0

        while True:
            self._event.pend(eventName = "readSensors")                                     # Wait the periodic event

            if self._isRunning == False:                                                    # If the thread is not running anymore, return
                self._logger.debug("Sensors' thread closed")
                return

            newData = dict()
            success = False

            while success == False:                                                         # RuntimeErrors are frequent with these sensors, keep reading until we have a success
                try:
                    # Read data from sensors
                    newData["pressure"] = int(self._sensorsList["P"].read_pressure())
                    newData["temperature"] = round(self._sensorsList["T&H"].temperature, 1)
                    newData["humidity"] = round(self._sensorsList["T&H"].humidity, 1)
                    newData["ligth"] = round(self._sensorsList["L"].lux, 3)
                    success = True
                    self._consecutiveFailure = 0                                            # This reading went well, reset this counter
                except RuntimeError:                                                        # Just a silly runtime error
                    self._logger.debug("RuntimeError while reading sensors")
                except Exception:                                                           # Unexpected error, stop this reading 
                    self._consecutiveFailure = self._consecutiveFailure + 1
                    self._logger.error("Sensors read failed for {number} times in a row".format(number = str(self._consecutiveFailure)))
                    break
            
            if self._consecutiveFailure == self._MAX_SENSOR_FAILURE:                        # Too many failure, reboot the system
                subprocess.run(["sudo", "reboot"])

            if success == False:                                                            # If the reading has been aborted, skip this iteration
                continue

            # Try to save these data. If something happen, recreate the item into Data interface
            if self._data.insertDict(itemName = "sampledData", key = int(time.time()), element = newData) == False:
                self._data.remove(itemName = "sampledData")
                self._data.store(itemName = "sampledData", item = newData, itemType = "dict")

            self._data.store(itemName = "dataReady", item = True, itemType = "bool")
            del newData                                                                     # Delete this temporary dict, it's useless


if __name__ == "__main__":
    print("Fatal error: This program has to be used as a module")
    exit()