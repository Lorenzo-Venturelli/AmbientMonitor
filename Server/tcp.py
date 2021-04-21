import threading, socket, logging, time, asyncio
from interfaces import System, Data, Event, CryptoHandler

class TcpServer(threading.Thread):

    _DEFAULT_TCP_SETTINGS = {"serverAddress" : socket.gethostname, "serverPort" : 1234, "RSA" : 1024}

    def __init__(self, data: object, event: object, system: object, logger: object):
        if (isinstance(event, Event) != True  or isinstance(data, Data) != True 
            or isinstance(system, System) != True or isinstance(logger, logging.Logger) == False):
            raise TypeError

        self._event = event
        self._data = data
        self._system = system
        self._logger = logger

        self._handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)           # Create the socket endpoint

        for item in self._DEFAULT_TCP_SETTINGS.keys():                              # If this thread's settings don't exist, create them from default ones
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TCP_SETTINGS[item]})

        self._isRunning = True
        super(daemon = False)

    def _openServer(self) -> bool:
        '''Open a server side endpoint. If this is not possible, kill this thread'''
        try:
            self._handler.bind((self._system.settings["serverAddress"], self._system.settings["serverPort"]))
            self._handler.listen(10)
            return True
        except Exception:
            self._logger.critical("Impossible to open a TCP server")
            self._isRunning = False
            return False

    def stopThread(self) -> None:
        '''Close the server endpoint'''
        self._isRunning = False
        self._handler.close()

    def run(self) -> None:
        while self._isRunning == True:
            
            