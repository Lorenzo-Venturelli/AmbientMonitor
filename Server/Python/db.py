import logging, sys, time, json
import System from interfaces
try:
    import mysql.connector
except ImportError:
    print("Fatal error: Missing MySQL modules!")
    sys.exit(2)

class MySQL:

    _DEFAULT_SQL_SETTINGS = {"host_db" : "localhost", "user_db" : "dbmanager", "password_db" : "Trattoreventurelli0406", "name_db" : "ambientmonitordb"}
    
    def __init__(self, system: object, logger : object):
        
        if isinstance(system, System) != True or isinstance(logger, logging.Logger) != True:
            raise TypeError

        # Interfaces
        self._logger = logger
        self._system = system

        # Load these settings into the System interface
        for item in self._DEFAULT_SQL_SETTINGS.keys():
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item: self._DEFAULT_SQL_SETTINGS[item]})

        # SQL objects
        self._handler = None
        self._cursor = None
        self._isOpen = False

    @property
    def status(self) -> bool:
        '''DB Status'''
        return self._isOpen

    # Update SQL class settings from outside, providing a new partial or full set of settings
    def updateSettings(self, newSettings: dict) -> bool:
        '''
        Update SQL settings. This will close and re-open the DB interface
        Return True in case of successfull re-opening, False otherwise.
        '''

        if type(newSettings) != bool:
            raise TypeError

        # Before updating the SQL settings, remove those are not acceptable
        for item in newSettings.keys():
            if item not in self._DEFAULT_SQL_SETTINGS.keys():
                del newSettings[item]

        self._system.updateSettings(newSettings = newSettings)
        self._logger.debug("DB settings updated")

        try:
            self._close()                       # Close the DB
            return self._connect()              # Reopen it using the new settings
        except Exception:
            self._logger.error("Error occurred while reopening the DB after a settings update")
            return False

    # Connect to B, using the current settings
    def _connect(self) -> bool:
        '''Connect to DB using the current settings. Return True on success'''
        
        # Connect to DB and get the cursor
        try:
            self._handler = mysql.connector.connect(host = self._system.settings["host_db"], user = self._system.settings["user_db"], 
                password = self._system.settings["password_db"], database = self._system.settings["name_db"])
            self._cursor = self._handler.cursor()
            self._logger.debug("Connected to DB")
            return True
        except Exception: 
            self._logger.error("Impossible to connect to DB", exc_info = True)
            return False
    
    # Close connection to DB
    def _close(self) -> None:
        '''Close the connection to DB'''

        try:
            self._cursor.close()
            self._handler.close()
            self._logger.debug("Disconnected from DB")
        except Exception:
            self._logger.critical("Impossible to close the DB", exc_info = True)
    
    # Create the main DB table
    def _createTable(self) -> bool:
        try:
            self._cursor.execute("CREATE TABLE Recordings (time stamp INT PRIMARY KEY, UID INT PRIMARY KEY, Pression INT, Temperature INT, Humidity INT, Ligth INT)")
            return True
        except Exception:
            print("Failed to create table")
            return False

    def _insertData(self, UID: str, data : dict) -> bool:
        try:
            for key in data.keys():
                query = "INSERT INTO Recordings (time stamp, UID, Pression, Temperature, Humidity, Light) VALUES (%s %s %s %s %s %s)"
                val = (str(key), UID, str(data[key][0]), str(data[key][1]), str(data[key][2]), str(data[key][3]))                
                self._cursor.execute(query,val)
            self._handler.commit()
            return True
        except Exception:
            print("Failed to insert data into DB")
            return False