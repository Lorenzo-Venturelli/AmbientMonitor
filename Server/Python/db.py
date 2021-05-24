import logging, sys, time, json, copy, asyncio
from typing import Type
from interfaces import System
try:
    import mysql.connector
except ImportError:
    print("Fatal error: Missing MySQL modules!")
    sys.exit(2)

class MySQL:

    _DEFAULT_SQL_SETTINGS = {"host_db" : "localhost", "user_db" : "dbmanager", "password_db" : "Trattoreventurelli0406!", "name_db" : "ambientmonitordb"}
    _SUPPORTED_DB_TABLES = ["Recordings", "Devices"]
    
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

    @property
    def supportedTables(self) -> list:
        '''Supported tables that can be managed by this module'''
        return copy.deepcopy(self._SUPPORTED_DB_TABLES)

    # Update SQL class settings from outside, providing a new partial or full set of settings
    async def updateSettings(self, newSettings: dict) -> bool:
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
            self.close()                        # Close the DB
            return self.open()                  # Reopen it using the new settings
        except Exception:
            self._logger.error("Error occurred while reopening the DB after a settings update")
            return False

    # Connect to DB, using the current settings
    def open(self) -> bool:
        '''Connect to DB using the current settings. Return True on success'''
        
        # Connect to DB and get the cursor
        try:
            self._handler = mysql.connector.connect(host = self._system.settings["host_db"], user = self._system.settings["user_db"], 
                password = self._system.settings["password_db"], database = self._system.settings["name_db"])
            self._cursor = self._handler.cursor()
            self._logger.debug("Connected to DB")
            self._isOpen = True
            return True
        except Exception: 
            self._logger.error("Impossible to connect to DB", exc_info = True)
            return False
    
    # Close connection to DB
    def close(self) -> None:
        '''Close the connection to DB'''

        try:
            self._cursor.close()
            self._handler.close()
            self._logger.debug("Disconnected from DB")
            self._isOpen = False
        except Exception:
            self._logger.critical("Impossible to close the DB", exc_info = True)
    
    # Create the main DB table
    async def createTable(self, tableName: str) -> bool:
        '''
        Create a table into the DB. (tableName) identifies one of the predefined tables that can be created
        Return True in case of success. If (tableName) is not recognised, the operation fails.
        If the table already exists, returns true without doing anything.
        '''

        if type(tableName) != str:
            raise TypeError

        if tableName not in self._SUPPORTED_DB_TABLES:              # The requested table is not supported by this system
            self._logger.debug("Table " + tableName + " is not supported")
            return False

        try:
            self._cursor.execute("SHOW TABLES")                     # Get the existing tables
            existingTables = self._cursor.fetchall()

            if existingTables != []:
                existingTables = existingTables[0]

            if tableName in existingTables:                             # The table already exists so there is no need to create it again
                return True
        except Exception:
            self._logger.error("Impossible to read tables from DB")
            return False

        try:
            if tableName == self._SUPPORTED_DB_TABLES[0]:           # Main table to store all the received data as they are
                self._cursor.execute("CREATE TABLE Recordings (timestamp INT NOT NULL, UID BIGINT NOT NULL, Pressure DECIMAL NOT NULL, Temperature DECIMAL NOT NULL, Humidity DECIMAL NOT NULL, Ligth DECIMAL NOT NULL, PRIMARY KEY (timestamp, UID))")
            elif tableName == self._SUPPORTED_DB_TABLES[1]:
                self._cursor.execute("CREATE TABLE Devices (UID BIGINT NOT NULL, Country VARCHAR[4] NOT NULL, City VARCHAR[255], PRIMARY KEY (UID))")
            return True
        except Exception:
            self._logger.error("Failed to create the table " + tableName)
            return False

    # Insert data into a table
    async def insertData(self, tableName: str, data : dict) -> bool:
        '''
        Insert the given (data) into the table (tableName). If the table doesn't exist, create it.
        (data) must be a dictionary with an adequate format for the desitnation table.
        Returns True on success. If (data) contains more than one line, invalid lines will be discharged.
        '''

        if type(tableName) != str or type(data) != dict:
            raise TypeError

        if tableName not in self._SUPPORTED_DB_TABLES:              # Requested table is not supported
            self._logger.debug("Table " + tableName + " is not supported")
            return False
        else:
            if await self.createTable(tableName = tableName) == False:
                return False

        if tableName == self._SUPPORTED_DB_TABLES[0]:               # Recordings table
            if "UID" not in data.keys() or "values" not in data.keys():                     # Lack of data
                return False
            if type(data["UID"]) != str or type(data["values"]) != dict:                    # Data formt is not valid
                return False
            else:                                                                           # Data format is valid
                # Write the query
                query = "INSERT INTO Recordings VALUES (%s, %s, %s, %s, %s, %s)"
                
                # For each element (a single recording from "UID" device), execute the query
                for timestamp in data["values"].keys():
                    try:
                        val = (str(timestamp), str(data["UID"]), str(data["values"][timestamp]["pressure"]), str(data["values"][timestamp]["temperature"]), str(data["values"][timestamp]["humidity"]), str(data["values"][timestamp]["Ligth"]))
                    except Exception:                                                       # Something in the data structure is wrong, this line is invalid, skip to the next one
                        continue

                    try:
                        self._cursor.execute(query,val)
                    except Exception:
                        self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                        continue

                self._handler.commit()                                                      # Commit changes to the DB to save them
                return True
        elif tableName == self._SUPPORTED_DB_TABLES[1]:
            # Write the query
            query = "INSERT INTO Devices VALUES (%s, %s, %s)"

            # For each element (a single device), execute the query
            for uid in data.keys():
                try:
                    val = (str(uid), str(data[uid]["Country"]), str(data[uid]["City"]))
                except Exception:                                                           # Something in the data structure is wrong, this line is invalid, skip to the next one
                    continue

                try:
                    self._cursor.execute(query, val)
                except Exception:
                    self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                    continue
            
            self._handler.commit()
            return True
        else:                                                                               # Table not supported
            return False

    async def readData(self, tableName: str, options: dict = {}) -> dict:
        '''
        Read data from a table. Particular constraints can be specified with (options)
        Return a dictionary containing the result.
        '''

        if type(tableName) != str or type(options) != dict:
            raise TypeError

        if tableName not in self._SUPPORTED_DB_TABLES:                                      # Requested table is not supported
            self._logger.debug("Table " + tableName + " is not supported")
            raise Exception("Table " + str(tableName) + " not supported")
        else:
            if await self.createTable(tableName = tableName) == False:                      # Try to create the table to avoid errors
                return False

        if tableName == self._SUPPORTED_DB_TABLES[0]:                                       # Recordings table
            result = dict()
            if options == dict():                                                           # No options are provided
                query = "SELECT * FROM Recordings"                                          # Let's read the entire table

                try:
                    self._cursor.execute(query)
                    res = self._cursor.fetchall()
                except Exception:
                    self._logger.warning("Error occurred while reading data from Recordings", exc_info = True)
                    return result
                
                for entry in res:
                    



            
                    