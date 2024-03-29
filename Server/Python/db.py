import logging, sys, time, json, copy, asyncio, datetime, pytz

from interfaces import System
try:
    import mysql.connector
except ImportError:
    print("Fatal error: Missing MySQL modules!")
    sys.exit(2)

class MySQL:

    _DEFAULT_SQL_SETTINGS = {"host_db" : "localhost", "user_db" : "dbmanager", "password_db" : "Trattoreventurelli0406!", "name_db" : "ambientmonitordb"}
    _SUPPORTED_DB_TABLES = ["Recordings", "Devices", "People", "Updates"]
    
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
            self._handler.autocommit = True
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
            result = self._cursor.fetchall()

            # Format the query output to get a list of tables
            existingTables = list()
            for row in result:
                existingTables.append(row[0])                       # A row is alwais a tuple, even if there is just one element inside

            if tableName in existingTables:                         # The table already exists so there is no need to create it again
                return True
        except Exception:
            self._logger.error("Impossible to read tables from DB")
            return False

        try:
            if tableName == self._SUPPORTED_DB_TABLES[0]:           # Main table to store all the received data as they are
                self._cursor.execute('''CREATE TABLE Recordings (timestamp BIGINT UNSIGNED NOT NULL, UID BIGINT UNSIGNED NOT NULL, Pressure DECIMAL(19, 5) NOT NULL, Temperature DECIMAL(19, 5) NOT NULL, Humidity DECIMAL(19, 5) NOT NULL,
                    Ligth DECIMAL(19, 5) NOT NULL, PRIMARY KEY (timestamp, UID), FOREIGN KEY (UID) REFERENCES Devices (UID))''')
            elif tableName == self._SUPPORTED_DB_TABLES[1]:         # List of available devices
                self._cursor.execute("CREATE TABLE Devices (UID BIGINT UNSIGNED NOT NULL, Country VARCHAR(4) NOT NULL, City VARCHAR(255), PRIMARY KEY (UID))")
            elif tableName == self._SUPPORTED_DB_TABLES[2]:         # List of users
                self._cursor.execute("CREATE TABLE People (ID BIGINT UNSIGNED NOT NULL, Name VARCHAR(255) NOT NULL, Surname VARCHAR(255) NOT NULL, PRIMARY KEY (ID))")
            elif tableName == self._SUPPORTED_DB_TABLES[3]:         # Relation between a user and its devices
                self._cursor.execute("CREATE TABLE Updates (Person BIGINT UNSIGNED NOT NULL, Sensor BIGINT UNSIGNED NOT NULL, PRIMARY KEY (Person, Sensor), FOREIGN KEY (Person) REFERENCES People (ID), FOREIGN KEY (Sensor) REFERENCES Devices (UID))")
            else:
                return False                                        # Table not supported
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
                        val = (str(timestamp), str(data["UID"]), str(data["values"][timestamp]["pressure"]), str(data["values"][timestamp]["temperature"]), str(data["values"][timestamp]["humidity"]), str(data["values"][timestamp]["ligth"]))
                    except Exception:                                                       # Something in the data structure is wrong, this line is invalid, skip to the next one
                        continue

                    try:
                        self._cursor.execute(query,val)
                    except Exception:
                        self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                        continue
        elif tableName == self._SUPPORTED_DB_TABLES[1]:             # Devices table
            # Write the query
            query = "INSERT INTO Devices VALUES (%s, %s, %s)"

            # For each element (a single device), execute the query
            for uid in data.keys():
                try:
                    val = (str(uid), str(data[uid]["country"]), str(data[uid]["city"]))
                except Exception:                                                           # Something in the data structure is wrong, this line is invalid, skip to the next one
                    continue

                try:
                    self._cursor.execute(query, val)
                except Exception:
                    self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                    continue
        elif tableName == self._SUPPORTED_DB_TABLES[2]:             # People table
            # Write the query
            query = "INSERT INTO People VALUES (%s, %s, %s)"

            # For each person, execute the query
            for id in data.keys():
                try:
                    val = (str(id), str(data[id]["name"]), str(data[id]["surname"]))
                except Exception:                                                           # Something in the data structure is wrong, this line is invalid, skip to the next one
                    continue
            
                try:
                    self._cursor.execute(query, val)
                except Exception:
                    self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                    continue
        elif tableName == self._SUPPORTED_DB_TABLES[3]:             # Updates table
            # Write the query
            query = "INSERT INTO Updates VALUES (%s, %s)"

            for entry in data.keys():
                try:
                    val = (str(data[entry]["person"]), str(data[entry]["sensor"]))
                except Exception:                                                           # Something in the data structure is wrong, this line is invalid, skip to the next one
                    continue
            
                try:
                    self._cursor.execute(query, val)
                except Exception:
                    self._logger.warning("Impossible to execute the query: Insert into " + tableName, exc_info = True)
                    continue
        else:                                                                               # Table not supported
            return False

        try:
            self._handler.commit()                                                              # Commit changes to the DB to save them
            return True
        except Exception:
            self._logger.warning("Impossible to commit an INSERT operation to DB regarding table: " + tableName)
            return False

    async def readData(self, tableName: str, options: dict = {}) -> list:
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
                raise Exception("Impossible to create the table, DB error")

        result = list()
        if tableName == self._SUPPORTED_DB_TABLES[0]:                                       # Recordings table
            query = "SELECT * FROM Recordings"                                              # In principle, we'll read the entire table
            if options != dict():                                                           # There are options
                query = query + " WHERE "                                                   # Add contidional statement
                queryLen = len(query)

                if "firstTime" in options.keys():                                           # It is specified the initial timestamp, read records that are younger
                    if type(options["firstTime"]) != int:
                        raise TypeError
                    if queryLen < len(query):
                        query = query + " AND "

                    query = query + "timestamp >= " + str(options["firstTime"])
                
                if "lastTime" in options.keys():                                            # It is specified the final timestamp, read records that are older
                    if type(options["lastTime"]) != int:
                        raise TypeError
                    if queryLen < len(query):
                        query = query + " AND "

                    query = query + "timestamp <= " + str(options["lastTime"])

                if "uidList" in options.keys():                                             # List of UIDs to request
                    if type(options["uidList"]) != list:
                        raise TypeError
                    if queryLen < len(query):
                        query = query + " AND ("
                    else:
                        query = query + "("

                    queryLen = len(query)
                    for uid in options["uidList"]:                                          # For each UID, update the query
                        if type(uid) != int:
                            raise TypeError

                        if queryLen < len(query):
                            query = query + " OR "
                        
                        query = query + "UID = " + str(uid)
                    
                    query = query + ")"

                if len(query) == queryLen:                                                  # No valid option detected, revert to the original query
                    query = "SELECT * FROM Devices"
        elif tableName == self._SUPPORTED_DB_TABLES[1]:                                     # Devices table
            query = "SELECT * FROM Devices"
            
            if options != dict():                       
                query = query + " WHERE "                                                   # Add contidional statement
                queryLen = len(query)

                if "uidList" in options.keys():                                             # Read only specified UIDs
                    if type(options["uidList"]) != list:
                        raise TypeError

                    if len(options["uidList"]) > 0:                                         # There is something here
                        if queryLen < len(query):
                            query = query + " AND "

                        query = query + "UID IN ("

                    for uid in options["uidList"]:                                          # For each UID, update the query
                        if type(uid) != int:
                            raise TypeError

                        query = query + "{uid},".format(uid = str(uid))
                    
                    query = query[0:-1] + ")"                                               # Substitute the last comma with a bracket
                
                if "cityList" in options.keys():                                            # Devices from the specified cities
                    if type(options["cityList"]) != list:
                        raise TypeError

                    if len(options["cityList"]) > 0:                                        # There is something here
                        if queryLen < len(query):
                            query = query + " AND "

                        query = query + "City IN ("

                    for city in options["cityList"]:                                        # For each City, update the query
                        if type(city) != str:
                            raise TypeError

                        query = query + ''''{city}','''.format(city = str(city))
                    
                    query = query[0:-1] + ")"                                               # Substitute the last comma with a bracket

                if "countryList" in options.keys():                                         # Devices from the specified countries
                    if type(options["countryList"]) != list:
                        raise TypeError

                    if len(options["countryList"]) > 0:                                     # There is something here
                        if queryLen < len(query):
                            query = query + " AND "

                        query = query + "Country IN ("

                    for country in options["countryList"]:                                  # For each Country, update the query
                        if type(country) != str:
                            raise TypeError

                        query = query + ''''{country}','''.format(country = str(country))
                    
                    query = query[0:-1] + ")"                                               # Substitute the last comma with a bracket

                if len(query) == queryLen:                                                  # No valid option detected, revert to the original query
                    query = "SELECT * FROM Devices"
        elif tableName == self._SUPPORTED_DB_TABLES[2]:                                     # People table 
            query = "SELECT * FROM People"

            if options != dict():
                if "id" in options.keys():
                    if type(options["id"]) != int:
                        raise TypeError

                    query = query + " WHERE ID = " + str(options["id"])
        elif tableName == self._SUPPORTED_DB_TABLES[3]:                                     # Updates table
            query = "SELECT * FROM Updates"

            if options != dict():
                query = query + " WHERE "
                queryLen = len(query)

                if "person" in options.keys():
                    if type(options["person"]) != int:
                        raise TypeError

                    query = query + "Person = " + str(options["person"])

                if "sensor" in options.keys():
                    if type(options["sensor"]) != int:
                        raise TypeError

                    if queryLen < len(query):
                        query = query + " AND "

                    query = query + "Sensor = " + str(options["sensor"])

                if len(query) == queryLen:
                    query = "SELECT * FROM Updates"
        else:                                                                               # Unsupported table
            self._logger.warning("Table " + str(tableName) + " is not supported")
            raise Exception("Table " + str(tableName) + " not supported")
        
        try:                                                                                # Execute the query and collect the results
            self._cursor.execute(query)
            result = self._cursor.fetchall()
            return result
        except Exception:
            self._logger.warning("Error occurred while reading data from Recordings", exc_info = True)
            return result

    async def removeData(self, tableName: str, options: dict = dict()) -> bool:
        '''
        Delete records from the specified table according to the given options.
        Each table has its own options, the caller must know them!
        In case of success returns True, otherwhise False.
        '''

        if type(tableName) != str or type(options) != dict:
            raise TypeError

        if tableName == self._SUPPORTED_DB_TABLES[0]:                                       # Recordings
            query = "DELETE FROM Recordings"                                                # By default, it drops the whole table
            conditions = 0

            if "UID" in options.keys():                                                     # Delete all records with this specified UID
                if type(options["UID"]) != int:
                    raise TypeError

                query = query + " WHERE UID = " + str(options["UID"])
                conditions = conditions + 1
            
            if "firstTime" in options.keys():                                               # Only with timestamp bigger than this
                if type(options["firstTime"]) != int:
                    raise TypeError

                if conditions > 0:
                    query = query + " AND ("
                else:
                    query = query + " WHERE "

                query = query + "timestamp >= " + str(options["firstTime"])
                conditions = conditions + 1
            
            if "lastTime" in options.keys():                                                # Only with timestamp smaller than this
                if type(options["lastTime"]) != int:
                    raise TypeError

                if conditions > 0:
                    query = query + " AND "
                else:
                    query = query + " WHERE "

                query = query + "timestamp <= " + str(options["lastTime"])

                if conditions > 1:
                    query = query + ")"
        elif tableName == self._SUPPORTED_DB_TABLES[1]:                                     # Devices 
            query = "DELETE FROM Devices"

            if "UID" in options.keys():
                if type(options["UID"]) != int:
                    raise TypeError

                query = query + " WHERE UID = " + str(options["UID"])
        elif tableName == self._SUPPORTED_DB_TABLES[2]:                                     # People
            query = "DELETE FROM People"

            if "id" in options.keys():
                if type(options["id"]) != int:
                    raise TypeError

                query = query + " WHERE ID = " + str(options["id"])
        elif tableName == self._SUPPORTED_DB_TABLES[3]:                                     # Updates
            query = "DELETE FROM Updates"

            if options != dict():
                query = query + " WHERE "
                queryLen = len(query)

            if "person" in options.keys():
                if type(options["person"]) != int:
                    raise TypeError

                query = query + "Person = " + str(options["person"])

            if "sensor" in options.keys():
                if type(options["sensor"]) != int:
                    raise TypeError

                if queryLen < len(query):
                    query = query + " AND "
                
                query = query + "Sensor = " + str(options["sensor"])

            if len(query) == queryLen:
                query = "DELETE FROM Updates"
        else:                                                                               # Table not supported
            self._logger.warning("Table " + str(tableName) + " is not supported")
            raise Exception("Table " + str(tableName) + " not supported")

        try:
            self._cursor.execute(query)
            self._handler.commit()                                                          # Commit changes to the DB to save them
            return True
        except Exception:
            self._logger.warning("Error occurred while reading data from Recordings", exc_info = True)
            return False
    
    def _recordsByUID(self, uid: str, rawRecords: list) -> tuple:
        '''
        Generator which returns a single record (tuple) that matches the given UID
        '''

        if type(uid) != int or type(rawRecords) != list:
            raise TypeError

        try:
            for record in rawRecords:
                if uid == int(record[1]):
                    yield record
        except Exception as e:
            self._logger.error("Records list passed to generator has a wrong structure")
            raise e

    async def optimizeDB(self, tableName: str, option: dict) -> bool:
        '''
        Reduce the size of the given table by summarizing its records according to the given options.
        Each table have its own constraints. The user of this method must know them.
        If one or more constraints are violated, this method returns False.
        '''

        if type(tableName) != str or type(option) != dict:
            raise TypeError

        if tableName == self._SUPPORTED_DB_TABLES[0]:                                       # Reocrding table, optimization supported

            # The final time is the current hour at its beginnig
            currentTime = datetime.datetime.now()
            finalTime = datetime.datetime(year = currentTime.year, month = currentTime.month, day = currentTime.day, hour = currentTime.hour)

            if "period" in option.keys():                                                   # It's specified the period that has to be covered by this action
                
                if option["period"] == "day":                                               # We have to optimize the last 24 hours
                    initialTime = finalTime + datetime.timedelta(hours = -24)               # The range is in the last 24 hours
                elif option["period"] == "month":                                           # We have to optimize the last month
                    finalTime = finalTime + datetime.timedelta(hours = -24)                 # We have to let the last 24 hours unchanged
                    initialTime = finalTime + datetime.timedelta(days = -30)                # Standard 30 days period
                elif option["period"] == "year":                                            # We have to optimize the last year
                    finalTime = finalTime + datetime.timedelta(days = -30)                  # We have to leave the last 30 days unchanged
                    initialTime = finalTime + datetime.timedelta(days = -365)               # Standard 365 days period
                else:                                                                       # The given period is not supported, assume "day"
                    option["period"] = "day"
                    initialTime = finalTime + datetime.timedelta(hours = -24)
            else:                                                                           # No period is specified, let's assume "day"
                option["period"] = "day"
                initialTime = finalTime + datetime.timedelta(hours = -24)

            # We need the timestamp to manage records
            finalTime = int(finalTime.timestamp())
            initialTime = int(initialTime.timestamp())

            try:
                # Get the records that will be optimized
                rawRecords = await self.readData(tableName = tableName, options = {"firstTime" : initialTime, "lastTime" : finalTime})
            except Exception:
                self._logger.warning("Error occurred while fatching raw record for a DB optimization")
                return False

            # Get a list of UIDs that will  be processed
            uids = list()
            for record in rawRecords:
                uids.append(int(record[1]))
            
            # Delete duplicates
            uids = set(uids)

            # List of new records that will substitute the raw ones
            avgRecords = dict()

            # For each UID in the set, get and ordered sequence of records and build the compressed output
            try:
                for uid in uids:
                    tmpRecord = [0, uid, 0, 0, 0, 0]                                    # Let's create a new empty record that will store the computed average
                    iterations = 0                                                      # Number of records that will be substituted by this new one
                    avgRecords["UID"] = str(uid)
                    avgRecords["values"] = dict()
                    
                    # Let's analize each record, according to the period
                    for record in sorted(self._recordsByUID(uid = uid, rawRecords = rawRecords)):
                        
                        if tmpRecord[0] == 0:                                           # If this is the first iteration for this new summarized record
                            # From the first record in this block calculate the timestamp (always consider minutes and seconds equal to 0)
                            newBlock = datetime.datetime.fromtimestamp(record[0])
                            newBlock = datetime.datetime(year = newBlock.year, month = newBlock.month, day = newBlock.day, hour = newBlock.hour)
                            tmpRecord[0] = int(newBlock.timestamp())

                        # Depending on the period, check if this record is still within the current block. If yes, use it to calculate the average
                        if ((option["period"] == "day" and (record[0] - tmpRecord[0]) < 3600) or (option["period"] == "month" and (record[0] - tmpRecord[0]) < 14400) or
                            (option["period"] == "year" and (record[0] - tmpRecord[0]) < 43200)):
                            
                            for element in (2, 3, 4, 5):
                                tmpRecord[element] = tmpRecord[element] + record[element]

                            iterations = iterations + 1
                        else:                                                           # This record is outside of the current block
                            for element in (2, 3, 4, 5):
                                tmpRecord[element] = tmpRecord[element] / iterations    # Calculate the average for each value
                            
                            # Now that we have calculated a summarized record associated to this block, let's save it in a valid format
                            avgRecords["values"][str(tmpRecord[0])] = {"pressure" : tmpRecord[2], "temperature" : tmpRecord[3], "humidity" : tmpRecord[4], "ligth" : tmpRecord[5]}
                            
                            # Initialize a new block
                            newBlock = datetime.datetime.fromtimestamp(record[0])
                            newBlock = datetime.datetime(year = newBlock.year, month = newBlock.month, day = newBlock.day, hour = newBlock.hour)
                            tmpRecord[0] = int(newBlock.timestamp())
                            for element in (2, 3, 4, 5):
                                tmpRecord[element] = record[element]
                            iterations = 1

                    if iterations != 0:
                        # Also for the last block, even if it's not complete, store it
                        for element in (2, 3, 4, 5):
                            tmpRecord[element] = tmpRecord[element] / iterations    # Calculate the average for each value
                        
                        # Now that we have calculated a summarized record associated to this block, let's save it in a valid format
                        avgRecords["values"][str(tmpRecord[0])] = {"pressure" : tmpRecord[2], "temperature" : tmpRecord[3], "humidity" : tmpRecord[4], "ligth" : tmpRecord[5]}
                        

                    # Now we have processed every block in the given range for this UID so it's time to remove the old records and insert the new ones
                    try:
                        if await self.removeData(tableName = tableName, options = {"UID" : uid, "firstTime" : initialTime, "lastTime" : finalTime}) == True:
                            if await self.insertData(tableName = tableName, data = avgRecords) != True:
                                raise Exception("Impossible to insert the new data, the processed blocks are lost!")
                        else:
                            raise Exception("Impossible to remove the raw records. Nothing has been lost")
                    except Exception:
                        self._logger.error("An error occurred while updating the DB with optimized data", exc_info = True)
                        return False
                
                # Every UID within the time window has been processed and the summarized records took the place of raw one, all done
                return True
            except Exception:                                                           # Something went wrong so in order not to lose anything, abort the whole operation
                self._logger.error("An error occurred while optimizing the fetched records", exc_info = True)
                return False
        else:                                                                           # This table doesn't exixt or it doesn't support optimization
            return False

    async def getUserList(self) -> dict:
        '''
        Wrapper to read the People table and format data into a convenient dictionary
        '''

        # Read the DB table and get a list of tuple
        try:
            rawData = await self.readData(tableName = self._SUPPORTED_DB_TABLES[2])
        except Exception:
            self._logger.error("An error occurred while reading the People table from getUserList", exc_info = True)

        # Create the dictionary that will store the formatted data
        formattedData = dict()

        # For each user, store the info. In case of error on a single user, just ignore it
        for user in rawData:
            try:
                formattedData[user[0]] = dict()
                formattedData[user[0]]["Name"] = user[1]
                formattedData[user[0]]["Surname"] = user[2]
            except IndexError:
                continue
            except Exception:
                self._logger.warning("An unknown error occurred while formatting users fetched from People table")
                continue

        return formattedData
        
    async def getSensorsByUser(self, ID: int) -> tuple:
        '''
        Get a tuple of sensors' UID associated with a single user ID
        '''

        if type(ID) != int:
            raise TypeError

        # Get the tuple directly from DB
        query = "SELECT Sensor FROM Updates WHERE Person = " + str(ID)

        # Execute the query and fetch the result
        try:
            self._cursor.execute(query)
            updates = self._cursor.fetchall()
        except Exception:
            self._logger.warning("Error occurred while reading data from Recordings", exc_info = True)
            return tuple()

        # Get the list of supported cities
        try:
            cityList = await self.getCityList()
        except Exception:
            self._logger.warning("Error occurred while obtaining the city list", exc_info = True)
            return tuple()

        # Format the output to return a tuple of sensors UID
        sensors = set()
        for city in cityList:
            try:
                if int(city[2]) in list(map(lambda update: update[0], updates)):
                    sensors.add(city)
            except Exception:
                self._logger.warning("Impossible to format a row in getSensorByUser")
                continue

        return tuple(sensors)

    async def getUpdateByCity(self, cities: tuple, options: dict = None) -> dict:
        '''
        Get a structured packet of records. Select the most recent record for each city in the list
        '''

        if type(cities) != tuple and (options != None and type(options) != dict):
            raise TypeError

        # An empty tuple is meaningless, let's return immediately
        if cities == tuple():
            return dict()

        uids = set()

        # Get UIDs from the list of cities
        for city in cities:
            uids.add(city[2])

        uids = tuple(uids)

        # To avoid errors with the query, if there is only one city, let's fix the format
        if len(uids) == 1:
            uids = "({})".format(uids[0])

        # The result will be stored here
        formattedData = dict()

        if options != None:
            if "mode" not in options.keys():
                raise Exception("Invalid options")

            try:
                if "timezone" not in options.keys():
                    tz = pytz.timezone("UTC")
                else:
                    tz = pytz.timezone(options["timezone"])
            except Exception:
                self._logger.warning("Invalid timezone provided to fetch data")
                raise Exception("Invalid timezone")

        # Create the query
        if options == None or options["mode"] == "auto":
            query = "SELECT * FROM {table} R1 WHERE R1.timestamp = (SELECT MAX(R2.timestamp) FROM {table} R2 WHERE R2.UID = R1.UID) AND R1.UID IN {UID}"
            query = query.format(table = self._SUPPORTED_DB_TABLES[0], UID = str(uids))
        elif options["mode"] == "daily":
            query = "SELECT * FROM {table} R1 WHERE R1.timestamp <= {maxTime} AND R1.timestamp >= {minTime} AND R1.UID in {UID}"
            maxTime = datetime.datetime.utcnow()
            maxTime = datetime.datetime(year = maxTime.year, month = maxTime.month, day = maxTime.day, hour = 0, minute = 0, second = 0)
            maxTime = tz.localize(maxTime)
            minTime = maxTime + datetime.timedelta(days = -1)
            maxTime = int(maxTime.timestamp())
            minTime = int(minTime.timestamp())
            query = query.format(table = self._SUPPORTED_DB_TABLES[0], UID = uids, maxTime = maxTime, minTime = minTime)
        elif options["mode"] == "weekly":
            query = "SELECT * FROM {table} R1 WHERE R1.timestamp <= {maxTime} AND R1.timestamp >= {minTime} AND R1.UID in {UID}"
            maxTime = datetime.datetime.utcnow()
            maxTime = datetime.datetime(year = maxTime.year, month = maxTime.month, day = maxTime.day, hour = 0, minute = 0, second = 0)
            maxTime = tz.localize(maxTime)
            minTime = maxTime + datetime.timedelta(weeks = -1)
            maxTime = int(maxTime.timestamp())
            minTime = int(minTime.timestamp())
            query = query.format(table = self._SUPPORTED_DB_TABLES[0], UID = uids, maxTime = maxTime, minTime = minTime)
        elif options["mode"] == "montly":
            query = "SELECT * FROM {table} R1 WHERE R1.timestamp <= {maxTime} AND R1.timestamp >= {minTime} AND R1.UID in {UID}"
            maxTime = datetime.datetime.utcnow()
            maxTime = datetime.datetime(year = maxTime.year, month = maxTime.month, day = maxTime.day, hour = 0, minute = 0, second = 0)
            maxTime = tz.localize(maxTime)
            minTime = maxTime + datetime.timedelta(days = -30)
            maxTime = int(maxTime.timestamp())
            minTime = int(minTime.timestamp())
            query = query.format(table = self._SUPPORTED_DB_TABLES[0], UID = uids, maxTime = maxTime, minTime = minTime)
        elif options["mode"] == "yearly":
            query = "SELECT * FROM {table} R1 WHERE R1.timestamp <= {maxTime} AND R1.timestamp >= {minTime} AND R1.UID in {UID}"
            maxTime = datetime.datetime.utcnow()
            maxTime = datetime.datetime(year = maxTime.year, month = maxTime.month, day = maxTime.day, hour = 0, minute = 0, second = 0)
            maxTime = tz.localize(maxTime)
            minTime = maxTime + datetime.timedelta(days = -365)
            maxTime = int(maxTime.timestamp())
            minTime = int(minTime.timestamp())
            query = query.format(table = self._SUPPORTED_DB_TABLES[0], UID = uids, maxTime = maxTime, minTime = minTime)
        else:
            self._logger.debug("Invalid mode for getUpdateByCity")
            raise Exception("Invalid mode")


        # For each city, request the data
        try:
            self._cursor.execute(query)
            result = self._cursor.fetchall()
        except Exception:
            self._logger.warning("Error occurred while fetching recordings by city", exc_info = True)
            return dict()

        # Format the fetched records
        for record in result:
            try:
                formattedData[(record[0], record[1])] = {"Pressure" : record[2], "Temperature" : record[3], "Humidity" : record[4], "Ligth" : record[5]}
            except Exception:
                self._logger.warning("Error: Impossible to format a record fetched by city")
                continue

        return formattedData

    async def getUserInfo(self, userID: int) -> tuple:
        '''
        Get a structured info pack about a single user
        '''

        if type(userID) != int:
            raise TypeError

        # Get the raw data
        raw = await self.readData(tableName = "People", options = {"id" : userID})

        if raw == []:
            return tuple()
        else:
            return raw[0]

    async def getSensorInfo(self, UID: int = None, city: str = None, country: str = None) -> tuple:
        '''
        Get info about the requested device if it exists in our database
        '''

        if (UID != None and type(UID) != int) or (city != None and type(city) != str) or (country != None and type(country) != str):
            raise TypeError

        # Build the options set
        options = dict()
        if UID != None:
            options["uidList"] = [UID]
        
        if city != None:
            options["cityList"] = [city]

        if country != None:
            options["countryList"] = [country]

        # Get the raw data
        try:
            raw = await self.readData(tableName = "Devices", options = options)
        except Exception as e:
            print(e)
            raw = list()

        if raw == []:
            return tuple()
        else:
            return raw[0]

    async def getCityList(self) -> tuple:
        '''
        Get a list (tuple) of supported city. Every entry is in the form (city, country)
        '''

        # Get a raw list of sensors, each represent a city
        try:
            raw = list()
            raw = await self.readData(tableName = "Devices")
        except Exception:
            self._logger.warning("Impossible to get raw data of sensors's list")
            raw = list()

        # Prepare the formatted output
        formattedData = list()

        # Unless the list is empty, format it
        for sensor in raw:
            try:
                formattedData.append((str(sensor[2]), str(sensor[1]), int(sensor[0])))
            except IndexError:
                self._logger.debug("While formatting a city list, a sensor row raised an IndexError")
                continue
            except Exception:
                self._logger.error("Unexpected error occurred while formatting a city list")
                formattedData = list()
                break

        return tuple(formattedData)
        
    async def getUpdateListByUser(self, userID: int) -> tuple:
        '''
        Return a tuple of cities containing the sensors associated with the provided userID
        '''

        if type(userID) != int:
            raise TypeError

        try:
            rawUpdates, rawCities = list(), list()
            rawUpdates = await self.readData(tableName = "Updates", options = {"person" : userID})
            rawCities = await self.getCityList()
        except Exception:
            self._logger.warning("Impossible to get raw data of sensors's list")
            rawUpdates = list()
            rawCities = list()

        # Prepare the data structure
        existingUpdates = set()

        # Format the output
        for city in rawCities:                                                                  # For each city in our database
            try:
                if city[2] in list(map(lambda update: update[1], rawUpdates)):                  # Check if it's associated to an update for this user
                    existingUpdates.add(city)
            except IndexError:
                self._logger.debug("While formatting a city list, a sensor row raised an IndexError")
                continue
            except Exception:
                self._logger.error("Unexpected error occurred while formatting a city list")
                existingUpdates = list()
                break

        return tuple(existingUpdates)
