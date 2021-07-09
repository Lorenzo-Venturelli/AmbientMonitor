import threading, socket, logging, time, asyncio, json, datetime, random
from interfaces import System, Data, Event, CryptoHandler
from db import MySQL

class TcpServer(threading.Thread):

    _DEFAULT_TCP_SETTINGS = {"serverAddress" : socket.gethostname(), "serverPort" : 1234, "RSA" : 1024}
    _HANDSHAKE_REQUEST = b"199"
    _TCP_ACK_OK = b"200"
    _TCP_ACK_ERROR = b"400"
    _TCP_DEVICE_INFO_REQUEST = b"210"
    _TCP_SET_DEVICE_UID = b"220"
    _TCP_TERMINATOR = ["\r\r", b"\r\r"]

    def __init__(self, data: object, event: object, system: object, logger: object):
        if (isinstance(event, Event) != True  or isinstance(data, Data) != True 
            or isinstance(system, System) != True or isinstance(logger, logging.Logger) == False):
            raise TypeError

        self._event = event
        self._data = data
        self._system = system
        self._logger = logger
        self._db = MySQL(system = system, logger = logger)

        for item in self._DEFAULT_TCP_SETTINGS.keys():                              # If this thread's settings don't exist, create them from default ones
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TCP_SETTINGS[item]})

        self._isRunning = True

        # Asyncio objects
        self._server = None
        self._asyncLoop = None
        self._asyncApp = None
        self._periodicDBTask = None

        (self._srvPubKey, self._srvPrivKey) = CryptoHandler.generateRSA(length = self._system.settings["RSA"])
        super().__init__(daemon = False, name = "TCP Server")

    def _openServer(self) -> bool:
        '''Open a server side endpoint. If this is not possible, kill this thread'''
        try:
            self._asyncLoop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._asyncLoop)
            self._asyncApp = asyncio.start_server(self._handle_connection, self._system.settings["serverAddress"], self._system.settings["serverPort"], loop = self._asyncLoop)
            self._server = self._asyncLoop.run_until_complete(self._asyncApp)
            return True
        except Exception:
            self._logger.error("Impossible to open TCP server, error in async loop creation", exc_info = True)
            return False

    async def _handshake(self, reader: object, writer: object) -> tuple:
        '''Execute connection's handshake. Return the keys in case of success, otherwise return (None, None)'''

        try:
            writer.write(self._HANDSHAKE_REQUEST + self._TCP_TERMINATOR[1])                 # Send handshake request to the client
            await writer.drain()
            
            # Client has got the request and it's ready
            if await asyncio.wait_for(reader.readuntil(separator = self._TCP_TERMINATOR[1]), timeout = 5) == self._TCP_ACK_OK + self._TCP_TERMINATOR[1]:

                writer.write(str(self._system.settings["RSA"]).encode() + self._TCP_TERMINATOR[1])            # Send the RSA key length. This connection will use this key length
                await writer.drain()

                # Key length received and adopted
                if await asyncio.wait_for(reader.readuntil(separator = self._TCP_TERMINATOR[1]), timeout = 5) == self._TCP_ACK_OK + self._TCP_TERMINATOR[1]:
                    writer.write(CryptoHandler.exportRSApub(pubkey = self._srvPubKey) + self._TCP_TERMINATOR[1])        # Send the server's RSA public key
                    await writer.drain()

                    # Receive the AES key and use the server's private RSA key to decrypt is
                    aesKey = await asyncio.wait_for(reader.readuntil(separator = self._TCP_TERMINATOR[1]), timeout = 5)
                    aesKey = CryptoHandler.RSAdecrypt(privkey = self._srvPrivKey, secret = aesKey[0:-2], skipDecoding = True)

                    if aesKey != None:                                                      # AES key received and successfully decrypted
                        writer.write(self._TCP_ACK_OK + self._TCP_TERMINATOR[1])            # Send ACK
                        await writer.drain()

                        # Receive the client RSA public key and use the AES key to decrypt it
                        cltPubKey = await asyncio.wait_for(reader.readuntil(separator = self._TCP_TERMINATOR[1]), timeout = 5)
                        cltPubKey = CryptoHandler.AESdecrypt(key = aesKey, secret = cltPubKey[0:-2], byteObject = True)
                        
                        if cltPubKey != False:                                              # If the RSA public key is successfully received
                            cltPubKey = CryptoHandler.importRSApub(PEMfile = cltPubKey)     # Import it

                            writer.write(self._TCP_ACK_OK + self._TCP_TERMINATOR[1])        # Handshake completed successfully, send the last ACK
                            await writer.drain()

                            return (cltPubKey, aesKey)
                        else:
                            self._logger.debug("Handshake failed in step 4 for client " + str(writer.get_extra_info("peername")))
                    else:
                        self._logger.debug("Handshake failed in step 3 for client " + str(writer.get_extra_info("peername")))
                else:
                    self._logger.debug("Handshake failed in step 2 for client " + str(writer.get_extra_info("peername")))
            else:
                self._logger.debug("Handshake failed in step 1 for client " + str(writer.get_extra_info("peername")))

            # Something went wrong, handshake failed
            self._logger.warning("Handshake failed for client " + str(writer.get_extra_info("peername")))
            return (None, None)
        except asyncio.TimeoutError:                                                        # At some point, client didn't send anythingù
            self._logger.warning("Timeout connection in handshake for client " + str(writer.get_extra_info("peername")))
            return (None, None)
        except Exception:                                                                   # An error occurred, handshake failed
            self._logger.error("Error occurred during TCP handshake", exc_info = True)
            return (None, None)

    async def _send(self, data: object, aesKey: object, writer: object, byteObject: bool = False) -> None:
        if type(byteObject) != bool:
            raise TypeError

        data = CryptoHandler.AESencrypt(key = aesKey, raw = data, byteObject = byteObject) + self._TCP_TERMINATOR[1]
        writer.write(data)
        await writer.drain()
        return

    async def _recv(self, aesKey: object, reader: object, byteObject: bool = False) -> object:
        if type(byteObject) != bool:
            raise TypeError

        data = await asyncio.wait_for(reader.readuntil(separator = self._TCP_TERMINATOR[1]), timeout = 5)
        data = CryptoHandler.AESdecrypt(key = aesKey, secret = data[0:-2], byteObject = byteObject)
        return data

    async def _handle_connection(self, reader, writer) -> None:
        '''Handle a TCP client connection'''

        time.sleep(0.1)                                                                     # Small delay to let the client sync
        (cltPubKey, aesKey) = await self._handshake(reader, writer)

        if cltPubKey != None and aesKey != None:
            try:
                clientUID = await self._recv(aesKey = aesKey, reader = reader, byteObject = False)
                if len(clientUID) == 10:
                    await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                    sensorData = await self._recv(aesKey = aesKey, reader = reader, byteObject = False)
                    sensorData = json.loads(sensorData)
                    if type(sensorData) == dict:
                        # Now that we have both UID and data, let's check if there is something to do
                        if int(clientUID) == 0 or len(str(clientUID)) != 10:                                             # This device has never been configured
                            try:
                                # Request info about this device
                                await self._send(data = self._TCP_DEVICE_INFO_REQUEST, aesKey = aesKey, writer = writer, byteObject = True)

                                # Decode the info
                                try:
                                    deviceInfo = await self._recv(aesKey = aesKey, reader = reader, byteObject = False)
                                    deviceInfo = json.loads(deviceInfo)
                                except Exception:
                                    self._logger.debug("Device info not received")
                                    raise Exception

                                # Check in the DB
                                try:
                                    storedDeviceInfo = await self._db.getSensorInfo(city = deviceInfo["City"], country = deviceInfo["Country"])
                                except KeyError:
                                    self._logger.debug("Received device info are in a broken format")
                                    storedDeviceInfo = None
                                except Exception:
                                    self._logger.warning("An error occurred while getting device info")
                                    storedDeviceInfo = None
                                
                                if storedDeviceInfo == tuple():                             # This device do not exist in our system
                                    newUID = str(random.randint(1000000000, 9999999999))    # Generate a new random UID for this device
                                    try:                                                    # Record this device into our system
                                        await self._insertDataIntoDB(tableName = "Devices", data = {newUID : {"country" : deviceInfo["Country"], "city" : deviceInfo["City"]}})
                                    except Exception:                                       # We couldn't record this device so we can't go on
                                        self._logger.warning("An error occurred while recording a new device into the DB")
                                        await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                                        writer.close()
                                        await writer.wait_closed()
                                        return
                                    clientUID = newUID                                      # Data received from this device must be recorded with the new UID
                                elif storedDeviceInfo == None:                              # We couldn't get info from this device so we can't record these data, just kill the connection
                                    await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                                    writer.close()
                                    await writer.wait_closed()
                                    self._logger.debug("Connection closed with client " + str(writer.get_extra_info("peername")))
                                    return
                                else:
                                    clientUID = str(storedDeviceInfo[0])
                                
                                # Now that we have a valid UID to set, notify our intention
                                await self._send(data = self._TCP_SET_DEVICE_UID, aesKey = aesKey, writer = writer, byteObject = True)

                                # Wait an ACK from the device. If it's arrive, send the new UID
                                try:
                                    answer = await self._recv(aesKey = aesKey, reader = reader, byteObject = True)
                                except Exception:
                                    answer = None

                                if answer == self._TCP_ACK_OK:
                                    await self._send(data = clientUID, aesKey = aesKey, writer = writer, byteObject = False)

                                    # Check if everything is okay
                                    try:
                                        answer = await self._recv(aesKey = aesKey, reader = reader, byteObject = True)
                                    except Exception:
                                        answer = None

                                    if answer != self._TCP_ACK_OK:
                                        self._logger.debug("Last ACK for update UID not received")
                                        raise Exception
                                else:
                                    self._logger.debug("First ACK for update UID not received")
                                    raise Exception
                            except Exception:
                                self._logger.warning("Impossible to configure a device. Data lost")
                                await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                                writer.close()
                                await writer.wait_closed()
                                self._logger.debug("Connection closed with client " + str(writer.get_extra_info("peername")))
                                return
                        elif int(clientUID) != 0 and len(str(clientUID)) == 10:             # This client is configured, let's check if it exists in our system
                                # Check in the DB
                                try:
                                    storedDeviceInfo = await self._db.getSensorInfo(UID = int(clientUID))
                                except KeyError:
                                    self._logger.debug("Received device info are in a broken format")
                                    storedDeviceInfo = None
                                except Exception:
                                    self._logger.warning("An error occurred while getting device info")
                                    storedDeviceInfo = None

                                # If we don't have this device in our DB, request info and store it
                                if storedDeviceInfo == None:
                                    # Request info about this device
                                    await self._send(data = self._TCP_DEVICE_INFO_REQUEST, aesKey = aesKey, writer = writer, byteObject = True)

                                    # Decode the info
                                    try:
                                        deviceInfo = await self._recv(aesKey = aesKey, reader = reader, byteObject = False)
                                        deviceInfo = json.loads(deviceInfo)
                                    except Exception:
                                        self._logger.debug("Device info not received")
                                        raise Exception

                                    try:                                                    # Record this device into our system
                                        await self._insertDataIntoDB(tableName = "Devices", data = {clientUID : {"country" : deviceInfo["Country"], "city" : deviceInfo["City"]}})
                                    except Exception:                                       # We couldn't record this device so we can't go on
                                        self._logger.warning("An error occurred while recording a new device into the DB")
                                        await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                                        writer.close()
                                        await writer.wait_closed()
                                        return
                                    
                        # Create a new task in background to insert data into the DB without blocking
                        self._asyncLoop.create_task(self._insertDataIntoDB(tableName = "Recordings", data = {"UID" : clientUID, "values" : sensorData}))
                        await self._send(data = self._TCP_ACK_OK, aesKey = aesKey, writer = writer, byteObject = True)
                        self._logger.debug("Data recorded for device UID = " + str(clientUID))
                    else:
                        await self._send(data = self._TCP_ACK_ERROR, aesKey = aesKey, writer = writer, byteObject = True)
                        self._logger.warning("Sensors data are unreadable from client " + str(writer.get_extra_info("peername")))
                    
                    writer.close()
                    await writer.wait_closed()
                    self._logger.debug("Connection closed with client " + str(writer.get_extra_info("peername")))
                    return
            except asyncio.TimeoutError:                        # At some point, the client didn't responde, close the connection
                self._logger.warning("Timeout connection for client " + str(writer.get_extra_info("peername")))
                writer.close()
                await writer.wait_closed()
                return
            except Exception:                                   # Unknown error occurred, close the connection
                self._logger.error("Error occurred while talking with client " + str(writer.get_extra_info("peername")), exc_info = True)
                writer.close()
                await writer.wait_closed()
                return
        else:
            self._logger.debug("TCP handshake failed for client " + str(writer.get_extra_info("peername")))
            writer.close()
            await writer.wait_closed()
            return

    async def _insertDataIntoDB(self, tableName: str, data: dict) -> None:
        '''
        Insert the given data into the specified table. In case of error, try to rebuild the DB connection
        '''

        if type(tableName) != str or type(data) != dict:
            raise TypeError

        try:
            if await self._db.insertData(tableName = tableName, data = data) == False:          # Something went wrong with the connection
                self._logger.warning("An error occurred while inserting data into DB in table {table}".format(table = tableName))
                self._db.close()
                self._db.open()
                await self._db.insertData(tableName = tableName, data = data)                   # Try to insert these data again
        except Exception:
            self._logger.error("Unexpected error while inserting data into DB", exc_info = True)

    async def _optimizeDB(self) -> None:
        '''
        This coroutine execute once every 24 hours to optimize the DB usage by reducing the records resolution
        '''
        
        try:
            while self._isRunning == True:
                currentTime = datetime.datetime.now()                                       # Get the current time in order to understand which optimization has to be done

                if currentTime.month == 1 and currentTime.day == 1:                         # Today is 01/01
                    if await self._db.optimizeDB(tableName = "Recordings", option = {"period" : "year"}) == True:
                        self._logger.debug("Year wide optimization has been successfull")
                    else:
                        self._logger.warning("Year wide optimization failed")

                if currentTime.day == 1:                                                    # First day of the month
                    if await self._db.optimizeDB(tableName = "Recordings", option = {"period" : "month"}) == True:
                        self._logger.debug("Month wide optimization has been successfull")
                    else:
                        self._logger.warning("Month wide optimization failed")
                
                if await self._db.optimizeDB(tableName = "Recordings", option = {"period" : "day"}) == True:
                    self._logger.debug("Day wide optimization has been successfull")
                else:
                    self._logger.warning("Day wide optimization failed")

                await asyncio.sleep(delay = 86400)                                  # Sleep for 24 hours before repeating this procedure
        except asyncio.CancelledError:
            self._logger.debug("Periodic DB optimization routine terminated gracefully")
        except Exception:
            self._logger.critical("Periodic DB optimization routine terminated with errors", exc_info = True)
        return
 
    def stopThread(self) -> None:
        '''Close the server endpoint'''
        self._isRunning = False                                                     # Update the status flag

        if self._asyncLoop != None:                                                 # If the async loop already exists
            if self._periodicDBTask != None:                                        # There is the periodic DB optimization routine 
                self._asyncLoop.call_soon_threadsafe(self._periodicDBTask.cancel)

            self._asyncLoop.stop()                                                  # Stop the async loop. This won't wake up the system so let's fake a connection

            # The async loop is stopped so this connection will wake up the async ThreadPoolExecutor which will then terminate
            try:
                fakeClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                fakeClient.connect((self._system.settings["serverAddress"], self._system.settings["serverPort"]))
                fakeClient.close()
            except OSError:                                                         # Impossible to establish a connection, it means that the server is already closed
                pass
            except Exception:                                                       # Unknown exception
                self._logger.error("Something happened while trying to close the TCP server throught a fake connection")

            self._db.close()                                                        # Close the connection to DB
        return

    def run(self) -> None:
        while True:
            if self._isRunning == False:                                            # Check the thread status
                try:
                    self._server.close()                                            # Close the server coroutine
                    self._asyncLoop.run_until_complete(self._server.wait_closed())  # Wait for its termination
                except Exception:                                                   # Something went wrong
                    self._logger.critical("Error occurred while closing the TCP server async loop", exc_info = True)

                self._logger.debug("TCP Server closed")                             # Thread is dead, leave this function
                return
            else:                                                                   # Thread is alive
                if self._db.status == False:                                        # Check the DB handler status
                    if self._db.open() == False:                                    # If it was closed, try to open it
                        self._logger.critical("Fatal error: impossible to open DB") # In case of failure, the whole program is broken
                        self.stopThread()
                        continue

                if asyncio.run(self._db.createTable(tableName = "Recordings")) == False:         # Prepare the tables. In case of failure we can't go on
                    self._logger.critical("Fatal error: impossible to create the Recordings table into DB")
                    self.stopThread()
                    continue
                
                if self._openServer() == True:                                      # Try to open the async server
                    self._periodicDBTask = self._asyncLoop.create_task(self._optimizeDB())
                    self._asyncLoop.run_forever()                                   # In case of success, wait here the closure
                else:                                                               # Impossible to open the server
                    time.sleep(120)                                                 # Wait 2 minutes and then try again
                    continue
