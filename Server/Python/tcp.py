import threading, socket, logging, time, asyncio, json, datetime
from interfaces import System, Data, Event, CryptoHandler
from db import MySQL

class TcpServer(threading.Thread):

    _DEFAULT_TCP_SETTINGS = {"serverAddress" : socket.gethostname(), "serverPort" : 1234, "RSA" : 1024}
    _HANDSHAKE_REQUEST = b"199"
    _TCP_ACK_OK = b"200"
    _TCP_ACK_ERROR = b"400"

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
            writer.write(self._HANDSHAKE_REQUEST)                                           # Send handshake request to the client
            await writer.drain()
            
            if await asyncio.wait_for(reader.read(1024), timeout = 5) == self._TCP_ACK_OK:  # Client has got the request and it's ready

                writer.write(str(self._system.settings["RSA"]).encode())                    # Send the RSA key length. This connection will use this key length
                await writer.drain()

                if await asyncio.wait_for(reader.read(1024), timeout = 5) == self._TCP_ACK_OK:  # Key length received and adopted
                    writer.write(CryptoHandler.exportRSApub(pubkey = self._srvPubKey))          # Send the server's RSA public key
                    await writer.drain()

                    # Receive the AES key and use the server's private RSA key to decrypt is
                    aesKey = CryptoHandler.RSAdecrypt(privkey = self._srvPrivKey, secret = await asyncio.wait_for(reader.read(1024), timeout = 5), skipDecoding = True)

                    if aesKey != None:                                                      # AES key received and successfully decrypted
                        writer.write(self._TCP_ACK_OK)                                      # Send ACK
                        await writer.drain()

                        # Receive the client RSA public key and use the AES key to decrypt it
                        cltPubKey = CryptoHandler.AESdecrypt(key = aesKey, secret = await asyncio.wait_for(reader.read(1024), timeout = 5), byteObject = True)
                        
                        if cltPubKey != None:                                               # If the RSA public key is successfully received
                            cltPubKey = CryptoHandler.importRSApub(PEMfile = cltPubKey)     # Import it

                            writer.write(self._TCP_ACK_OK)                                  # Handshake completed successfully, send the last ACK
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

    async def _handle_connection(self, reader, writer) -> None:
        '''Handle a TCP client connection'''

        time.sleep(0.1)                                                                     # Small delay to let the client sync
        (cltPubKey, aesKey) = await self._handshake(reader, writer)

        if cltPubKey != None and aesKey != None:
            try:
                clientUID = CryptoHandler.AESdecrypt(key = aesKey, secret = await asyncio.wait_for(reader.read(1024), timeout = 5), byteObject = False)
                if len(clientUID) == 10:
                    writer.write(CryptoHandler.AESencrypt(key = aesKey, raw = self._TCP_ACK_OK, byteObject = True))
                    await writer.drain()
                    sensorData = CryptoHandler.AESdecrypt(key = aesKey, secret = await asyncio.wait_for(reader.read(1024), timeout = 5), byteObject = True)
                    sensorData = json.loads(sensorData)
                    if type(sensorData) == dict:
                        # Da introdurre loop per invio comandi prima di mandare l'ok che poi chiude la connessione
                        # Create a new task in background to insert data into the DB without blocking
                        self._asyncLoop.create_task(self._db.insertData(tableName = "Recordings", data = {"UID" : clientUID, "values" : sensorData}))
                        writer.write(CryptoHandler.AESencrypt(key = aesKey, raw = self._TCP_ACK_OK, byteObject = True))
                        await writer.drain()
                    else:
                        writer.write(CryptoHandler.AESencrypt(key = aesKey, raw = self._TCP_ACK_ERROR, byteObject = True))
                        await writer.drain()
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
                self._periodicDBTask.cancel()                                       # Close it

            self._asyncLoop.stop()                                                  # Stop the async loop. This won't wake up the system so let's fake a connection

            # The async loop is stopped so this connection will wake up the async ThreadPoolExecutor which will then terminate
            fakeClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            fakeClient.connect((self._system.settings["serverAddress"], self._system.settings["serverPort"]))
            fakeClient.close()

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
