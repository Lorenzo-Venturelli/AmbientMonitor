import threading, socket, logging, time, asyncio
from interfaces import System, Data, Event, CryptoHandler

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

        for item in self._DEFAULT_TCP_SETTINGS.keys():                              # If this thread's settings don't exist, create them from default ones
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TCP_SETTINGS[item]})

        self._isRunning = True

        (self._srvPrivKey, self._srvPubKey) = CryptoHandler.generateRSA(length = self._system.settings["RSA"])
        super().__init__(daemon = False)

    def _openServer(self) -> bool:
        '''Open a server side endpoint. If this is not possible, kill this thread'''
        try:
            self._asyncLoop = asyncio.get_event_loop()
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
            
            if await reader.read() == self._TCP_ACK_OK:                                     # Client has got the request and it's ready

                writer.write(str(self._system.settings["RSA"]).encode())                    # Send the RSA key length. This connection will use this key length
                await writer.drain()

                if await reader.read() == self._TCP_ACK_OK:                                 # Key length received and adopted
                    writer.write(CryptoHandler.exportRSApub(pubkey = self._srvPubKey))      # Send the server's RSA public key
                    await writer.drain()

                    # Receive the AES key and use the server's private RSA key to decrypt is
                    aesKey = CryptoHandler.RSAdecrypt(privkey = self._srvPrivKey, secret = await reader.read(), skipDecoding = True)

                    if aesKey != None:                                                      # AES key received and successfully decrypted
                        writer.write(self._TCP_ACK_OK)                                      # Send ACK
                        await writer.drain()

                        # Receive the client RSA public key and use the AES key to decrypt it
                        cltPubKey = CryptoHandler.AESdecrypt(key = aesKey, secret = await reader.read(), byteObject = True)
                        
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
        except Exception:                                                                   # An error occurred, handshake failed
            self._logger.error("Error occurred during TCP handshake", exc_info = True)
            return (None, None)

    async def _handle_connection(self, reader, writer) -> None:
        '''Handle a TCP client connection'''

        (cltPubKey, aesKey) = await self._handshake(reader, writer)

        if cltPubKey != None and aesKey != None:
            try:
                clientUID = CryptoHandler.AESdecrypt(key = aesKey, secret = await reader.read(), byteObject = False)
                if len(clientUID) == 10:
                    writer.write(CryptoHandler.AESencrypt(key = aesKey, raw = self._TCP_ACK_OK, byteObject = True))
                    await writer.drain()
                    sensorData = CryptoHandler.AESdecrypt(key = aesKey, secret = await reader.read(), byteObject = True)
                    if type(sensorData) == dict:
                        # passare clientUID e sensorData alla classe SQL per inserimento del DB
                        # Da introdurre loop per invio comandi prima di mandare l'ok che poi chiude la connessione
                        print("UID: " + str(clientUID))
                        print(sensorData)
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
            except Exception:
                self._logger.error("Error occurred while talking with client " + str(writer.get_extra_info("peername")), exc_info = True)
                writer.close()
                await writer.wait_closed()
                return
        else:
            self._logger.debug("TCP handshake failed for client " + str(writer.get_extra_info("peername")))
            writer.close()
            await writer.wait_closed()
            return

    def stopThread(self) -> None:
        '''Close the server endpoint'''
        self._isRunning = False
        self._asyncLoop.stop()

    def run(self) -> None:
        while True:
            if self._isRunning == False:                                            # Check the thread status
                self._logger.debug("TCP Server closed")                             # Thread is dead, leave this function
                return
            else:                                                                   # Thread is alive
                if self._openServer() == True:                                      # Try to open the async server
                    self._asyncLoop.run_forever()                                   # Server opened, start to listen
                else:                                                               # Impossible to open the server
                    time.sleep(secs = 120)                                          # Wait 2 minutes and then try again
                    continue
