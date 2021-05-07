import socket, threading, logging, json
from interfaces import Data, Event, CryptoHandler, System

class TcpClient(threading.Thread):

    _DEFAULT_TCP_SETTINGS = {"serverAddress" : "ssh.ambientmonitor.page", "serverPort" : 1234, "RSA" : 1024}
    _HANDSHAKE_REQUEST = b"199"
    _TCP_ACK_OK = b"200"
    _TCP_ACK_ERROR = b"400"

    def __init__(self,  data: object, event: object, system: object, logger: object):
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

        self._connected = False                                                     # This client is connected to the server
        self._isRunning = True                                                      # This thread is alive
        self._srvPubKey = None                                                      # Server's RSA public key
        self._cltPubKey = None                                                      # This client's RSA public key
        self._cltPrivKey = None                                                     # This client's RSA private key
        self._aesKey = None                                                         # AES key
        
        self._event.createEvent(eventName = "sendData")                             # Periodic TCP event to send data
        super().__init__(daemon = False)
    
    def _connect(self) -> bool:
        '''Connect to the server. Return True on success otherwise False'''

        try:
            self._handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)           # Create the socket endpoint
            self._handler.settimeout(5.0)                                               # Timeout after 5 seconds
            self._handler.connect((self._system.settings["serverAddress"], self._system.settings["serverPort"]))
            self._logger.debug("TCP connected")
            return True
        except OSError:
            self._logger.error("Impossible to connect to the TCP server", exc_info = True)
            self._handler.close()
            return False
        except Exception:
            self._logger.critcal("Unexpected error while connecting TCP")
            self._handler.close()
            return False
            
    def _disconnect(self) -> None:
        '''Disconnect from the server'''

        self._handler.close()
        self._logger.debug("TCP connection closed")

    def stopThread(self) -> None:
        '''Kill this thread'''

        self._isRunning = False                                             # Set the status flag
        self._periodicClb.cancel()                                          # Stop the timer
        self._event.post(eventName = "sendData")                            # Raise the event to unlock the thread

    def _handshake(self) -> bool:
        '''Execute the connection's handshake'''

        try:
            msg = self._handler.recv(1024)                                  # Wait the server to request handshake
            if msg == self._HANDSHAKE_REQUEST:                              # Handshake request
                self._handler.sendall(self._TCP_ACK_OK)                     # Asnwer that we are ready

                msg = self._handler.recv(1024)                              # Get RSA keys length
                if msg != None and msg != 0 and msg != '' and msg != b'':   # Valid message
                    msg = int(msg.decode())

                    if msg in CryptoHandler.RSA_LENGTH:                                         # Key length is supported
                        if msg != self._system.settings["RSA"]:                                 # If the key length is different
                            self._system.updateSettings(newSettings = {"RSA" : msg})            # Update the RSA key settings
                        self._handler.sendall(self._TCP_ACK_OK)                                 # Acknowledge the reception
                    else:                                                                       # Key length not supported, handshake failed
                        self._handler.sendall(self._TCP_ACK_ERROR)                              # Notify the problem
                        return False

                    msg = self._handler.recv(1024)                                              # Get the server public key
                    self._srvPubKey = CryptoHandler.importRSApub(PEMfile = msg)                 # Import it

                    # Generate a new random AES key
                    self._aesKey = CryptoHandler.generateAES()
                    
                    # Generate a new RSA key pair and AES key
                    (self._cltPubKey, self._cltPrivKey) = CryptoHandler.generateRSA(length = self._system.settings["RSA"])

                    # Encrypt the AES key with the server's public RSA key
                    msg = CryptoHandler.RSAencrypt(pubkey = self._srvPubKey, raw = self._aesKey)

                    self._handler.sendall(msg)                              # Send the encrypted AES key
                    msg = self._handler.recv(1024)
                    if msg == self._TCP_ACK_OK:                             # Server confirmed reception
                        
                        # Encrypt this client RSA pubkey with the shared AES key
                        msg = CryptoHandler.AESencrypt(key = self._aesKey, raw = CryptoHandler.exportRSApub(pubkey = self._cltPubKey), byteObject = True)
                        self._handler.sendall(msg)
                        msg = self._handler.recv(1024)
                        if msg == self._TCP_ACK_OK:                         # Server confirmed reception
                            self._logger.debug("TCP handshake done")
                            return True                                     # Handshake completed successfully

            # If something goes wrong, the handshake fail and all the keys are destoryed. The problem is notified
            self._handler.sendall(self._TCP_ACK_ERROR)
            raise Exception("Handshake failed")                                               
        except Exception:                                                   # In case of error, restore default settings
            self._logger.error("Error occurred during TCP handshake", exc_info = True)
            self._srvPubKey = None
            self._cltPubKey = None
            self._cltPrivKey = None
            self._aesKey = None
            self._system.updateSettings(newSettings = self._DEFAULT_TCP_SETTINGS)
            return False

    def _sendData(self) -> None:                                    
        '''Periodic timer callback for TCP thread. Raise sendData event'''

        if self._event.isPresent(eventName = "sendData") == False:          
            self._event.createEvent(eventName = "sendData")
        
        #Raise the event and restart the timer
        self._event.post(eventName = "sendData")
        self._periodicClb = threading.Timer(interval = (self._system.settings["sendingFreq"] * 60), function = self._sendData)
        self._periodicClb.start()
        self._logger.debug("Send data Clb")

    def _send(self, data: object, byteObject: bool = False) -> None:

        if type(byteObject) != bool:
            raise TypeError

        return self._handler.sendall(CryptoHandler.AESencrypt(key = self._aesKey, raw = data, byteObject = byteObject))

    def _receive(self, byteObject: bool = False) -> object:

        if type(byteObject) != bool:
            raise TypeError

        message = self._handler.recv(1024)                                                                      # Receive data

        if message == None or message == '' or message == b'':                                                  # If nothing has been received, return None
            return None
        
        return CryptoHandler.AESdecrypt(key = self._aesKey, secret = message, byteObject = byteObject)          # Decrypt the message

    def run(self) -> None:
        
        #Create the timer  
        self._periodicClb = threading.Timer(interval = (self._system.settings["sendingFreq"] * 60), function = self._sendData)
        self._periodicClb.start()
        self._logger.debug("TCP thread started")
        
        while True:
            self._event.pend(eventName = "sendData")                            # Wait until the event is raised
            
            if self._isRunning == False:                                        # Check the thread status
                self._logger.debug("TCP thread closed")
                break
            
            if self._data.load(itemName = "dataReady") != True:                 # Check if there is something to send, otherwise skip this iteration
                continue

            if self._connect() == True:                                                 # Connect to the TCP server
                if self._handshake() == True:                                           # Cryptographic handshake
                    try:                                                                # Encrypt data and send them
                        self._send(data = self._system.settings["UID"])                 # Send UID
                        if self._receive(byteObject = True) != self._TCP_ACK_OK:        # In case of error, disconnect and skip the cycle
                            self._logger.error("Failed to send UID")
                            continue

                        sensorsData = json.dumps(self._data.load(itemName = "sampledData"))
                        self._send(data = sensorsData)
                        
                        while True:                                                                 # Commands loop
                            if self._receive(byteObject = True) == self._TCP_ACK_OK:                # Standard acknowledge
                                self._data.remove(itemName = "sampledData")                                                 # Delete data because we have already sent them
                                self._data.store(itemName = "dataReady", item = False, itemType = "bool")                   # Update status flag
                                break
                            else:                                                                                           # Unkown answer, close connection 
                                self._logger.warning("Unknown answer from the TCP server")
                                break
                    except socket.timeout:                                              # No answer from server
                        self._logger.warning("No answer from server after sending data")
                    except OSError:                                                     # TCP socket error 
                        self._logger.warning("Socket error while sending data")
                    except Exception:                                                   # Unkown exception (it's a very very very big pitty)
                        self._logger.critical("Unexpected error while sending data (very very unexpected)", exc_info = True)
                else:
                    self._logger.error("TCP handshake failed")

                self._disconnect()                                                      # Close the TCP connection
            else:
                self._logger.error("TCP connection failed")        
        
        self._logger.debug("TCP thread closed")
        return

    