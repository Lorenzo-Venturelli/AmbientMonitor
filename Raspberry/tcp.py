import socket, threading, logging
from interfaces import Data, Event, CryptoHandler, System

class TcpClient(threading.Thread):

    _DEFAULT_TCP_SETTINGS = {"serverAddress" : "www.ambientmonitor.page", "serverPort" : 1234, "RSA" : 1024}
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
        
        self._handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)           # Create the socket endpoint
        self._handler.settimeout(5.0)                                               # Timeout after 5 seconds

        for item in self._DEFAULT_TCP_SETTINGS.keys():                              # If this thread's settings don't exist, create them from default ones
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TCP_SETTINGS[item]})

        self._connected = False                                                     # This client is connected to the server
        self._isRunning = True                                                      # This thread is alive
        self._srvPubKey = None                                                      # Server's RSA public key
        self._cltPubKey = None                                                      # This client's RSA public key
        self._cltPrivKey = None                                                     # This client's RSA private key
        self._aesKey = None                                                         # AES key
        
        super(daemon = False)
    
    def _connect(self) -> None:
        '''Connect to the server'''

        try:
            self._handler.connect((self._system.settings["serverAddress"], self._system.settings["serverPort"]))
            self._status = True
        except OSError:
            self._logger.error("Impossible to connect to the TCP server")
            self._handler.close()
            self._status = False

    def _disconnect(self) -> None:
        '''Disconnect from the server'''

        self._handler.close()
        self._status = False

    def stopThread(self) -> None:
        '''Kill this thread'''

        self._running = False
        self._event.post(eventName = "tcpEvent")###################### da vedere

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
            self._system.updateSettings(RSA = self._system.defaultSettings["RSA"])
            return False

    def run(self) -> None:
        '''while True:
            self._msgReady.wait(timeout = None)                             # Wait until a message is ready to be sent
            self._msgReady.clear                                            

            if self._running == False:                                      # Check if the sistem is still alive
                break

            self._connect()                                                 # Try to connect to the server
            if self._status == True:                                        # Connected
                print("Connection successfull")
                failed = list()                                             # Prepare a list to store failed messages (we will try next time)
                msgQueue = self._dataProxy.getValue["msgQueue"]             # Get the messages to send
                
                for msg in msgQueue:                                        # Try to send each message
                   self._handler.sendall(msg)
                   answer = self._handler.recv(1024)                        # Wait the answer
                   if answer != b"200":                                     # If no answer or a worng answer is received, save the message to try again
                       failed.append(msg)

                self._dataProxy.setValue["msgQueue"] = failed               # Update the message queue
                self._disconnect()                                          # Close connection
            else:
                print("Connection failed")
                continue'''
        
        return

