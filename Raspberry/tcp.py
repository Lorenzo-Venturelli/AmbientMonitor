import socket, threading
from interfaces import Data, Event, CryptoHandler, System

class tcpClient(threading.Thread):
    def __init__(self, address: str, port: int, event: object, data: object, system: object):
        if (type(address) != str or type(port) != int or isinstance(event, Event) != True 
            or isinstance(data, Data) != True or isinstance(system, System) != True):
            raise TypeError
        
        self._handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._handler.settimeout(5.0)                                       # Timeout after 5 seconds
        self._serverAddr = address
        self._serverPort = port
        self._status = False
        self._event = event
        self._data = data
        self._system = system
        self._srvPubKey = None
        self._cltPubKey = None
        self._cltPrivKey = None
        self._aesKey = None
        self._running = True
    
    def _connect(self):
        '''Connect to the server'''

        try:
            self._handler.connect((self._serverAddr, self._serverPort))
            self._status = True
        except OSError:
            self._handler.close()
            self._status = False

    def _disconnect(self):
        '''Disconnect from the server'''

        self._handler.close()
        self._status = False

    def stopThread(self):
        '''Kill this thread'''

        self._running = False
        self._event.post(eventName = "tcpEvent")

    def _handshake(self) -> bool:
        '''Execute the connection's handshake'''

        try:
            self._handler.sendall(b'199')                                   # Request RSA handshake
            msg = self._handler.recv(1024)                                  # Get RSA keys length
            if msg != None and msg != 0 and msg != '' and msg != b'':
                msg = msg.decode()
                
                if msg in CryptoHandler.RSA_LENGTH:                         # Key length is supported
                    if msg != self._system.settings["RSA"]:                 # If the key length is different
                        self._system.updateSettings(RSA = msg)              # Update the RSA key settings
                    self._handler.sendall(b'200')                           # Acknowledge the reception
                    msg = self._handler.recv(1024)                          # Get RSA pub key from the server
                    
                    if msg != None and msg != 0 and msg != '' and msg != b'':
                        self._srvPubKey = CryptoHandler.importRSApub(PEMfile = msg)         # Import the server public key
                        self._handler.sendall(b'200')                                       # Acknowledge the reception
                        
                        # Generate a new RSA key pair and AES key
                        (self._cltPubKey, self._cltPrivKey) = CryptoHandler.generateRSA(length = self._system.settings["RSA"])

                        # Generate a new 128 bit AES key and encrypt it with the server public key
                        self._aesKey = CryptoHandler.generateAES()
                        msg = CryptoHandler.RSAencrypt(pubkey = self._srvPubKey, raw = self._aesKey)
                        
                        self._handler.sendall(msg)                          # Send the AES key
                        msg = self._handler.recv(1024)                      # Get the server pub key
                        
                        if msg != None and msg != 0 and msg != '' and msg != b'':
                            
                            # Export and crypt my rsa public key
                            msg = CryptoHandler.AESencrypt(key = self._aesKey, raw = CryptoHandler.exportRSApub(pubkey = self._cltPubKey), byteObject = False)
                            self._handler.sendall(msg)                      # Send this key to the server
                        
                            msg = self._handler.recv(1024)                  # Wait ack
                            if msg == b"200":                               # Ack received handshake done
                                return True
            return False                                                    # Something went wrong, handshake failed
        except Exception:                                                   # In case of error, restore default settings
            self._srvPubKey = None
            self._cltPubKey = None
            self._cltPrivKey = None
            self._aesKey = None
            self._system.updateSettings(RSA = self._system.defaultSettings["RSA"])
            return False

    def run(self):
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

