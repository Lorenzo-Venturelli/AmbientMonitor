import socket, threading

class tcpClient(threading.Thread):
    def __init__(self, address: str, port: int, event: object, data: object):
        if type(address) != str or type(port) != int or isinstance(event, threading.Event) != True or isinstance(data):
            raise TypeError
        
        self._handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._handler.settimeout(5.0)                                       # Timeout after 5 seconds
        self._serverAddr = address
        self._serverPort = port
        self._status = False
        self._msgReady = event
        self._dataProxy = data
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
        self._msgReady.set()

    def run(self):
        while True:
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
                continue
        
        return

