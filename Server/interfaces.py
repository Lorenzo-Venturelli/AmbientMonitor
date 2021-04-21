import threading, json, os, subprocess, copy, logging, hashlib, base64, sys, signal, time
try:
    import rsa
except ImportError:
    print("Fatal error: Missing RSA module")
    exit()
try:
    
    from Crypto.Cipher import AES
    from Crypto import Random
except ImportError:
    print("Fatal error: Missing Crypto module")
    exit()

class System():

    _DEFAULT_SETTINGS = {}
    _DEFAULT_PATH = "./"

    def __init__(self, logger: object, path: str = System._DEFAULT_PATH):
        if type(path) != str or isinstance(logger, logging.Logger) == False:
            raise TypeError

        self._logger = logger

        if path[-1] != "/":                                                 # Check the path format and eventually correct it
            path = path + "/"
        
        self._filename = path + "settings.json"
        self._settings = dict()
        self._lock = threading.Lock()

        try:
            if os.path.exists(path) == False:                               # Check wether the file exists or not
                self._logger.debug("Settings file doesn't exist")
                subprocess.call("touch " + path)                            # Eventually create it
                subprocess.call("chmod 744 " + path)                        # Set privileges
                self._settings = copy.deepcopy(System._DEFAULT_SETTINGS)    # Initialize the file with default settings
                with open(path, "w") as fp:
                    json.dump(System._DEFAULT_SETTINGS, fp, indent = 4, sort_keys = True)
            else:
                self._logger.debug("Settings file exists")
                with open(path, "r") as fp:
                    self._settings = json.load(fp)
        except json.JSONDecodeError:                                        # Corrupted settings file
            self._logger.error("Settings file is corrupted, overwrite")
            self._settings = copy.deepcopy(System._DEFAULT_SETTINGS)        # Initialize the file with default settings
            with open(path, "w") as fp:
                json.dump(System._DEFAULT_SETTINGS, fp, indent = 4, sort_keys = True)
        except Exception as e:                                              # Unknown exception, propagate it
            self._logger.critical("Unexpected error in System constructor.", exc_info = True)
            raise e

    def _rebuildFile(self) -> None:
        '''Rebuild the settings file'''

        self._lock.acquire()                                                # Avoid race conditions
        try:
            self._logger.debug("Rebuild settings file")
            if os.path.exists(self._filename) == False:                     # Check wether the file exists or not
                self._logger.debug("Settings file doesn't exist")
                subprocess.call("touch " + self._filename)                  # Eventually create it
                subprocess.call("chmod 744 " + self._filename)              # Set privileges
                self._settings = copy.deepcopy(System._DEFAULT_SETTINGS)    # Initialize the file with default settings
            with open(self._filename, "w") as fp:
                json.dump(System._DEFAULT_SETTINGS, fp, indent = 4, sort_keys = True)
        except Exception as e:                                              # Unknown exception, propagate it
            self._logger.critical("Unexpected error while rebuilding the settings file", exc_info = True)
            self._lock.release()                                            
            raise e
        self._lock.release()                                                

    def updateSettings(self, newSettings: dict) -> None:
        '''Update System settings. Invalid settings will be ignored'''

        if type(newSettings) != dict:
            raise TypeError

        self._lock.acquire()                                                # Avoid race conditions

        for item in newSettings.keys:                                       # Check each parameter and if it's valid save it
            if item in self._DEFAULT_SETTINGS.keys():                       # If this item is well known, check its validity
                if type(newSettings[item]) == type(self._DEFAULT_SETTINGS[item]):
                    self._settings[item] = copy.deepcopy(newSettings[item])
            else:                                                           # This item is not recognizd, just update it and assume it as the default value
                self._settings[item] = copy.deepcopy(newSettings[item])
                self._DEFAULT_SETTINGS[item] = copy.deepcopy(newSettings[item])

        try:
            with open(self._filename, "w") as fp:                           # Update the settings file
                json.dump(self._settings, fp, indent = 4, sort_keys = True)
        except Exception:
            self._logger.error("Impossible to deal with settings file, rebuild it", exc_info = True)
            self._lock.release()                                            # Rebuild the settings file and try to write again
            self._rebuildFile()

        self._lock.release()                                                # Release this resource

    def updatePath(self, newPath: str) -> bool:
        '''Change the settings file location. Return True on success'''

        if type(newPath) != str:
            raise TypeError
        
        if newPath[-1] != "/":                                              # Check the path format and eventually correct it
            newPath = newPath + "/"

        newPath = newPath + "settings.json"

        self._lock.acquire()

        try:
            subprocess.call("mv " + self._filename + " " + newPath)         # Move the settings file
            self._filename = newPath
        except Exception:
            self._logger.error("Impossoible to move the settings file", exc_info = True)
            self._lock.release()
            return False

        self._lock.release()
        return True
    
    @property
    def settings(self) -> dict:
        return copy.deepcopy(self._settings)

    @property
    def filename(self) -> str:
        return copy.deepcopy(self._filename)

    @property
    def defaultSettings(self) -> dict:
        return copy.deepcopy(self._DEFAULT_SETTINGS)

class Data():
    _SUPPORTED_TYPES = ["int", "float", "str", "tuple", "list", "dict", "object", "func"]

    def __init__(self, logger: object):
        if isinstance(logger, logging.Logger) == False:
            raise TypeError

        self._lock = threading.Lock()
        self._data = dict()
        self._logger = logger
    
    @property
    def supportedTypes(self):
        '''Supported value types'''
        return copy.deepcopy(Data._SUPPORTED_TYPES)

    def isPresetn(self, itemName: str) -> bool:
        '''Check if an item with (itemName) is stored in this interface'''

        if type(itemName) != str:
            raise TypeError
        
        if itemName in self._data.keys():
            return True
        else:
            return False

    def store(self, itemName: str, item: object, itemType: str = "object") -> bool:
        '''
        Store (item) of type (itemType) with (itemName) label.
        Verify the type and return True on success.
        '''

        if type(itemName) != str or type(itemType) != str:
            raise TypeError

        if itemType not in Data._SUPPORTED_TYPES:               # If the item type is not supported, treat this item like a generic object
            itemType = "object"

        if itemType == "int" and type(item) != int:
            return False
        if itemType == "float" and type(item) != float:
            return False
        if itemType == "str" and type(item) != str:
            return False
        if itemType == "tuple" and type(item) != tuple:
            return False
        if itemType == "list" and type(item) != list:
            return False
        if itemType == "dict" and type(item) != dict:
            return False
        if itemType == "func" and callable(item) == False:
            return False

        try:
            self._lock.acquire()                                    
            self._data[itemName] = (copy.deepcopy(item), itemType)  # Store the item
            self._lock.release()
        except Exception:
            self._logger.error("Impossible to store an item.", exc_info = True)
            self._lock.release()
            return False

        return True

    def load(self, itemName: str) -> object:
        '''Return the item stored with (itemName) label. If this item doesn't exist, return None'''

        if type(itemName) != str:
            raise TypeError

        if self.isPresetn(itemName = itemName) == False:
            return None
        else:
            try:
                self._lock.acquire()
                item = copy.deepcopy(self._data[itemName][0])
                self._lock.release()
            except Exception:                                       # Unexpected error, return None
                self._logger.error("Impossible to load an item.", exc_info = True)
                self._lock.release()
                return None
            return item

    def remove(self, itemName: str) -> bool:
        '''Remove an item stored with (itemName) label. Return True on success, even if the item wasn't stored'''

        if type(itemName) != str:
            raise TypeError

        if self.isPresetn(itemName = itemName) == False:            # The item doesn't exist
            return True
        else:                                                       # The item exists
            try:
                self._lock.acquire()
                del self._data[itemName]                            # Delete it
                self._lock.release()
            except Exception:                                       # Unexpected error
                self._logger.error("Impossible to remove an item.", exc_info = True)
                self._lock.release()
                return False
            
            return True

    def insertList(self, itemName: str, element: object, index: int) -> bool:
        '''
        Insert (element) into (itemName) list in (index) position.
        If the list doesn't exists, create it. If (itemName) doesn't point to a list, return False.
        '''

        if type(itemName) != str or type(index) != int:
            raise TypeError

        if self.isPresetn(itemName = itemName) == False:                                    # The item doesn't exist
            self.store(itemName = itemName, item = list(element), itemType = "list")        # Create the queue and insert the item
        else:                                                                               # The item exists
            if self._data[itemName][1] != "list":                                           # If it's not a list, return False
                return False
            else:                                                                           # Insert the new element
                try:
                    self._lock.acquire()
                    self._data[itemName][0].append(index, copy.deepcopy(element))
                    self._lock.release()
                except Exception:                                                           # Unexpected error
                    self._logger.error("Impossible to insert an item into a list.", exc_info = True)
                    self._lock.release()
                    return False

        return True

    def getFromList(self, itemName: str, index: int, remove: bool = False) -> object:
        '''
        Get the element in position (index) from the (itemName) list.
        If the list doesn't exist, (index) is not valid or (itemName) is not a list, return None.
        If (remove) is True, the element is removed from the list
        '''

        if type(itemName) != str or type(index) != int or type(remove) != bool:
            raise TypeError

        if self.isPresetn(itemName = itemName) == False:                                    # The item doesn't exist
            return None
        elif self._data[itemName][1] != "list":                                             # The item is not a list
            return None
        else:                                                                               # The item is a list
            try:
                self._lock.acquire()
                element = copy.deepcopy(self._data[itemName][0][index])                     # Get the element
                if remove == True:                                                          # If requested, delete the element
                    del self._data[itemName][0][index]
                self._lock.release()
            except IndexError:                                                              # The index is not valid
                self._logger.error("Invalid index, item not in list.")
                self._lock.release()
                return None
            except Exception as e:                                                          # Unknown exception, release this interface and propagate the error
                self._logger.critical("Unknown error occurred while reading a lsit", exc_info = True)
                self._lock.release()
                raise e
            return element

    def insertDict(self, itemName: str, key: object, element: object) -> bool:
        '''Insert (element) using (key) into the (itemName) dictionary. Return True on success'''

        if type(itemName) != str:
            raise TypeError

        if self.isPresetn(itemName = itemName) == False:                                    # Item doesn't exist, create it
            self.store(itemName = itemName, item = {copy.deepcopy(key) : copy.deepcopy(element)}, itemType = "dict")
        else:
            if self._data[itemName][1] != "dict":                                           # Item is not a dict, fail
                return False
            else:
                try:
                    self._lock.acquire()
                    self._data[itemName][0][copy.deepcopy(key)] = copy.deepcopy(element)    # Insert the new element into item
                    self._lock.release()
                    return True
                except Exception:
                    self._logger.error("Impossible to insert an item into a dict.", exc_info = True)
                    self._lock.release()
                    return False

    def getFromDict(self, itemName: str, key: object, remove: bool = False) -> object:
        '''
        Get element pointed by (key) from (itemName) dict. If remove = True the element is removed.
        If the (itemName) is not a dict, it doesn't exist or (key) doesn't exist, return None.
        '''

        if type(itemName) != str or type(remove) != bool:
            raise TypeError

        if self.isPresetn(itemName = itemName) == True:                                     # Item exist
            if self._data[itemName][1] == "dict":                                           # Item is a dict
                if key in self._data[itemName][0].keys():                                   # Key exists
                    try:
                        self._lock.acquire()
                        element = copy.deepcopy(self._data[itemName][0][copy.deepcopy(key)])    # Get the element
                        if remove == True:
                            del self._data[itemName][0][copy.deepcopy(key)]                     # If requested, remove this element
                        self._lock.release()
                        return copy.deepcopy(element)
                    except Exception:                                                       # Unexpected error, return None
                        self._logger.error("Impossible to get an item from a dict.", exc_info = True)
                        self._lock.release()
                        return None
        
        return None                                                                         # Operation not completed

class Event():

    def __init__(self, logger: object):
        if isinstance(logger, logging.Logger) == False:
            raise TypeError

        self._logger = logger
        self._events = dict()
        self._lock = threading.Lock()

    @property
    def eventList(self):
        return copy.deepcopy(self._events.keys())

    def isPresent(self, eventName: str) -> bool:
        '''Return True if an event with (eventName) exists'''

        if type(eventName) != str:
            raise TypeError

        if eventName in self._events.keys():
            return True
        else:
            return False

    def createEvent(self, eventName: str) -> bool:
        '''Create a new event called (eventName).'''

        if type(eventName) != str:
            raise TypeError

        if self.isPresent(eventName = eventName) == True:
            return False
        else:
            self._lock.acquire()
            self._events[eventName] = [threading.Event(), 0]
            self._lock.release()
            return True

    def deleteElement(self, eventName: str) -> bool:
        '''Delete an event. If there are pending requests on this element, the operation fails'''

        if type(eventName) != str:
            raise TypeError

        if self.isPresent(eventName = eventName) == False:              # This event doesn't exist
            return True
        else:
            if self._events[eventName][1] != 0:                         # There are pending requests on this event
                return False
            else:                                                       # No pending requests, delete this item
                try:
                    self._lock.acquire()
                    del self._events[eventName]
                    self._lock.release()
                    return True
                except Exception:                                       # Unexpected error
                    self._logger.error("Impossible to delete an event", exc_info = True)
                    self._lock.release()
                    return False

    def pend(self, eventName: str, timeout: int = None) -> bool:
        '''
        Pend on (eventName) until (timeout) expires or the event is posted.
        Return False if the timeout expires, otherwhise return True.
        If the event doesn't exist, return False
        '''

        if type(eventName) != str or timeout != None and type(timeout) != int:
            raise TypeError

        if self.isPresent(eventName = eventName) == False:                  # Event doesn't exist
            return False
        else:
            self._lock.acquire()
            self._events[eventName][1] = self._events[eventName][1] + 1     # Take this pending request in account
            self._lock.release()
            
            result = self._events[eventName][0].wait(timeout = timeout)     # Wait the event to be posted
            
            self._lock.acquire()
            self._events[eventName][1] = self._events[eventName][1] - 1
            self._lock.release()

            self._events[eventName][0].clear()                              # Clear this event
            return result

    def post(self, eventName: str) -> bool:
        '''Post (eventName)'''

        if type(eventName) != str:
            raise TypeError
            
        if self.isPresent(eventName = eventName) == False:
            return False
        else:
            self._events[eventName][0].set()
            return True

class CryptoHandler():

    # AES protocol costants
    _BLOCK_SIZE = 32
    _PAD = lambda s: s + (CryptoHandler._BLOCK_SIZE - len(s) % CryptoHandler._BLOCK_SIZE) * chr(CryptoHandler._BLOCK_SIZE - len(s) % CryptoHandler._BLOCK_SIZE)
    _UNPAD = lambda s: s[:-ord(s[len(s) - 1:])]

    # RSA protocol costants
    RSA_LENGTH = [521, 1024, 2048, 4092]

    def __init__(self):
        raise Exception("This class can't be istantiated")

    @classmethod
    def generateRSA(cls, length: int = 1024):
        '''Generate a new RSA keys pair with the given length'''

        if type(length) != int:
            raise TypeError

        if length not in cls.RSA_LENGTH:
            raise Exception("Invalid RSA length")

        (pubkey, privkey) = rsa.newkeys(length)
        return (pubkey, privkey)

    @classmethod
    def generateAES(cls):
        '''Generate a random 128 bit AES key'''

        key = hashlib.sha256(rsa.randnum.read_random_bits(128)).digest()
        return key

    @classmethod
    def AESencrypt(cls, key: str, raw: object, byteObject: bool = False):
        '''Encrypt a plain message (raw) with (key) using AES-128'''

        if type(key) != str or type(byteObject) != bool:
            raise TypeError

        try:
            if byteObject == False:
                raw = cls._PAD(raw)
            else:
                raw = raw.decode()
                raw = cls._PAD(raw)
                raw = raw.encode()
            iv = Random.new().read(AES.block_size)
            chiper = AES.new(key, AES.MODE_CBC, iv)
            secret = base64.b64encode(iv + chiper.encrypt(raw))
        except Exception:
            secret = None

        return secret

    @classmethod
    def AESdecrypt(cls, key: str, secret: object, byteObject: bool = False):
        '''Decrypt a secret message (secret) with (key) using AES-128'''

        if type(key) != str or type(byteObject) != bool:
            raise TypeError

        try:
            secret = base64.b64decode(secret)
            iv = secret[:AES.block_size]
            cipher = AES.new(key, AES.MODE_CBC, iv)
            raw = cipher.decrypt(secret[AES.block_size:])
            if byteObject == False:
                raw = cls._UNPAD(raw)
                raw = raw.decode()
            else:
                raw = raw.decode()
                raw = cls._UNPAD(raw)
                raw = raw.encode()
        except Exception:
            raw = None

        return raw

    @classmethod
    def RSAencrypt(cls, pubkey: object, raw: object):
        '''Encrypt a plain message (raw) with (pubkey) using RSA'''

        try:
            if type(raw) is not bytes:
                raw = raw.encode('UTF-8')
            secret = rsa.encrypt(message = raw, pub_key = pubkey)
        except Exception:
            secret = None
            
        return secret

    @classmethod
    def RSAdecrypt(cls, privkey: object, secret: object, skipDecoding: bool = False):
        '''
        Decrypt a secret message (secret) with (privkey) using RSA
        skipDecoding = False -> After decryption the plain message will be decoded using UTF-8
        '''

        if type(skipDecoding) != bool:
            raise TypeError

        try:
            raw = rsa.decrypt(crypto = secret, priv_key = privkey)
            if skipDecoding == False:
                raw = raw.decode('UTF-8')
            return raw
        except rsa.pkcs1.DecryptionError:
            return False

    @classmethod
    def exportRSApub(cls, pubkey: object):
        '''Export RSA key'''

        return pubkey.save_pkcs1(format = "PEM")

    @classmethod
    def importRSApub(cls, PEMfile: object):
        '''Import RSA public key'''

        return rsa.PublicKey.load_pkcs1(keyfile = PEMfile, format = "PEM")

class InterruptHandler(object):
    '''Handle system signals gracefully to permit a clean exit'''

    def __init__(self, signals: tuple = (signal.SIGINT, signal.SIGTERM)):
        if type(signals) != tuple:
            raise TypeError

        self.signals = signals                                          # Touple of handled signals (for us only the ones related to closign the program)
        self.original_handlers = {}                                     # Original handlers from the signal module

    def __enter__(self):                                                # Method called when this object is opened as an handler
        self.interrupted = False                                        # Reset status flags
        self.released = False

        for sig in self.signals:                                        
            self.original_handlers[sig] = signal.getsignal(sig)         # Get the original handlers for each signal
            signal.signal(sig, self.handler)                            # Substitute the origina ones with this class' one

        return self

    def __exit__(self, type, value, tb) -> None:                        # Method called when this class' object is closed
        self.release()

    def handler(self, signum, frame) -> None:                           # Method invoked when a system signal is received
        self.release()
        self.interrupted = True

    def release(self) -> bool:                                          # For each signal that we are handling, set back the original handler
        if self.released == True:
            return False

        for sig in self.signals:
            signal.signal(sig, self.original_handlers[sig])

        self.released = True
        return True


if __name__ == "__main__":
    print("Fatal error: This program has to be used as a module")
    exit()