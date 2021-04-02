#!/bin/python3
import threading, logging, sys, time, signal
import tcp, sensors
from interfaces import System, Data, Event

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

    def __exit__(self, type, value, tb):                                # Method called when this class' object is closed
        self.release()

    def handler(self, signum, frame):                                   # Method invoked when a system signal is received
        self.release()
        self.interrupted = True

    def release(self):                                                  # For each signal that we are handling, set back the original handler
        if self.released == True:
            return False

        for sig in self.signals:
            signal.signal(sig, self.original_handlers[sig])

        self.released = True
        return True


if __name__ == "__main__":
    with InterruptHandler() as sig:
        try:
            logger = logging.getLogger(name = "systemLog")                      # Create the logger handler

            if len(sys.argv) > 1:                                               # Check inline args
                if sys.argv[1] == "debug":                                      # Run the probram in debug mode
                    logger.setLevel(logging.DEBUG)                              # Verbose logging
                else:
                    logger.setLevel(logging.ERROR)                              # Only error messages
            else:
                logger.setLevel(logging.ERROR)

            systemInterface = System(logger = logger)                           # Create interfaces' objects
            dataInterface = Data(logger = logger)
            eventInterface = Event(logger = logger)

            # Create threads
            sensorsThread = sensors.Sensors(data = dataInterface, event = eventInterface, system = systemInterface, logger = logger)
            tcpThread = tcp.TcpClient(data = dataInterface, event = eventInterface, system = systemInterface, logger = logger)

            # Store the threads' objects so that they'll be available program-wide
            dataInterface.store(itemName = "threads", item = (sensorsThread, tcpThread), itemType = "tuple")

            # Start the threads
            sensorsThread.start()
            tcpThread.start()

            while True:                                                         # Sleep until a keyboard interrupt occur
                time.sleep(1)

        except Exception as e:                                                  # Unexpected error, exit
            print("Unexpected error\n" + e)

    try:                                                                        # Stop each thread and exit
        sensorsThread.stopThread()
        sensorsThread.join()
        tcpThread.stopThread()
        tcpThread.join()
        sys.exit("Program closed gracefully")
    except Exception as e:                                                      # Unknown errors occurred                                                               
        print(e)
        sys.exit("Program closed with errors")