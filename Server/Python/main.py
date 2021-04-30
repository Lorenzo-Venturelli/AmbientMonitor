#!/bin/python3

import sys
arguments = len(sys.argv) - 1
if arguments > 0:
    if sys.argv[1] == 'debug':
        import ptvsd
        # Allow other computers to attach to ptvsd at this IP address and port.
        ptvsd.enable_attach(address=("10.0.0.4", 3000))
        # Pause the program until a remote debugger is attached	
        ptvsd.wait_for_attach()
        
import threading, logging, sys, time, signal
from interfaces import Data, Event, System, InterruptHandler
import tcp

if __name__ == "__main__":
    with InterruptHandler() as sig:
        try:
            logging.basicConfig(filename = "../Files/loggerFile.log", filemode = "w")
            logger = logging.getLogger(name = "systemLog")                      # Create the logger handler

            if len(sys.argv) > 1:                                               # Check inline args
                if sys.argv[1] == "debug":                                      # Run the probram in debug mode
                    logger.setLevel(logging.DEBUG)                              # Verbose logging
                elif sys.argv[1] == "info":
                    logger.setLevel(logging.info)                               # Verbose logging
                else:
                    logger.setLevel(logging.ERROR)                              # Only error messages
            else:
                logger.setLevel(logging.ERROR)

            systemInterface = System(logger = logger)                           # Create interfaces' objects
            dataInterface = Data(logger = logger)
            eventInterface = Event(logger = logger)

            # Create threads
            serverThread = tcp.TcpServer(data = dataInterface, event = eventInterface, system = systemInterface, logger = logger)

            # Store the threads' objects so that they'll be available program-wide
            dataInterface.store(itemName = "threads", item = (serverThread), itemType = "tuple")

            # Start the threads
            serverThread.start()

            while sig.interrupted == False:                                     # Sleep until a keyboard interrupt occur
                time.sleep(1)

        except Exception as e:
            print("Unexpected error\n")
            print(e)
    
    try:
        serverThread.stopThread()
        serverThread.join()
    except Exception as e:
        print(e)
        sys.exit(1)

    sys.exit(0)
