#!/bin/python3

import threading, logging, sys, time, signal
from interfaces import Data, Event, System, InterruptHandler

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
        except Exception as e:
            print("Unexpected error\n" + e)
    
    try:
        sys.exit("Program closed gracefully")
    except Exception as e:
        print(e)
        sys.exit("Program closed with errors")
