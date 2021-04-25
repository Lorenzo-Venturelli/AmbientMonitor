#!/bin/python3

import sys
arguments = len(sys.argv) - 1
if arguments > 0:
    if sys.argv[1] == 'debug':
        import ptvsd
        # Allow other computers to attach to ptvsd at this IP address and port.
        ptvsd.enable_attach(address=("192.168.1.117", 3000))
        # Pause the program until a remote debugger is attached	
        ptvsd.wait_for_attach()

import threading, logging, time, signal
import tcp, sensors
from interfaces import System, Data, Event, InterruptHandler


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