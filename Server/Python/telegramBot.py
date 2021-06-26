import time, datetime, threading, logging
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from interfaces import Data, Event, System
from db import MySQL

class TelegramBot(threading.Thread):
    _AUTH_TOKEN = "1830886980:AAEY8fGLTn9uY0qLKhaZ7T29HgLMuMlU1Yo"
    _DEFAULT_TELEGRAM_BOT_SETTINGS = {"botUpdatePeriod" : 4}

    def __init__(self, data: object, event: object, system: object, logger: object):
        if (isinstance(event, Event) != True  or isinstance(data, Data) != True 
            or isinstance(system, System) != True or isinstance(logger, logging.Logger) == False):
            raise TypeError
        
        self._event = event
        self._data = data
        self._system = system
        self._logger = logger
        self._db = MySQL(system = system, logger = logger)
        self._commandCallbacks = {
            "aggiungi_città"        :   self._newSub, 
            "rimuovi_città"         :   self._removeSub,
            "controlla_città"       :   self._getUpdate, 
            "ricevi_statistiche"    :   self._getStats, 
            "mostra_panoramica"     :   self._showBrief
        }

        for item in self._DEFAULT_TELEGRAM_BOT_SETTINGS.keys():
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TELEGRAM_BOT_SETTINGS[item]})

        self._updater = None
        self._dispatcher = None
        self._commandHandlers = list()
        self._periodicUpdateThread = None

        self._isRunning = True
        super().__init__(daemon = False, name = "Telegram Bot")

    def _newSub(self, update: dict, context: object) -> None:
        pass

    def _removeSub(self, update: dict, context: object) -> None:
        pass

    def _getUpdate(self, update: dict, context: object) -> None:
        pass

    def _getStats(self, update: dict, context: object) -> None:
        pass

    def _showBrief(self, update: dict, context: object) -> None:
        pass

    def _sendPeriodicUpdate(self) -> None:
        pass
    
    def stopThread(self) -> None:
        '''
        Close this Telegram Bot Thread
        '''
        if self._isRunning == True:
            try:
                self._periodicUpdateThread.cancel()                         # Stop the periodic update routine
                self._updater.stop()                                        # Shut the Telegram Bot down
                self._db.close()                                            # Close the connection to DB
                self._isRunning = False
            except Exception as e:
                self._logger.critical("Impossible to gracefully terminate the Telegram Bot thread!", exc_info = True)
                raise e

        return

    def run(self) -> None:
        
        # Create the Bot Updater and it's linked Dispatcher
        try:
            self._updater = Updater(self._AUTH_TOKEN, use_context = True)
            self._dispatcher = self._updater.dispatcher
        except Exception:
            self._logger.critical("Impossible to create the Updater for Telegram Bot", exc_info = True)
            self._isRunning = False

        # If the Bot has been successfully created, proceed to register all the commands callbacks
        try:
            for command in self._commandCallbacks.keys():
                self._commandHandlers.append(CommandHandler(command, self._commandCallbacks[command], run_async = True))
        except Exception:
            self._logger.critical("Impossible to create command handlers for Telegram Bot", exc_info = True)
            self._isRunning = False

        # Now that we have the command handlers, add them to the dispatcher in order to trigger those callbacks when it's needed
        try:
            for handler in self._commandHandlers:
                self._dispatcher.add_handler(handler)
        except Exception:
            self._logger.critical("Impossible to register command handlers to the Telegram Bot's dispatcher", exc_info = True)
            self._isRunning = False

        # The bot is ready to run, let's start listenig and let's create the periodic routine to send updates
        if self._isRunning == True:

            # Create a periodic timer that executes every "botUpdatePeriod" hours and send to every subscribed user an update
            self._periodicUpdateThread = threading.Timer(interval = (self._system.settings["botUpdatePeriod"] * 3600), function = self._sendPeriodicUpdate)
            self._periodicUpdateThread.start()

            self._updater.start_polling()                           # Enable the Telegram Server polling, looking for new messages
            self._logger.debug("Telegram Bot's polling started")
            self._updater.idle()                                    # Put this thread in sleeping mode

        
        
        self._logger.debug("Telegram Bot's thread terminated")
        return
