import time, datetime, threading, logging, asyncio
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

        self._asyncLoop = None
        self._periodicUpdateTask = None

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

    async def _sendPeriodicUpdate(self) -> None:
        '''
        Task that sends updates to every subscribed user
        '''

        while self._isRunning == True:                                          # Of course if the thread is dead, this routine must return as well but we expect to cancel the task anyway
            
            # This task must wake up every "botUpdatePeriod" hours to send all the updates
            await asyncio.sleep(delay = self._system.settings["botUpdatePeriod"] * 3600)

            # UserList dovrebbe essere un dizionario con tutte le info dell'utente. Prima di proseguire con le select è meglio verificare che 
            # quell'utente voglia l'aggiornamento proprio adesso (comunque multiplo di 4)
            # Dal dizionario si tira fuori la chiave che è il chat id e si prendono i sensori, dai quali si prende il record più recente
            # Si manda un messaggio all'utente dicendo "Ciao UTENTE, ecco i tuoi aggiornamenti in tempo reale..."

            userList = await self._db.getUserList()                                     # Get a list of subscribed users
            for user in userList:                                                       # For each user
                cityList = await self._db.getSensorsByUser(userID = user)               # Get a list of associated cities
                updatePacket = await self._db.getUpdateByCity(cities = cityList)        # For all the associated cities, get the most recent record
    def stopThread(self) -> None:
        '''
        Close this Telegram Bot Thread
        '''
        if self._isRunning == True:
            try:
                if self._asyncLoop != None:
                    if self._periodicUpdateTask != None:
                        self._periodicUpdateTask.cancel()                       # Cancel the periodic task
                    self._asyncLoop.stop()                                      # Stop the event loop
            except Exception as e:
                self._logger.critical("Impossible to gracefully terminate the Telegram Bot event loop!", exc_info = True)
                raise e

            try:
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

        # Create the Async event loop for this thread and create the main bot task as well as the periodic update task
        try:
            self._asyncLoop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._asyncLoop)
            self._periodicUpdateTask = self._asyncLoop.create_task(self._sendPeriodicUpdate)
        except Exception:
            self._logger.critical("Impossible to set up Async environment for Telegram Bot Thread", exc_info = True)
            self._isRunning = False

        # The bot is ready to run, let's start listenig and let's start the event loop
        if self._isRunning == True:
            try:
                self._updater.start_polling()                           # Enable the Telegram Server polling, looking for new messages
                self._logger.debug("Telegram Bot's polling started")
            except Exception:
                self._logger.critical("An error occurred while starting the bot polling", exc_info = True)
                self._isRunning = False

        if self._isRunning == True:
            try:  
                self._asyncLoop.run_forever()                           # Start the Async loop and hang here
            except asyncio.CancelledError:
                self._logger.debug("Main loop raised a CancellatedError")
            except Exception:
                self._logger.critical("An error occurred while starting the bot polling or the Bot Async Loop", exc_info = True)

        self._logger.debug("Telegram Bot's thread terminated")
        return
