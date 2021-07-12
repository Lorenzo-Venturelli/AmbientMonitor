import time, datetime, threading, logging, asyncio
from typing import Counter
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.ext.dispatcher import run_async
from telegram.ext.filters import MessageFilter
from interfaces import Data, Event, System
from db import MySQL

class TelegramBot(threading.Thread):
    _AUTH_TOKEN = "1830886980:AAEY8fGLTn9uY0qLKhaZ7T29HgLMuMlU1Yo"
    _DEFAULT_TELEGRAM_BOT_SETTINGS = {"botUpdatePeriod" : 4}
    _SUPPORTED_TELEGRAM_COMMANDS = ["start", "register", "city_list", "add_city", "remove_city", "check_city", "get_stats", "show_brief"]

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
            "start"             :   self._welcomeUser,
            "help"              :   self._helpUser,
            "register"          :   self._registerUser,
            "city_list"         :   self._cityList,
            "add_city"          :   self._newSub, 
            "remove_city"       :   self._removeSub,
            "check_city"        :   self._getUpdate, 
            "get_stats"         :   self._getStats, 
            "show_brief"        :   self._showBrief
        }

        for item in self._DEFAULT_TELEGRAM_BOT_SETTINGS.keys():
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TELEGRAM_BOT_SETTINGS[item]})

        # Telegram Bot objects
        self._updater = None
        self._dispatcher = None
        self._commandHandlers = list()

        # Users that are interacting with a command are stored here in order to deal with multiple messages
        self._activeUserState = dict()
        self._activeUserStateLock = threading.Lock()

        # Asyncio objects
        self._asyncLoop = None
        self._periodicUpdateTask = None

        self._isRunning = True
        super().__init__(daemon = False, name = "Telegram Bot")

    def _welcomeUser(self, update: dict, context: object) -> None:
        '''
        Say Hi to new users and invite them to register
        '''

        update.message.reply_text('''Welcome to AmbientMonitorBot! You can start to use this app typing /city_list to see''' +
            '''which cities are supported and then you can use one of the following commands:\n\n/register to start using our update system!\n\n''' + 
            '''/check_city to receive real time data from a city of your choice\n\n/get_stats to receive fancy visual statistics for a city\n\n''' +
            '''/show_brief to have a complete view of what's going on right now in every city covered by our network\n\nAfter your registration you will ''' +
            '''also be able to add and remove cities to your watchlist by typing /add_city and /remove_city in order to receive automatic updates every ''' +
            '''4 hours.\nEnjoy!''')

    def _helpUser(self, update: dict, context: object) -> None:
        '''
        Send help
        '''

        update.message.reply_text('''Welcome to AmbientMonitorBot! You can start to use this app typing /city_list to see''' +
            '''which cities are supported and then you can use one of the following commands:\n\n/register to start using our update system!\n\n''' + 
            '''/check_city to receive real time data from a city of your choice\n\n/get_stats to receive fancy visual statistics for a city\n\n''' +
            '''/show_brief to have a complete view of what's going on right now in every city covered by our network\n\nAfter your registration you will ''' +
            '''also be able to add and remove cities to your watchlist by typing /add_city and /remove_city in order to receive automatic updates every ''' +
            '''4 hours.\nEnjoy!''')

    def _registerUser(self, update: dict, context: object) -> None:
        '''
        Register a new user into the system. An user to be accepted must provide Name and Surname.
        If the user is already registered, this command will overwrite Name and Surname.
        '''

        # Get the User ID from the enviornment. It is the unique Telegram Chat ID
        try:
            userID = update.message.chat.id
        except KeyError:                                            # There is something wrong in the context, this message can't be processed
            self._logger.warning("Impossible to get the chat ID from the context - /register")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /register")
            return

        if userID == None:                                          # We can't register this user because we don't have the unique chat ID
            update.message.reply_text("Sorry but something went wrong, we can't get your ChatID. Reopen Telegram and try again")
            return
            

        # Try to get info about this user from the DB
        try:
            user = asyncio.run_coroutine_threadsafe(self._db.getUserInfo(userID = userID), loop = self._asyncLoop).result(60)
        except asyncio.TimeoutError:
            user = None
        except Exception:
            self._logger.warning("Something went wrong while getting info about a user - /register")
            user = None

        # Check the DB output. If this user is already registered, there is nothing to do
        if  user != ():                                             # The user already exists
            update.message.reply_text("Chill, you're already registered!")
            return
        elif user == None:                                          # Something went wrong, abort this command
            update.message.reply_text("Sorry... Something went worng. At the moment I can't handle your request, please try again later")
            return

        if userID == None:                                          # We couldn't get the chat ID, let's try to notify the user
            update.message.reply_text("Something went wrong... Please try again")
        else:                                                       # We have the char ID, let's get Name and Surname
            try:
                # Obtain a list of Names and Surnames (last element is the surname, others are names)
                userData = str(update.message.text).replace("/register", "").split()
                userData = list(map(lambda word: word.strip().capitalize(), userData))
            except Exception:
                self._logger.warning("Impossible to get Name and Surname from the message - /register")
                userData = list()

            if len(userData) >= 2:                                  # If we have got both Name and Surname the registration is done
                userName = "".join(userData[0:-1])                  # Split the Names from the Surname
                userSurname = userData[-1]
                update.message.reply_text("Please wait...")         # Notify the user that the process is going on
                
                # Insert into the DB this new user
                try:
                    asyncio.run_coroutine_threadsafe(self._db.insertData(tableName = "People", data = {userID : {"name" : userName, "surname" : userSurname}}), loop = self._asyncLoop)
                except Exception:
                    update.message.reply_text("Sorry but the registration has failed, try again")
                    return

                # Notify the user that everything worked out
                update.message.reply_text("Registration done! Welcome!")
            else:                                                   # The user didn't provide Name and Surname
                self._activeUserStateLock.acquire()
                self._activeUserState[userID] = "register"          # Save this user's state in order to keep processing this command when the next message arrives
                self._activeUserStateLock.release()
                update.message.reply_text("Please provide your name and surname separated with a whitespace")

        return

    def _cityList(self, update: dict, context: object) -> None:
        '''
        Send a list of supported cities
        '''

        # Get a list of registered sensors
        try:
            cityList = asyncio.run_coroutine_threadsafe(self._db.getCityList(), loop = self._asyncLoop).result(60)
        except asyncio.TimeoutError:
            cityList = None
        except Exception:
            self._logger.warning("Impossible to get a city list")
            cityList = None

        # Check the result
        if cityList == tuple():                                     # No city is currently supported
            update.message.reply_text("There is no city currently active in our network... Try again tomorrow! We are working hard right now")
        elif cityList == None:                                      # An error occurred
            update.message.reply_text("Sorry... Something went wrong. I couldn't get the list of supported cities you're waiting for...")
        else:                                                       # We have the list
            message = "The following cities are currently supported:\n"

            # Build the message and then send it
            for city in cityList:
                message = message + "{city} - {country}\n".format(city = city[0], country = city[1])

            update.message.reply_text(message)

    def _newSub(self, update: dict, context: object) -> None:
        '''
        Allow an existing user to register for update to one or more supported cities
        '''

        # Get the User ID from the enviornment. It is the unique Telegram Chat ID.
        try:
            userID = update.message.chat.id
        except KeyError:                                            # There is something wrong in the context, this message can't be processed
            self._logger.warning("Impossible to get the chat ID from the context - /register")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /register")
            return

        if userID == None:                                          # We can't proceed without the unique chat ID
            update.message.reply_text("Sorry but something went wrong, we can't get your ChatID. Reopen Telegram and try again")
            return

        # Try to get info about this user from the DB
        try:
            user = asyncio.run_coroutine_threadsafe(self._db.getUserInfo(userID = userID), loop = self._asyncLoop).result(60)
        except asyncio.TimeoutError:
            user = None
        except Exception:
            self._logger.warning("Something went wrong while getting info about a user - /register")
            user = None

        if user == None:                                            # The DB didn't provide us informations
            update.message.reply_text("Sorry, apparently our system is overloaded at the moment... Please wait a few minutes and try again")
            return
        elif user == tuple():                                       # This user is not registered in our system
            update.message.reply_text("To use this function, you need to be registered! Please type /register")
            return
        else:
            try:
                # Obtain a list of cities
                userData = str(update.message.text).replace("/add_city", "").replace(",", "").replace(";", "").split()
                userData = list(map(lambda word: word.strip().capitalize(), userData))
            except Exception:
                self._logger.warning("Impossible to get a list of cities from the message - /add_city")
                userData = list()

            if userData == list():                                  # The user didn't provice any city, save this active state
                self._activeUserStateLock.acquire()
                self._activeUserState[userID] = "add_city"          # Save this user's state in order to keep processing this command when the next message arrives
                self._activeUserStateLock.release()
                update.message.reply_text("Please provide a list of cities that you want to subscribe to, separated with a whitespace\nType /city_list so see which cities are supported")
            else:                                                   # The user typed something inline
                # Get a list of registered sensors
                try:
                    cityList = asyncio.run_coroutine_threadsafe(self._db.getCityList(), loop = self._asyncLoop).result(60)
                except asyncio.TimeoutError:
                    cityList = None
                except Exception:
                    self._logger.warning("Impossible to get a city list")
                    cityList = None

                if cityList == None:                                # Impossible to get a list of cities so we can't go on
                    update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                    return
                else:                                               # We have the cities' list
                    valid = set()                                   # Keep track of valid cities to send a resume

                    try:
                        for city in cityList:                           # Compare each city to see if the user selected it
                            if city[0] in userData:                     # The user tyed this city
                                valid.add(city)                         # This city will be associated to this user
                                userData.remove(city[0])                # Remove this city from his list
                    except Exception:
                        self._logger.warning("An error occurred while checking the prompted cities - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return

                    # Before inserting the new valid cities into the DB, check if this user was already registered to any of them
                    try:
                        existingUpdates = asyncio.run_coroutine_threadsafe(self._db.getUpdateListByUser(userID = int(userID)), loop = self._asyncLoop).result(60)
                    except asyncio.TimeoutError:
                        update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                        return
                    except Exception:
                        self._logger.warning("Impossible to insert into Updates table - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return

                    # Add to the list of valid cities also the ones already registered for this user
                    for city in existingUpdates:
                        valid.add(city)

                    # Build the resume message
                    try:
                        answerMessage = "You have subscribed to these cities:\n"
                        for city in valid:
                            answerMessage = answerMessage + "- {cityName}\n".format(cityName = str(city[0]))

                        if userData != list():                      # There are cities that are not a valid choice (they are not in our network)
                            answerMessage = answerMessage + "\nUnfortunately the following cities are not supported yet...\n"
                            for city in userData:
                                answerMessage = answerMessage + "- {cityName}\n".format(cityName = str(city))
                    except Exception:
                        self._logger.warning("An error occurred while making the resume message - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return



                    # Insert data into DB
                    try:
                        validCities = dict()                        # Build a data structure suitable for the DB call
                        entry = 0

                        for city in valid:                          # Fulfill the data structure with only valid cities
                            if city not in existingUpdates:         # Only if this association doesn't already exists
                                validCities[entry] = {"person" : int(userID), "sensor" : int(city[2])}
                                entry = entry + 1

                        # Try to update the DB
                        if asyncio.run_coroutine_threadsafe(self._db.insertData(tableName = "Updates", data = validCities), loop = self._asyncLoop).result(60) != True:
                            self._logger("Impossible to save the new updates preferencies - /add_city")
                            update.message.reply_text("Sorry, something went wrong, we couldn't handle your request... Please try again")
                        else:
                            update.message.reply_text(answerMessage)
                    except asyncio.TimeoutError:
                        update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                    except Exception:
                        self._logger.warning("Impossible to insert into Updates table - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")

                    return


    def _removeSub(self, update: dict, context: object) -> None:
        pass

    def _getUpdate(self, update: dict, context: object) -> None:
        pass

    def _getStats(self, update: dict, context: object) -> None:
        pass

    def _showBrief(self, update: dict, context: object) -> None:
        pass

    def _processText(self, update: dict, context: object) -> None:
        '''
        Process text messages that are not commands
        '''

        userID = update.message.chat.id                                         # Get the chat ID

        if userID not in self._activeUserState.keys():                          # The user has no active state, this message means nothing
            update.message.reply_text("Sorry but I've not understood... Type /help if you're getting lost")
        else:                                                                   # The user has an active state
            if self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[2]:       # Register command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                       # Remove the active state for this user
                self._activeUserStateLock.release()

                try:
                    # Obtain a list of Names and Surnames (last element is the surname, others are names)
                    userData = str(update.message.text).replace("/register", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get Name and Surname from the message - /register")
                    userData = list()

                if len(userData) >= 2:                                  # If we have got both Name and Surname the registration is done
                    userName = "".join(userData[0:-1])                  # Split the Names from the Surname
                    userSurname = userData[-1]
                    update.message.reply_text("Please wait...")         # Notify the user that the process is going on
                    
                    # Insert into the DB this new user
                    try:
                        asyncio.run_coroutine_threadsafe(self._db.insertData(tableName = "People", data = {userID : {"name" : userName, "surname" : userSurname}}), loop = self._asyncLoop)
                    except Exception:
                        update.message.reply_text("Sorry but the registration has failed, try again")
                        return

                    # Notify the user that everything worked out
                    update.message.reply_text("Registration done! Welcome!")
                else:                                                   # The user didn't provide Name and Surname
                    update.message.reply_text("The info you've provided are not valid. Please try again")
            elif self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[4]:       # Add city command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                       # Remove the active state for this user
                self._activeUserStateLock.release()
                
                try:
                    # Obtain a list of cities
                    userData = str(update.message.text).replace("/add_city", "").replace(",", "").replace(";", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get a list of cities from the message - /add_city")
                    update.message.reply_text("Sorry, you didn't provide a valid list of cities information. Try again")
                    return

                # Get a list of registered sensors
                try:
                    cityList = asyncio.run_coroutine_threadsafe(self._db.getCityList(), loop = self._asyncLoop).result(60)
                except asyncio.TimeoutError:
                    cityList = None
                except Exception:
                    self._logger.warning("Impossible to get a city list")
                    cityList = None

                if cityList == None:                                # Impossible to get a list of cities so we can't go on
                    update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                    return
                else:                                               # We have the cities' list
                    valid = set()                                   # Keep track of valid cities to send a resume

                    try:
                        for city in cityList:                           # Compare each city to see if the user selected it
                            if city[0] in userData:                     # The user tyed this city
                                valid.add(city)                         # This city will be associated to this user
                                userData.remove(city[0])                # Remove this city from his list
                    except Exception:
                        self._logger.warning("An error occurred while checking the prompted cities - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return

                    # Before inserting the new valid cities into the DB, check if this user was already registered to any of them
                    try:
                        existingUpdates = asyncio.run_coroutine_threadsafe(self._db.getUpdateListByUser(userID = int(userID)), loop = self._asyncLoop).result(60)
                    except asyncio.TimeoutError:
                        update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                        return
                    except Exception:
                        self._logger.warning("Impossible to insert into Updates table - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return

                    # Add to the list of valid cities also the ones already registered for this user
                    for city in existingUpdates:
                        valid.add(city)
                        
                    # Build the resume message
                    try:
                        answerMessage = "You have subscribed to these cities:\n"
                        for city in valid:
                            answerMessage = answerMessage + "- {cityName}\n".format(cityName = str(city[0]))

                        if userData != list():                      # There are cities that are not a valid choice (they are not in our network)
                            answerMessage = answerMessage + "\nUnfortunately the following cities are not supported yet...\n"
                            for city in userData:
                                answerMessage = answerMessage + "- {cityName}\n".format(cityName = str(city))
                    except Exception:
                        self._logger.warning("An error occurred while making the resume message - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                        return



                    # Insert data into DB
                    try:
                        validCities = dict()                        # Build a data structure suitable for the DB call
                        entry = 0

                        for city in valid:                          # Fulfill the data structure with only valid cities
                            if city not in existingUpdates:         # Only if this association doesn't already exists
                                validCities[entry] = {"person" : int(userID), "sensor" : int(city[2])}
                                entry = entry + 1

                        # Try to update the DB
                        if asyncio.run_coroutine_threadsafe(self._db.insertData(tableName = "Updates", data = validCities), loop = self._asyncLoop).result(60) != True:
                            self._logger("Impossible to save the new updates preferencies - /add_city")
                            update.message.reply_text("Sorry, something went wrong, we couldn't handle your request... Please try again")
                        else:
                            update.message.reply_text(answerMessage)
                    except asyncio.TimeoutError:
                        update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                    except Exception:
                        self._logger.warning("Impossible to insert into Updates table - /add_city")
                        update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")

                    return
            else:                                                               # This active state is not actually supported
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                       # Delete this weird state
                self._activeUserStateLock.release()
                update.message.reply_text("I'm a bit lost... Could you please start again the thing you are trying to do?")



    async def _sendPeriodicUpdate(self) -> None:
        '''
        Task that sends updates to every subscribed user
        '''

        while self._isRunning == True:                                          # Of course if the thread is dead, this routine must return as well but we expect to cancel the task anyway
            
            try:
                # This task must wake up every "botUpdatePeriod" hours to send all the updates
                await asyncio.sleep(delay = self._system.settings["botUpdatePeriod"] * 3600)
            except asyncio.CancelledError:                                      # If this task get's cancellated it means that this thread is dead so let's break the loop
                self._logger.debug("Periodic Telegram Task cancelled")
                break

            userList = await self._db.getUserList()                                     # Get a list of subscribed users
            for user in userList.keys():                                                # For each user
                cityList = await self._db.getSensorsByUser(ID = user)                   # Get a list of associated cities
                updatePacket = await self._db.getUpdateByCity(cities = cityList)        # For all the associated cities, get the most recent record

                # Avendo Nome, Cognome e chat ID dentro a userList[user], inviare un messaggio con i dati raccolti
    
    def stopThread(self) -> None:
        '''
        Close this Telegram Bot Thread
        '''
        if self._isRunning == True:                                                     # If this thread is still alive
            try:
                if self._asyncLoop != None:                                             # If the Async loop exists and it's running
                    if self._periodicUpdateTask != None:                                # If also the periodic Task exists and it's running, cancel it
                        self._asyncLoop.call_soon_threadsafe(self._periodicUpdateTask.cancel)
                    self._asyncLoop.stop()                                              # Stop the event loop
            except Exception as e:                                                      # Something really unexpected happened, propagate the exception
                self._logger.critical("Impossible to gracefully terminate the Telegram Bot event loop!", exc_info = True)
                raise e

            try:
                self._updater.stop()                                                    # Shut the Telegram Bot down
                self._db.close()                                                        # Close the connection to DB
                self._isRunning = False                                                 # This thread is officially dead
            except Exception as e:
                self._logger.critical("Impossible to gracefully terminate the Telegram Bot thread!", exc_info = True)
                raise e
        return

    def run(self) -> None:
        
        # Open the DB handler. In case of error, it's not possible to keep going
        try:
            if self._db.status == False:
                if self._db.open() == False:
                    raise Exception
        except Exception:
            self._logger.critical("Impossible to open the DB Handler for Telegram bot")
            self.stopThread()
            return

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

        # Add also a text message handler to deal with spare messages
        try:
            self._dispatcher.add_handler(MessageHandler(Filters.text, self._processText, run_async = True))
        except Exception:
            self._logger.critical("Impossible to register the message filter handlert to the Telegram Bot's dispatcher", exc_info = True)
            self._isRunning = False

        # Create the Async event loop for this thread and create the main bot task as well as the periodic update task
        try:
            self._asyncLoop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._asyncLoop)
            self._periodicUpdateTask = self._asyncLoop.create_task(self._sendPeriodicUpdate())
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
