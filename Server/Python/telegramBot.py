import os, datetime, threading, logging, asyncio, pytz
from matplotlib import use
from typing import AsyncContextManager, Counter
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.ext.dispatcher import run_async
from telegram.ext.filters import MessageFilter
import matplotlib.pyplot as plt
from interfaces import Data, Event, System
from db import MySQL

class TelegramBot(threading.Thread):
    _AUTH_TOKEN = "1830886980:AAEY8fGLTn9uY0qLKhaZ7T29HgLMuMlU1Yo"
    _DEFAULT_TELEGRAM_BOT_SETTINGS = {"botUpdatePeriod" : 4}
    _SUPPORTED_TELEGRAM_COMMANDS = ["start","help", "register", "city_list", "add_city", "remove_city", "check_city", "get_stats", "show_brief"]

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
            self._SUPPORTED_TELEGRAM_COMMANDS[0]    :   self._welcomeUser,
            self._SUPPORTED_TELEGRAM_COMMANDS[1]    :   self._helpUser,
            self._SUPPORTED_TELEGRAM_COMMANDS[2]    :  self._registerUser,
            self._SUPPORTED_TELEGRAM_COMMANDS[3]    :   self._cityList,
            self._SUPPORTED_TELEGRAM_COMMANDS[4]    :   self._newSub, 
            self._SUPPORTED_TELEGRAM_COMMANDS[5]    :   self._removeSub,
            self._SUPPORTED_TELEGRAM_COMMANDS[6]    :   self._getUpdate, 
            self._SUPPORTED_TELEGRAM_COMMANDS[7]    :   self._getStats, 
            self._SUPPORTED_TELEGRAM_COMMANDS[8]    :   self._showBrief
        }

        for item in self._DEFAULT_TELEGRAM_BOT_SETTINGS.keys():
            if item not in self._system.settings:
                self._system.updateSettings(newSettings = {item : self._DEFAULT_TELEGRAM_BOT_SETTINGS[item]})

        # Timezone object
        self._tz = pytz.timezone("Europe/Rome")

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
                self._processRegister(update = update, context = context, userID = userID, userData = userData)
            else:                                                   # The user didn't provide Name and Surname
                self._activeUserStateLock.acquire()
                self._activeUserState[userID] = "register"          # Save this user's state in order to keep processing this command when the next message arrives
                self._activeUserStateLock.release()
                update.message.reply_text("Please provide your name and surname separated with a whitespace")

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
            self._logger.warning("Impossible to get the chat ID from the context - /add_city")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /add_city")
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
            self._logger.warning("Something went wrong while getting info about a user - /add_city")
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
            else: 
                self._processNewSub(update = update, context = context, userID = userID, userData = userData)

    def _removeSub(self, update: dict, context: object) -> None:
        '''
        Allow an existing user to remove one or more cities from its update list
        '''

        # Get the User ID from the enviornment. It is the unique Telegram Chat ID.
        try:
            userID = update.message.chat.id
        except KeyError:                                            # There is something wrong in the context, this message can't be processed
            self._logger.warning("Impossible to get the chat ID from the context - /remove_city")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /remove_city")
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
            self._logger.warning("Something went wrong while getting info about a user - /remove_city")
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
                userData = str(update.message.text).replace("/remove_city", "").replace(",", "").replace(";", "").split()
                userData = list(map(lambda word: word.strip().capitalize(), userData))
            except Exception:
                self._logger.warning("Impossible to get a list of cities from the message - /remove_city")
                userData = list()

            if userData == list():                                  # The user didn't provice any city, save this active state
                self._activeUserStateLock.acquire()
                self._activeUserState[userID] = "remove_city"       # Save this user's state in order to keep processing this command when the next message arrives
                self._activeUserStateLock.release()
                update.message.reply_text("Please provide a list of cities that you want to remove, separated with a whitespace\nType /city_list so see which cities are supported")
            else:                                                   # The user typed something inline
                self._processRemoveSub(update = update, context = context, userID = userID, userData = userData)

    def _getUpdate(self, update: dict, context: object) -> None:
        '''
        Send a real time update for the specified city without subscribing to it
        '''

        # Get the User ID from the enviornment. It is the unique Telegram Chat ID.
        try:
            userID = update.message.chat.id
        except KeyError:                                            # There is something wrong in the context, this message can't be processed
            self._logger.warning("Impossible to get the chat ID from the context - /check_city")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /check_city")
            return

        if userID == None:                                          # We can't proceed without the unique chat ID
            update.message.reply_text("Sorry but something went wrong, we can't get your ChatID. Reopen Telegram and try again")
            return

        try:
            # Obtain a list of cities
            userData = str(update.message.text).replace("/check_city", "").replace(",", "").replace(";", "").split()
            userData = list(map(lambda word: word.strip().capitalize(), userData))
        except Exception:
            self._logger.warning("Impossible to get a list of cities from the message - /check_city")
            userData = list()

        if userData == list():                                  # The user didn't provide any city
            self._activeUserStateLock.acquire()
            self._activeUserState[userID] = "check_city"        # Save this user's state in order to keep processing this command when the next message arrives
            self._activeUserStateLock.release()
            update.message.reply_text("Please type the name of a city. To see a list of available cities type /city_list")
        else:
            self._processGetUpdate(update = update, context = context, userID = userID, userData = userData)
            return
 
    def _getStats(self, update: dict, context: object) -> None:

        # Get the User ID from the enviornment. It is the unique Telegram Chat ID.
        try:
            userID = update.message.chat.id
        except KeyError:                                            # There is something wrong in the context, this message can't be processed
            self._logger.warning("Impossible to get the chat ID from the context - /get_stats")
            userID = None
        except Exception:                                           # There is something really wrong here, abort this callback
            self._logger.error("Unexpected error occurred while getting the chat ID from the context - /get_stats")
            return

        if userID == None:                                          # We can't proceed without the unique chat ID
            update.message.reply_text("Sorry but something went wrong, we can't get your ChatID. Reopen Telegram and try again")
            return

        try:
            # Obtain a list of cities
            userData = str(update.message.text).replace("/get_stats", "").replace(",", "").replace(";", "").split()
            userData = list(map(lambda word: word.strip().capitalize(), userData))
        except Exception:
            self._logger.warning("Impossible to get a list of cities from the message - /get_stats")
            userData = list()

        if userData == list():                                  # The user didn't provide any city
            self._activeUserStateLock.acquire()
            self._activeUserState[userID] = "get_stats"         # Save this user's state in order to keep processing this command when the next message arrives
            self._activeUserStateLock.release()
            update.message.reply_text("Please type the name of one or more cities. To see a list of available cities type /city_list")
        else:
            self._processGetStats(update = update, context = context, userID = userID, userData = userData)
            return

    def _showBrief(self, update: dict, context: object) -> None:
        '''
        Show updates for every city in our system
        '''

        # Get a list of cities recorded in our system and the most recent record for each of them
        try:
            cityList = asyncio.run_coroutine_threadsafe(self._db.getCityList(), loop = self._asyncLoop).result(60)
            updatePacket = asyncio.run_coroutine_threadsafe(self._db.getUpdateByCity(cities = cityList), loop = self._asyncLoop).result(60)
        except Exception:
            self._logger.warning("Impossible to fetch data - /show_brief", exc_info = True)
            update.message.reply_text("Sorry, impossible to process this request at the moment. We are already investigating")
            return

        # Prepare the update message
        updateMessage = "Complete, real-time view of our network:\n\n"

        # For each record
        for element in updatePacket:

            try:
                # Prepare the data we'll need for the message
                time = datetime.datetime.fromtimestamp(element[0], tz = self._tz)
                for sensor in cityList:
                    if sensor[2] == element[1]:
                        city = sensor
            except Exception:
                self._logger.warning("Impossible to prepare data for a periodic message", exc_info = True)
                continue

            try:
                updateMessage = updateMessage + "{city} - {country} - {day}/{month}/{year} {hour}:{min}\n".format(city = city[0], country = city[1], 
                    day = time.day, month = time.month, year = time.year, hour = time.hour, min = time.minute)

                updateMessage = updateMessage + "Pressure: {pre} Pa\nTemperature: {temp}°C\nHumidity {hum}%\nLuminosity: {lux} Lux\n\n".format(pre = round(updatePacket[element]["Pressure"], 1),
                temp = round(updatePacket[element]["Temperature"], 1), hum = round(updatePacket[element]["Humidity"], 1), lux = round(updatePacket[element]["Ligth"], 3))
            except Exception:
                self._logger.warning("An error occurred while formatting the city info for an updateMessage", exc_info = True)
                continue

        try:
            update.message.reply_text(updateMessage)
        except Exception:
            self._logger.warning("An error occurred while sending the update message - /show_brief", exc_info = True)
   
    def _processRegister(self, update: dict, context: object, userID: int, userData: list) -> None:
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

    def _processNewSub(self, update: dict, context: object, userID: int, userData: list) -> None:
            
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

    def _processRemoveSub(self, update: dict, context: object, userID: int, userData: list) -> None:
        
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
                self._logger.warning("An error occurred while checking the prompted cities - /remove_city")
                update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                return
        
            # Get a list of cities associated to this user
            try:
                existingUpdates = asyncio.run_coroutine_threadsafe(self._db.getUpdateListByUser(userID = int(userID)), loop = self._asyncLoop).result(60)
                existingUpdates = set(existingUpdates)
            except asyncio.TimeoutError:
                update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                return
            except Exception:
                self._logger.warning("Impossible to insert into Updates table - /remove_city")
                update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                return

            # Build a list of cities to remove by subtracting the propmted cities to the list of existing updates
            removedCities = set()
            for city in valid:                                  # For each valid city given by the user
                if city in existingUpdates:                     # Check if the user was registered to that city
                    existingUpdates.remove(city)                # If yes, remove it from the list of cities he's registered to
                    removedCities.add(city)                     # Put in the list of removed cities

            # Build the resume message
            try:
                if existingUpdates != set():
                    answerMessage = "All done!\nYou are now subscribed to these cities:\n"
                    for city in existingUpdates:
                        answerMessage = answerMessage + "- {cityName}\n".format(cityName = str(city[0]))
                else:
                    answerMessage = "All done!\nYou are not subscribed to any city"
            except Exception:
                self._logger.warning("An error occurred while making the resume message - /remove_city")
                update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                return

            # Remove the selected cities from the DB
            try:
                # Try to update the DB
                for city in removedCities:
                    if (asyncio.run_coroutine_threadsafe(self._db.removeData(tableName = "Updates", 
                        options = {"person" : int(userID), "sensor" : int(city[2])}), loop = self._asyncLoop).result(60) != True):
                        self._logger("Impossible to save the new updates preferencies - /add_city")
                        update.message.reply_text("Sorry, something went wrong, we couldn't handle your request... Please try again")
                        return
            except asyncio.TimeoutError:
                update.message.reply_text("Sorry, our system is overloaded at the moment. Please try again later")
                return
            except Exception:
                self._logger.warning("Impossible to remove data from into Updates table - /add_city", exc_info = True)
                update.message.reply_text("Sorry, an error occurred. We are already investigating the issue. Please try again later")
                return

            update.message.reply_text(answerMessage)
            return

    def _processGetUpdate(self, update: dict, context: object, userID: int, userData: list) -> None:
        
        requestedCity = userData[0]                         # Only one city will be handled

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
        else:                                               # We have a list of valid sensors
            for city in cityList:                           # Check if the requested city is available
                if city[0] == requestedCity:                # Matched
                    requestedCity = city
                    break
            
            if type(requestedCity) != tuple:                # We didn't find the requested city
                update.message.reply_text("Sorry, the city you have requested is not available in our system yet")
                return

            try:
                cityRecord = asyncio.run_coroutine_threadsafe(self._db.getUpdateByCity(cities = (requestedCity,)), loop = self._asyncLoop).result(60)
            except asyncio.TimeoutError:
                cityRecord = None
            except Exception:
                self._logger.warning("Impossible to get a city list")
                cityRecord = None

            if cityRecord == None:
                update.message.reply_text("There are no data for the city of {cityName}...\nPlease try again later".format(cityName = requestedCity[0]))
                return
            else:
                answerMessage = "Here there is a real time update from the city of {cityName} - {countryName}\n".format(cityName = requestedCity[0], countryName = requestedCity[1])
                
                # Get the timestamp and convert it
                try:
                    index = list(cityRecord.keys())[0]
                    timestamp = index[0]
                    timestamp = datetime.datetime.fromtimestamp(timestamp, tz = self._tz)
                    answerMessage = answerMessage + "Last update: {day}/{month}/{year} {hour}:{minute}\n".format(day = timestamp.day, 
                        month = timestamp.month, year = timestamp.year, hour = timestamp.hour, minute = timestamp.minute)
                except Exception:
                    self._logger.warning("An error occurred while converting the timestamp - /check_city", exc_info = True)
                    update.message.reply_text("Sorry, an error occurred... We are already investigating")
                    return

                # Build the answer message
                try:
                    answerMessage = answerMessage + "Pressure: {pre} Pa\nTemperature: {temp}°C\nHumidity {hum}%\nLuminosity: {lux} Lux".format(pre = round(cityRecord[index]["Pressure"], 1),
                        temp = round(cityRecord[index]["Temperature"], 1), hum = round(cityRecord[index]["Humidity"], 1), lux = round(cityRecord[index]["Ligth"], 3))
                except Exception:
                    self._logger.warning("An error occurred while building the answer message - /check_city", exc_info = True)
                    update.message.reply_text("Sorry, an error occurred... We are already investigating")
                    return

                # Send the message
                update.message.reply_text(answerMessage)

    def _processGetStats(self, update: dict, context: object, userID: int, userData: list) -> None:

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
        else:
            # We have a list of valid sensors, check if the user prompted a valid list
            valid = set()
            for city in cityList:
                if city[0] in userData:
                    valid.add(city)

            valid = tuple(valid)
            try:
                daySet = asyncio.run_coroutine_threadsafe(self._db.getUpdateByCity(cities = valid, options = {"mode" : "daily", "timezone" : "Europe/Rome"}), loop = self._asyncLoop).result(60)
                weekSet = asyncio.run_coroutine_threadsafe(self._db.getUpdateByCity(cities = valid, options = {"mode" : "weekly", "timezone" : "Europe/Rome"}), loop = self._asyncLoop).result(60)
            except Exception:
                self._logger.warning("An error occurred while fetching data - /get_stats", exc_info = True)
                update.message.reply_text("Sorry, an error occurred, we are already investigating")
                return
            
            # Prepare a new data structure to store only data that will be used in the plot
            validDaySet, validWeekSet = list(), list()

            try:
                # For each record in both sets, decide wether to keep it or not. If yes, also format it
                for record in daySet:
                    tmp = {"time" : datetime.datetime.fromtimestamp(record[0], tz = self._tz), "city" : record[1], "Pressure" : round(daySet[record]["Pressure"], 1), 
                        "Temperature" : round(daySet[record]["Temperature"], 1), "Humidity" : round(daySet[record]["Humidity"], 1), "Ligth" : round(daySet[record]["Ligth"], 3)}

                    if validDaySet == list():                                       # Always keep the first record in the set
                        validDaySet.append(tmp)
                    else:                                                           # For the day set, keep a record every 4 hours or more
                        if (tmp["time"] - validDaySet[-1]["time"]) >= datetime.timedelta(hours = 4):
                            validDaySet.append(tmp)

                for record in weekSet:
                    tmp = {"time" : datetime.datetime.fromtimestamp(record[0], tz = self._tz), "city" : record[1], "Pressure" : round(weekSet[record]["Pressure"], 1), 
                        "Temperature" : round(weekSet[record]["Temperature"], 1), "Humidity" : round(weekSet[record]["Humidity"], 1), "Ligth" : round(weekSet[record]["Ligth"], 3)}

                    if validWeekSet == list():                                      # Always keep the first record in the set
                        validWeekSet.append(tmp)
                    else:                                                           # For the week set, keep a record every 12 hours or more
                        if (tmp["time"] - validWeekSet[-1]["time"]) >= datetime.timedelta(hours = 12):
                            validWeekSet.append(tmp)
            except Exception:
                self._logger.warning("An error occurred while formatting and selecting data - /get_stats", exc_info = True)
                update.message.reply_text("Sorry, an error occurred, we are already investigating")
                return


            # Prepare structures to store plotting data
            dayTimestamp, dayData = list(), {"Pressure" : list(), "Temperature" : list(), "Humidity" : list(), "Ligth": list()}
            weekTimestamp, weekData = list(), {"Pressure" : list(), "Temperature" : list(), "Humidity" : list(), "Ligth": list()}

            # For each city
            for city in valid:
                try:
                    # Fulfill those structures with the records previously selected
                    for record in validDaySet:
                        if int(record["city"]) == int(city[2]):                     # If this record is for this city
                            dayTimestamp.append(record["time"].strftime('%H:%M %d/%m/%Y'))
                            dayData["Pressure"].append(record["Pressure"])
                            dayData["Temperature"].append(record["Temperature"])
                            dayData["Humidity"].append(record["Humidity"])
                            dayData["Ligth"].append(record["Ligth"])

                    for record in validWeekSet:
                        if int(record["city"]) == int(city[2]):                     # If this record is for this city
                            weekTimestamp.append(record["time"].strftime('%H:%M %d/%m/%Y'))
                            weekData["Pressure"].append(record["Pressure"])
                            weekData["Temperature"].append(record["Temperature"])
                            weekData["Humidity"].append(record["Humidity"])
                            weekData["Ligth"].append(record["Ligth"])
                except Exception:
                    self._logger.warning("Impossible to fulfill data structures - /get_stats", exc_info = True)
                    update.message.reply_text("Sorry, an error occurred, we are already investigating")
                    return

                try:
                    constants = {"Pressure" : "Pa", "Temperature" : "°C", "Humidity" : "%", "Ligth" : "Lux"}
                    # Create the plots and send them
                    for element in constants.keys():
                        f = plt.figure(userID, figsize = (20, 10))
                        plt.clf()
                        plt.xlabel("Time")
                        plt.ylabel(constants[element])
                        plt.title("Daily - {element}".format(element = element))
                        plt.plot(dayTimestamp, dayData[element])
                        plt.savefig("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element))

                        context.bot.send_photo(chat_id = userID, photo = open("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element), "rb"))
                        os.remove("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element))

                    for element in constants.keys():
                        f = plt.figure(userID, figsize = (20, 10))
                        plt.clf()
                        plt.xlabel("Time")
                        plt.ylabel(constants[element])
                        plt.title("Weekly - {element}".format(element = element))
                        plt.plot(weekTimestamp, weekData[element])
                        plt.savefig("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element))

                        context.bot.send_photo(chat_id = userID, photo = open("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element), "rb"))
                        os.remove("../Files/tmp/{id}_{element}.png".format(id = str(userID), element = element))
                except Exception:
                    self._logger.warning("Impossible to build and send plots - /get_stats", exc_info = True)
                    update.message.reply_text("Sorry, an error occurred, we are already investigating")
                    return

    def _processText(self, update: dict, context: object) -> None:
        '''
        Process text messages that are not commands
        '''

        userID = update.message.chat.id                                         # Get the chat ID

        if userID not in self._activeUserState.keys():                          # The user has no active state, this message means nothing
            update.message.reply_text("Sorry but I've not understood... Type /help if you're getting lost")
        else:                                                                   # The user has an active state
            if self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[2]:           # Register command
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
                    self._processRegister(update = update, context = context, userID = userID, userData = userData)
                else:                                                   # The user didn't provide Name and Surname
                    update.message.reply_text("The info you've provided are not valid. Please try again")
            elif self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[4]:         # Add city command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                       # Remove the active state for this user
                self._activeUserStateLock.release()
                
                try:
                    # Obtain a list of cities
                    userData = str(update.message.text).replace("/remove_city", "").replace(",", "").replace(";", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get a list of cities from the message - /add_city")
                    update.message.reply_text("Sorry, you didn't provide a valid list of cities information. Try again")
                    return

                self._processNewSub(update = update, context = context, userID = userID, userData = userData)
            elif self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[5]:         # Remove city command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                   # Remove the active state for this user
                self._activeUserStateLock.release()

                try:
                    # Obtain a list of cities
                    userData = str(update.message.text).replace("/remove_city", "").replace(",", "").replace(";", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get a list of cities from the message - /remove_city")
                    userData = list()

                self._processRemoveSub(update = update, context = context, userID = userID, userData = userData)
            elif self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[6]:         # Check city command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                   # Remove the active state for this user
                self._activeUserStateLock.release()

                try:
                    # Obtain a list of cities
                    userData = str(update.message.text).replace("/check_city", "").replace(",", "").replace(";", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get a list of cities from the message - /check_city")
                    userData = list()

                if userData != list():                                  # The user didn't provide any city
                    self._processGetUpdate(update = update, context = context, userID = userID, userData = userData)
                else:
                    update.message.reply_text("You must provide a valid city to perform this command")
            elif self._activeUserState[userID] == self._SUPPORTED_TELEGRAM_COMMANDS[7]:         # Get stats command
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                   # Remove the active state for this user
                self._activeUserStateLock.release()

                try:
                    # Obtain a list of cities
                    userData = str(update.message.text).replace("/get_stats", "").replace(",", "").replace(";", "").split()
                    userData = list(map(lambda word: word.strip().capitalize(), userData))
                except Exception:
                    self._logger.warning("Impossible to get a list of cities from the message - /get_stats")
                    userData = list()

                if userData != list():                                  # The user didn't provide any city
                    self._processGetStats(update = update, context = context, userID = userID, userData = userData)
                else:
                    update.message.reply_text("You must provide a valid city to perform this command")
            else:                                                                               # This active state is not actually supported
                self._activeUserStateLock.acquire()
                del self._activeUserState[userID]                   # Delete this weird state
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
                try:
                    cityList = await self._db.getSensorsByUser(ID = user)                   # Get a list of associated cities
                    updatePacket = await self._db.getUpdateByCity(cities = cityList)        # For all the associated cities, get the most recent record
                except Exception:
                    self._logger.warning("Impossible to fetch data for the periodic update", exc_info = True)
                    continue

                try:
                    # Prepare the update message
                    updateMessage = "Hey {name} {surname} here it is your update!\n\n".format(name = userList[user]["Name"], surname = userList[user]["Surname"])
                except KeyError:
                    self._logger.warning("User data has a wrong format")
                    continue
                except Exception:
                    self._logger.error("An error occurred while building the updateMessage", exc_info = True)
                    continue

                # For each record
                for update in updatePacket:

                    try:
                        # Prepare the data we'll need for the message
                        time = datetime.datetime.fromtimestamp(update[0], tz = self._tz)
                        for sensor in cityList:
                            if sensor[2] == update[1]:
                                city = sensor
                    except Exception:
                        self._logger.warning("Impossible to prepare data for a periodic message", exc_info = True)
                        continue

                    try:
                        updateMessage = updateMessage + "{city} - {country} - {day}/{month}/{year} {hour}:{min}\n".format(city = city[0], country = city[1], 
                            day = time.day, month = time.month, year = time.year, hour = time.hour, min = time.minute)

                        updateMessage = updateMessage + "Pressure: {pre} Pa\nTemperature: {temp}°C\nHumidity {hum}%\nLuminosity: {lux} Lux\n\n".format(pre = round(updatePacket[update]["Pressure"], 1),
                        temp = round(updatePacket[update]["Temperature"], 1), hum = round(updatePacket[update]["Humidity"], 1), lux = round(updatePacket[update]["Ligth"], 3))
                    except Exception:
                        self._logger.warning("An error occurred while formatting the city info for an updateMessage", exc_info = True)
                        continue

                try:
                    self._dispatcher.bot.sendMessage(chat_id = int(user), text = updateMessage)
                except Exception:
                    self._logger.warning("An error occurred while sending an update message to user: " + str(user), exc_info = True)
    
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
