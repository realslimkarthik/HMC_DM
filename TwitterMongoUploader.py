import string
import json
import sys
import os
import random
import re
import Tkinter as tk
import ConfigParser
from pymongo import MongoClient, errors, ASCENDING
from datetime import datetime
import time
import logging

monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}


class TwitterMongoUploader(object):

    """
        Class that handles uploading data into Mongo, Object has the following properties:

        Attributes:
            year: Year in which the data was fetched
            month: Month in which data was fetched
            rules: Dictionary that provides Rule to Rule Index mapping
            tags: Dictionary that provides Tag to Tag Index mapping
            conf: ConfigParser object for some control information
            fields: List of fields that we keep from raw JSON and their corresponding mapping to new field names
            mongoConf: Mapping of fields from generated keys to optimized keys for Mongo Collections
            src: Path to where the input Source files are to be read from
            dest: Path to the Destination where the output files are to be written
            mongoClient: MongoClient instance used to communicate with the MongoDB instance
            collName: Name of the collection to which data is to be uploaded

    """


    def __init__(self, year, month, rules, tags):
        self.year = year
        self.month = month
        self.rules = rules
        self.tags = tags


        self.conf = ConfigParser.ConfigParser()
        self.conf.read('config\\config.cfg')
        conf_path = self.conf.get('conf', 'conf_path')
        
        self.fields = ConfigParser.ConfigParser()
        self.fields.read(conf_path.format('fields.cfg'))

        self.mongoConf = ConfigParser.ConfigParser()
        self.mongoConf.read(conf_path.format('fieldsToMongo.cfg'))
        
        self.src = conf.get('twitter', 'prod_src_path').format(self.year + monthToNames[self.month])
        self.dest = conf.get('twitter', 'prod_dest_path').format(self.year + monthToNames[self.month])


        host = self.conf.get('mongo', 'host')
        port = int(self.conf.get('mongo', 'port'))
        self.mongoClient = MongoClient(host, port)
        self.collName = self.month + self.year
        logging.basicConfig(filename=logs.format('prodUpload' + self.collName +'.log'), level=logging.DEBUG)





    def iterateOverFiles(self):
        fileList = os.listdir(self.src)
        # Iterate over every file in the source directory
        for j in fileList:
            # If it's a by-day file in the source directory it will have 3 parts around the '-'s
            if len(j.split('_')) == 3 and '.json' in j:
                # Extract the date of the corresponding file from it's name
                fileDate = int(j.split('_')[-1].split('.')[0])
                logging.info("Started uploading " + j)
                # Upload to the corresponding Mongo Collection based on the date extracted from the file
                if fileDate < 6:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_1")
                elif fileDate > 5 and  fileDate < 11:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_2")
                elif fileDate > 13 and fileDate < 16:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_3")
                elif fileDate > 15 and fileDate < 21:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_4")
                elif fileDate > 20 and fileDate < 26:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_5")
                elif fileDate > 25 and fileDate < 32:
                    self.CSVfromTwitterJSON(self.src + j, self.collName + "_6")



    # Populates the CSV. Gets CSV's file handle from caller
    def CSVfromTwitterJSON(self, jsonfilename, collName, errorfile=None, overwrite=False):
        if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
            jsonfile = open(jsonfilename, 'r')
            # List to hold a dictionary for each processed tweet
            tweetList = []
            
            # List to track all variables seen across all tweets in the file
            mykeys = []

            for line in jsonfile:
                myline = string.strip(line)
                if myline != "":
                    #For each tweet in the file, decode the weird characters without complaining
                    myline = myline.decode("utf-8", "ignore")
                    
                    #Remove new lines from within the tweet
                    myline = myline.replace('\\n' ' ')
                    #Remove carriage returns from within the tweet
                    myline = myline.replace('\\r' ' ')
                    #Remove problematic \s
                    myline = myline.replace('\\\\' ' ')
                    myline = myline.replace('\\ ' ' ')

                    #Create a dictionary using the JSON processor
                    try:
                        tweet = json.loads(myline)
                    except ValueError as e:
                        if errorfile != None:
                            write(jsonfilename+"\n"+myline+"\n"+e+"\n\n")
                        else:
                            print e
                    else:
                        #Find the summary count
                        if "Replay Request Completed" in myline:
                            print tweet['info']['activity_count']
                        else:
                            #Create an empty dictionary
                            tweetObj = {}
                            #Send the JSON dictionary, the empty dictionary, and the list of all keys
                            extract(self, tweet, tweetObj, mykeys)
                            #Add the output dictionary to the list
                            populateMongo(self, tweetObj, collName, ruleConf, tagConf) # , configData)
                            
            #Print the number of tweets processed
            jsonfile.close()
            print "Finished populating collection ", collName

    # Function to map json key name to user defined key name
    def replaceKey(self, key):
        if self.fields.has_option("fields", key):
            return self.fields.get("fields", key)
        else:
            return ""


    #Recursive function to process the input dictionary
    def extract(self, DictIn, Dictout, keys, nestedKey=""):

        # Explicitly adding keys to Dictout
        if nestedKey == "twitter_entities_user_mentions":
            Dictout["entitiesusrmentions"] = []
            mentionSet = set()
            inObj = {}
            for i in DictIn:
                inObj['is'] = i['id_str']
                inObj['n'] = i['name']
                inObj['sn'] = i['screen_name']
                Dictout["entitiesusrmentions"].append(inObj)
                inObj = {}
        elif "coordinates" in nestedKey:
            newKey = self.replaceKey(nestedKey)
            if newKey != "":
                Dictout[newKey] = DictIn
        
        # If DictIn is a dictionary
        elif isinstance(DictIn, dict):
            #Process each entry
            for key, value in DictIn.iteritems():
                #If nested, prepend the previous variables
                if nestedKey != "":
                    mykey = nestedKey+"_"+key
                else:
                    mykey = key
                # If value is a dictionary or a list
                if isinstance(value, dict) or isinstance(value, list):
                    extract(self, value, Dictout, keys, nestedKey=mykey)
                else: #Value is just a string
                    newKey = self.replaceKey(mykey)
                    if newKey == "":
                        continue
                    value = value.strip()
                    if value != "":
                        #If this is a new variable, add it to the list
                        if not newKey in keys:
                            keys.append(newKey)
                        #Add it to the output dictionary
                        if not newKey in Dictout:
                            Dictout[newKey] = value
                        else:
                            Dictout[newKey] = unicode(Dictout[newKey])+"; "+unicode(value)
                    else:
                        if not newKey in keys:
                            keys.append(newKey)
                        if not newKey in Dictout:
                            Dictout[newKey] = ""

        #If DictIn is a list, call extract on each member of the list
        elif isinstance(DictIn, list):
            newKey = replaceKey(nestedKey)
            if newKey != "":
                if not newKey in keys:
                    keys.append(newKey)
                #Add it to the output dictionary
                if not newKey in Dictout:
                    Dictout[newKey] = value
            else:
                for value in DictIn:
                    extract(self, value,Dictout,keys,nestedKey=nestedKey)
        #If DictIn is a string, check if it is a new variable and then add to dictionary
        else:
            if isinstance(DictIn, unicode) or isinstance(DictIn, str):
                newKey = self.replaceKey(nestedKey)
                if newKey == "":
                    return
                if isinstance(DictIn, unicode) or isinstance(DictIn, str):
                    DictIn = DictIn.strip()
                if DictIn != "":
                    if not newKey in keys:
                        keys.append(newKey)
                    if not newKey in Dictout:
                        Dictout[newKey] = DictIn
                    else:
                        Dictout[newKey] = unicode(Dictout[newKey])+"; "+unicode(DictIn)
                else:
                    if not newKey in keys:
                        keys.append(newKey)
                    if not newKey in Dictout:
                        Dictout[newKey] = ""

    
    def populateMongo(self, inputTweet, collName):
        # Creating a new MongoDB client
        client = self.mongoClient
        db = client.twitter
        collection = db[collName]

        collection.ensure_index([("mrv", ASCENDING)])
        collection.ensure_index([("mrt", ASCENDING)])
        #packing the entitieshtagstext field into an array
        if 'entitieshtagstext' not in inputTweet:
            inputTweet['entitieshtagstext'] = []
        else:
            inputTweet['entitieshtagstext'] = inputTweet['entitieshtagstext'].split(';')

        # Renaming id field
        inputTweet['_id'] = inputTweet['Idpost'].split(':')[2]
        # Remove the former id key
        inputTweet.pop('Idpost', None)
        # packing the matchingrulesvalue field into an array
        inputTweet['matchingrulesvalue'] = inputTweet['matchingrulesvalue'].split(';')
        inputTweet['matchingrulestag'] = inputTweet['matchingrulestag'].split(';')

        # Changing postedTime into ISO format for processing using JavaScript in Mongo
        dateFrag = inputTweet['postedTime'].split('T')
        dateFrag[1] = dateFrag[1].split('.')[0]
        dateStr = ''.join(' ' + d for d in dateFrag).strip()
        timeStruct = time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
        dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
        inputTweet['postedTime'] = dateObj
        
        # Mapping the rules into integers from the rules
        ruleIndex = []
        for j in inputTweet['matchingrulesvalue']:
            try:
                ruleIndex.append(int(self.rules[j.strip()]))
            except KeyError:
                logging.warning("Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip())
                logging.debug(str(inputTweet))
                print "Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip()

        tagIndex = set()
        for j in inputTweet['matchingrulestag']:
            if j.strip() == 'LCC':
                tag = 'cigar/cigarillo'
            else:
                tag = j.lower().strip()
            try:
                tagIndex.add(int(self.tags[tag]))
            except KeyError:
                logging.warning("Invalid tag fetched via GNIP with _id=" + inputTweet['_id'] + " with tag=" + j.strip())
                logging.debug(str(inputTweet))
                print "Invalid tag fetched via GNIP with _id=" + inputTweet['_id'] + " with tag=" + j.strip()

        # Remove the former matchingrulesvalue key
        inputTweet['matchingrulesvalue'] = ruleIndex
        inputTweet['matchingrulestag'] = list(tagIndex)
        # Initialize a new Dict to hold the record that needs to be uploaded into the corresponding Mongo Collection
        newRecord = {}
        
        # Iterate through each of the keys in the original Dict
        for (key, val) in inputTweet.iteritems():
            # Get the new translated key from the mongoConf dict
            newKey = self.mongoConf.get('fields', key)
            # create a new with translated field names to upload onto the corresponding Mongo Collection
            newRecord[newKey] = val

        # Attempt to insert record into the collection. If it fails, do an update
        try:
            collection.insert(newRecord)
        except errors.DuplicateKeyError:
            oldRecord = collection.find({'_id': newRecord['_id']})
            for i in oldRecord:
                mrv = set(newRecord['mrv'] + i['mrv'])
                if i['mrt'] is not None:
                    mrt = set(newRecord['mrt'] + i['mrt'])
                else:
                    mrt = set(newRecord['mrt'])
            newRecord['mrv'] = list(mrv)
            newRecord['mrt'] = list(mrt)
            collection.save(newRecord)