import string
import json
import sys
import os
import random
import re
import ConfigParser
from pymongo import MongoClient, errors, ASCENDING
from datetime import datetime
import time
import logging
import calendar


class TwitterMongoUploader(object):

    """
        Class that handles uploading data into Mongo, Object has the following properties:

        Attributes:
            year: Year in which the data was fetched
            month: Month in which data was fetched
            monthDict: Mapping of abbreviated month to month index
            _conf: ConfigParser object for some control information
            _rules: Dictionary that provides Rule to Rule Index mapping
            _max_rule: Current largest Rule Index
            _tags: Dictionary that provides Tag to Tag Index mapping
            _rules_tags: Dictionary that provides Rule to Tag mapping
            fields: List of fields that we keep from raw JSON and their corresponding mapping to new field names
            mongoConf: Mapping of fields from generated keys to optimized keys for Mongo Collections
            src: Path to where the input Source files are to be read from
            dest: Path to the Destination where the output files are to be written
            db: MongoDB Database in which to write into/read from
            collName: Name(s) of the collection to which data is to be uploaded

    """


    def __init__(self, year, month):
        self.year = str(year)
        self.month = month.lower()
        
        self.monthDict = {v.lower(): str(k).zfill(2) for (k, v) in enumerate(calendar.month_abbr)}

        self._conf = ConfigParser.ConfigParser()
        self._conf.read('config\\config.cfg')
        conf_path = self._conf.get('conf', 'conf_path')
        
        rules_file = open(self._conf.get('twitter', 'rules'))
        self._rules = json.loads(rules_file.read())
        self._max_rule = sorted(self._rules, key = lambda r: r[1])[-1][1]
        rules_file.close()

        tags_file = open(self._conf.get('twitter', 'tags'))
        self._tags = json.loads(tags_file.read())
        tags_file.close()

        rules_tags_file = open(self._conf.get('twitter', 'rules_tags'))
        self._rules_tags = json.loads(rules_tags_file.read())
        rules_tags_file.close()

        self.fields = ConfigParser.ConfigParser()
        self.fields.read(conf_path.format('fields.cfg'))

        self.mongoConf = ConfigParser.ConfigParser()
        self.mongoConf.read(conf_path.format('fieldsToMongo.cfg'))
        
        self.src = self._conf.get('twitter', 'prod_src_path').format(self.year + self.monthDict[self.month])
        self.dest = self._conf.get('twitter', 'prod_dest_path').format(self.year + self.monthDict[self.month], 'CSVRULES')


        host = self._conf.get('mongo', 'host')
        port = int(self._conf.get('mongo', 'port'))
        username = self._conf.get('mongo', 'username')
        password = self._conf.get('mongo', 'password')
        authDB = self._conf.get('mongo', 'authDB')
        mongoClient = MongoClient(host, port)
        mongoClient.twitter.authenticate(username, password, source=authDB)
        self.db = mongoClient['twitter']

        self.collName = tuple(self.month + self.year + '_' + str(i) for i in range(1, 7))
        logs = self._conf.get("conf", "prod_log_path")
        logging.basicConfig(filename=logs.format('prodUpload' + self.collName[0].split('_')[0] +'.log'), level=logging.DEBUG)


    def fixByMonth(self):
        jsonDirectory = self.src
        myfiles = []
        myfilename = ""
        jsonfile = None
        print jsonDirectory
        #Use the os package to find all files in the directory and its subdirectories
        for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
            #Add the full name of the file to the list
            myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
        #For each file in the list
        for myfile in myfiles:
            #Require .json and exclude .csv
            if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
                onefile = open(myfile, "r")
                name = myfile.split("_")
                filename = "tw"+"_".join(name[3:len(name)-2])+".json"
                #Check if still same day file
                if filename != myfilename:
                    if jsonfile != None:
                        jsonfile.close()
                    jsonfile = open(jsonDirectory+filename,"a")
                    myfilename = filename
                    print filename
                jsonfile.write(onefile.read())
                onefile.close()
        jsonfile.close()

    # Method to add new rules added in twitter_master_rules.json to our set of rules
    def updateRules(self):
        # Open and iterate through each line of the master rules file
        master_rules_file = open(self._conf.get('twitter', 'master_rules_file'))
        for line in master_rules_file.readlines():
            
            # Attempt to create a dict from each line of the file
            try:
                line_json = json.loads(line[:-2])
            except ValueError:
                continue

            # If the rule is not already in our set of rules
            if line_json['value'] not in self._rules:
                # Increment the current maximum Rule Index
                self._max_rule += 1
                # Set the new maximum Rule Index for the new Rule
                self._rules[line_json['value']] = self._max_rule
                # Map the new Rule to its corresponding Tag from the line in the file
                self._rules_tags[line_json['value']] = line_json['tag']
        
        master_rules_file.close()

        # Once the file is fully iterated through, dump the _rules and _rules_tags dict to their corresponding files
        with open('tw_rules.json', 'w') as rule_file:
            rule_file.write(json.dumps(self._rules))

        with open('tw_rules_tags.json', 'w') as rule_tag_file:
            rule_tag_file.write(json.dumps(self._rules_tags))


    # Method to go over individual files in the directory to go through the data to upload to Mongo
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
                    self.dictFromTwitterJSON(self.src + j, self.collName[0])
                elif fileDate > 5 and  fileDate < 11:
                    self.dictFromTwitterJSON(self.src + j, self.collName[1])
                elif fileDate > 13 and fileDate < 16:
                    self.dictFromTwitterJSON(self.src + j, self.collName[2])
                elif fileDate > 15 and fileDate < 21:
                    self.dictFromTwitterJSON(self.src + j, self.collName[3])
                elif fileDate > 20 and fileDate < 26:
                    self.dictFromTwitterJSON(self.src + j, self.collName[4])
                elif fileDate > 25 and fileDate < 32:
                    self.dictFromTwitterJSON(self.src + j, self.collName[5])


    # Generates an array of dicts from json files
    def dictFromTwitterJSON(self, jsonfilename, collName, errorfile=None):
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
                        self.extract(tweet, tweetObj, mykeys)
                        #Add the output dictionary to the list
                        self.populateMongo(tweetObj, collName) # , configData)
                        
        #Print the number of tweets processed
        jsonfile.close()
        print "Finished populating collection ", collName


    # Method to map json key name to user defined key name
    def replaceKey(self, key):
        if self.fields.has_option("fields", key):
            return self.fields.get("fields", key)
        else:
            return ""


    # Recursive method to process the rawTweet dictionary
    def extract(self, rawTweet, finalTweet, keys, nestedKey=""):

        # If rawTweet is a dictionary
        if isinstance(rawTweet, dict):
            # Process each entry
            for key, value in rawTweet.iteritems():
                # If nested, prepend the previous variables
                if nestedKey != "":
                    mykey = nestedKey+"_"+key
                else:
                    mykey = key
                # If value is a dictionary or a list
                if isinstance(value, dict) or isinstance(value, list):
                    self.extract(value, finalTweet, keys, nestedKey=mykey)
                else: # Value is just a string
                    newKey = self.replaceKey(mykey)
                    if newKey == "":
                        continue
                    if isinstance(value, str) or isinstance(value, unicode):
                        value = value.strip()
                    if value != "":
                        # If this is a new variable, add it to the list
                        if not newKey in keys:
                            keys.append(newKey)
                        # Add it to the output dictionary
                        if not newKey in finalTweet:
                            finalTweet[newKey] = value
                        else:
                            if isinstance(finalTweet[newKey], str) or isinstance(finalTweet[newKey], unicode):
                                finalTweet[newKey] = [unicode(finalTweet[newKey]), unicode(value)]
                            elif isinstance(finalTweet[newKey], list):
                                finalTweet[newKey].append(unicode(value))
                    else:
                        if not newKey in keys:
                            keys.append(newKey)
                        if not newKey in finalTweet:
                            finalTweet[newKey] = ""

        # If rawTweet is a list, call extract on each member of the list
        elif isinstance(rawTweet, list):
            newKey = self.replaceKey(nestedKey)
            if newKey != "":
                if not newKey in keys:
                    keys.append(newKey)
                # Add it to the output dictionary
                if not newKey in finalTweet:
                    finalTweet[newKey] = []
                    for item in rawTweet:
                        if isinstance(item, dict):
                            inObj = {}
                            for (k, v) in item.iteritems():
                                innerKey = self.replaceKey(nestedKey + '_' + k)
                                if innerKey != "":
                                    inObj[innerKey] = v
                            finalTweet[newKey].append(inObj)
                        else:
                            finalTweet[newKey] = item
            else:
                for value in rawTweet:
                    self.extract(value, finalTweet, keys, nestedKey=nestedKey)

    
    # Method to input tweet into Mongo Collection
    def populateMongo(self, inputTweet, collName):
        # Obtaining a reference to the MongoDB Collection
        collection = self.db[collName]

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
                ruleIndex.append(int(self._rules[j.strip()]))
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
                tagIndex.add(int(self._tags[tag]))
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