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
from pympler import asizeof
import pandas as pd
from utility import mkdir_p, dump


class TwitterClient(object):

    """
        Class that handles uploading data into Mongo, Object has the following properties:

        Attributes:
            _server: Denotes if the class is being used on the server or not
            _year: Year in which the data was fetched
            _month: Month in which data was fetched
            _proj: The specific name of the project whose files are being uploaded (mainly for path modification)
            _conf: ConfigParser object for some control information
            _rules: Dictionary that provides Rule to Rule Index mapping
            _ruleLines: Means to get a reverse mapping of the dictionary
            _max_rule: Current largest Rule Index
            _tags: Dictionary that provides Tag to Tag Index mapping
            _max_tag: Current largest Tag Index
            _rules_tags: Dictionary that provides Rule to Tag mapping
            _db: MongoDB Database in which to write into/read from
            _fields: List of fields that we keep from raw JSON and their corresponding mapping to new field names
            _mongoConf: Mapping of fields from generated keys to optimized keys for Mongo Collections
            _src: Path to where the input Source files are to be read from
            _dest: Path to the Destination where the output files are to be written

    """


    def __init__(self, year, month, server, proj):
        self._server = server
        self._year = str(year)
        self._month = str(month).zfill(2)
        self._proj = proj

        self._conf = ConfigParser.ConfigParser()
        self._conf.read('config\\config.cfg')
        
        rules_file = open(self._conf.get('twitter', 'rules'))
        self._rules = json.loads(rules_file.read())
        self._invertedRules = {v: k for k, v in self._rules.items()}
        rules_file.close()

        self._max_rule = max(v for k, v in self._rules.iteritems())

        tags_file = open(self._conf.get('twitter', 'tags'))
        self._tags = json.loads(tags_file.read())
        self._invertedTags = {v: k for k, v in self._tags.items()}
        tags_file.close()

        self._max_tag = max(v for k, v in self._tags.iteritems())

        rules_tags_file = open(self._conf.get('twitter', 'rules_tags'))
        self._rules_tags = json.loads(rules_tags_file.read())
        rules_tags_file.close()

        with open(self._conf.get('twitter', 'fields')) as fieldsFile:
            self._fields = json.loads(fieldsFile.read())

        with open(self._conf.get('twitter', 'fields_mongo')) as fieldsToMongoFile:
            self._mongoConf = json.loads(fieldsToMongoFile.read())

        with open(self._conf.get('twitter', 'mongo_fields')) as mongoToFieldsFile:
            self._mongo_fields = json.loads(mongoToFieldsFile.read())
        
        host = self._conf.get('mongo', 'host')
        port = int(self._conf.get('mongo', 'port'))
        username = self._conf.get('mongo', 'username')
        password = self._conf.get('mongo', 'password')
        authDB = self._conf.get('mongo', 'authDB')
        mongoClient = MongoClient(host, port)
        mongoClient.twitter.authenticate(username, password, source=authDB)
        self._db = mongoClient['twitter']

        self._updatePaths()

        logs = self._conf.get("conf", "prod_log_path")
        logging.basicConfig(level=logging.INFO,
                            filename=logs.format('prodUpload' + self._year + self._month +'.log'),
                            format='%(asctime)s : %(levelname)s - %(message)s')


    @property
    def server(self):
        return self._server

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, yr):
        self._year = yr
        self._updatePaths

    @property
    def month(self):
        return self._month

    @month.setter
    def month(self, mon):
        self._month = mon
        self._updatePaths
    
    @property
    def proj(self):
        return self._proj

    @proj.setter
    def project(self, project):
        self._proj = project
        self._updatePaths
    
    @property
    def conf(self):
        return self._conf

    @property
    def rules(self):
        return self._rules

    @property
    def invertedRules(self):
        return self._invertedRules

    @property
    def max_rule(self):
        return self._max_rule

    @property
    def tags(self):
        return self._tags

    @property
    def invertedTags(self):
        return self._invertedTags
    
    @property
    def max_tag(self):
        return self._max_tag
    
    @property
    def rules_tags(self):
        return self._rules_tags

    @property
    def db(self):
        return self._db

    @property
    def fields(self):
        return self._fields
    
    @property
    def mongoConf(self):
        return self._mongoConf

    @property
    def mongo_fields(self):
        return self._mongo_fields
    
    @property
    def src(self):
        return self._src

    @property
    def dest(self):
        return self._dest
        
    
    def _updatePaths(self):
        if self._proj == "":
            if self._server:
                self._src = self._conf.get('twitter', 'prod_src_path').format(self._year + self.month)
                self._dest = self._conf.get('twitter', 'prod_dest_path').format(self._year + self.month, 'CSVRULES')
            else:
                self._src = self._conf.get('twitter', 'src_path').format(self._year + self.month)
                self._dest = self._conf.get('twitter', 'dest_path').format(self._year + self.month, 'CSVRULES')
        else:
            if self._server:
                self._src = self._conf.get('twitter', 'prod_spl_src_path').format(self._year + self.month, self._proj)
                self._dest = self._conf.get('twitter', 'prod_spl_dest_path').format(self._year + self.month, self._proj, 'CSVRULES')
            else:
                self._src = self._conf.get('twitter', 'spl_src_path').format(self._year + self.month, self._proj)
                self._dest = self._conf.get('twitter', 'spl_dest_path').format(self._year + self.month, self._proj, 'CSVRULES')


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
            
            # If the tag is not already in our set of tags
            if line_json['tag'].lower() not in self._tags:
                # Increment the current maximum Tag Index
                self._max_tag += 1
                # Set the new maximum Tag Index for the new Tag
                self._tags[line_json['tag'].lower()] = self._max_tag
        
        master_rules_file.close()

        self._invertedRules = {v: k for k, v in self._rules.items()}
        self._invertedTags = {v: k for k, v in self._tags.items()}
        # Once the file is fully iterated through, dump the _rules and _rules_tags and _tags dict to their corresponding files
        with open(self._conf.get('twitter', 'rules'), 'w') as rule_file:
            rule_file.write(json.dumps(self._rules))

        with open(self._conf.get('twitter', 'rules_tags'), 'w') as rule_tag_file:
            rule_tag_file.write(json.dumps(self._rules_tags))

        with open(self._conf.get('twitter', 'tags'), 'w') as tag_file:
            tag_file.write(json.dumps(self._tags))


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
                self.dictFromTwitterJSON(self.src + j)


    # Generates an array of dicts from json files
    def dictFromTwitterJSON(self, jsonfilename, errorfile=None):
        jsonfile = open(jsonfilename, 'r')
        # List to hold a dictionary for each processed tweet
        tweetList = []
        
        # List to track all variables seen across all tweets in the file
        mykeys = []

        for line in jsonfile:
            myline = string.strip(line)
            if myline != "":
                # For each tweet in the file, decode the weird characters without complaining
                myline = myline.decode("utf-8", "ignore")
                
                # Remove new lines from within the tweet
                myline = myline.replace('\\n', ' ')
                # Remove carriage returns from within the tweet
                myline = myline.replace('\\r', ' ')
                # Remove problematic \s
                myline = myline.replace('\\\\', ' ')
                myline = myline.replace('\\ ', ' ')

                # Create a dictionary using the JSON processor
                try:
                    tweet = json.loads(myline)
                except ValueError as e:
                    if errorfile != None:
                        write(jsonfilename+"\n"+myline+"\n"+e+"\n\n")
                    else:
                        print e
                else:
                    # Find the summary count
                    if "Replay Request Completed" in myline:
                        print tweet['info']['activity_count']
                    else:
                        # Create an empty dictionary
                        tweetObj = {}
                        # Send the JSON dictionary, the empty dictionary, and the list of all keys
                        self.extract(tweet, tweetObj, mykeys)
                        mongoObj = self.preprocessFHTweet(tweet)
                        # Add the output dictionary to the list
                        self.populateMongo(mongoObj[0], mongoObj[1])
                        
        # Print the number of tweets processed
        jsonfile.close()
        print "Finished populating json file ", jsonfilename


    # Method to map json key name to user defined key name
    def replaceKey(self, key):
        if key in self.fields:
            return self.fields[key]
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


    def preprocessFHTweet(self, inputTweet):
        date = inputTweet['postedTime'].split('T')[0].split('-')
        # Generate collection name from the date in the Tweet
        collName = ''.join(date[0:2]) + '_'
        day = int(date[2])
        if day < 6:
            collName += '1'
        elif day > 5 and  day < 11:
            collName += '2'
        elif day > 10 and day < 16:
            collName += '3'
        elif day > 15 and day < 21:
            collName += '4'
        elif day > 20 and day < 26:
            collName += '5'
        elif day > 25:
            collName += '6'

        # Packing the entitieshtagstext field into an array
        if 'entitieshtagstext' not in inputTweet:
            inputTweet['entitieshtagstext'] = []

        # Renaming id field
        inputTweet['_id'] = inputTweet['Idpost'].split(':')[2]
        # Remove the former id key
        inputTweet.pop('Idpost', None)
        # packing the matchingrulesvalue field into an array
        if isinstance(inputTweet['matchingrulesvalue'], unicode) or isinstance(inputTweet['matchingrulesvalue'], str):
            inputTweet['matchingrulesvalue'] = inputTweet['matchingrulesvalue'].split(';')
        if isinstance(inputTweet['matchingrulestag'], unicode) or isinstance(inputTweet['matchingrulestag'], str):
            inputTweet['matchingrulestag'] = [i.lower() for i in inputTweet['matchingrulestag'].split(';')]
        else:
            inputTweet['matchingrulestag'] = [i.lower() for i in inputTweet['matchingrulestag']]

        # Changing postedTime into ISO format for processing using JavaScript in Mongo
        dateFrag = inputTweet['postedTime'].split('T')
        dateFrag[1] = dateFrag[1].split('.')[0]
        dateStr = ''.join(' ' + d for d in dateFrag).strip()
        timeStruct = time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
        dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
        inputTweet['postedTime'] = dateObj

        newRecord = {}
        # Iterate through each of the keys in the original Dict
        for (key, val) in inputTweet.iteritems():
            # Get the new translated key from the mongoConf dict
            newKey = self.mongoConf[key]
            # create a new with translated field names to upload onto the corresponding Mongo Collection
            newRecord[newKey] = val
        return(collName, newRecord)


    # Method to input tweet into Mongo Collection
    def populateMongo(self, collName, inputTweet):
        # Obtaining a reference to the MongoDB Collection
        collection = self._db[collName]

        collection.ensure_index([("mrv", ASCENDING)])
        collection.ensure_index([("mrt", ASCENDING)])
        
        # Mapping the rules into integers from the rules
        ruleIndex = []
        for j in inputTweet['mrv']:
            try:
                ruleIndex.append(int(self._rules[j.strip()]))
            except KeyError:
                logging.warning("Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip())
                logging.debug(str(inputTweet))
                print "Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip()

        tagIndex = set()
        for j in inputTweet['mrt']:
            if j.strip() == 'lcc':
                tag = 'cigar/cigarillo'
            else:
                tag = j.lower().strip()
            try:
                tagIndex.add(int(self._tags[tag]))
            except KeyError:
                logging.warning("Invalid tag fetched with _id=" + inputTweet['_id'] + " with tag=" + j.strip())
                logging.debug(str(inputTweet))
                print "Invalid tag fetched with _id=" + inputTweet['_id'] + " with tag=" + j.strip()

        # Remove the former mrv key
        inputTweet['mrv'] = ruleIndex
        inputTweet['mrt'] = list(tagIndex)

        # Attempt to insert record into the collection. If it fails, do an update
        try:
            collection.insert(inputTweet)
        except errors.DuplicateKeyError:
            oldRecord = collection.find({'_id': inputTweet['_id']})
            for i in oldRecord:
                mrv = set(inputTweet['mrv'] + i['mrv'])
                if i['mrt'] is not None:
                    mrt = set(inputTweet['mrt'] + i['mrt'])
                else:
                    mrt = set(inputTweet['mrt'])
            inputTweet['mrv'] = list(mrv)
            inputTweet['mrt'] = list(mrt)
            collection.save(inputTweet)


    def queryDB(self, filterRule):
        coll_names = set()
        # Generate Collection names, i.e. MonYY_x, where x is in range(1, 7)
        collName = self._year + self._month
        mkdir_p(self.dest)
        # Initialize a list to hold all the data queried from the Database
        dataSet = []
        for j in range(1, 7):
            coll_names.add(collName + '_' + str(j))
        # Maintain counter to track the file number of the corresponding rule file
        counter = 1
        # For each of the collections
        for i in coll_names:
            coll = self._db[i]
            # Query to find all records of a particular rule
            data = coll.find({'mrv': {'$in': [int(filterRule)]}})
            # For each record returned by the query
            for k in data:
                print k['_id']
                modifiedObj = {}
                # Translate field names
                for (key, val) in k.iteritems():
                    modifiedObj[self.mongo_fields[key]] = val

                # Add extra fields for Posted Time
                if 'postedTime' in modifiedObj:
                    date = modifiedObj['postedTime']
                    modifiedObj['Year'] = date.strftime("%Y")
                    modifiedObj['Month'] = date.strftime("%b")
                    modifiedObj['Day'] = date.strftime("%d")
                    modifiedObj['Time'] = date.strftime("%H:%M:%S")
                # If actorutcOffset is None, then initialize field with a '.'
                if modifiedObj['actorutcOffset'] is None:
                    modifiedObj['actorutcOffset'] = '.'
                # Unroll all the arrays
                index = 1
                if 'entitieshtagstext' in modifiedObj:
                    for i in modifiedObj['entitieshtagstext']:
                        modifiedObj['entitieshtagstext' + str(index).zfill(2)] = i
                        index += 1
                    del(modifiedObj['entitieshtagstext'])
                    index = 1
                if 'entitiesusrmentions' in modifiedObj:
                    for i in modifiedObj['entitiesusrmentions']:
                        modifiedObj['entitiesusrmentionsidstr' + str(index).zfill(2)] = i['is']
                        modifiedObj['entitiesusrmentionsname' + str(index).zfill(2)] = i['n']
                        modifiedObj['entitiesusrmentionssname' + str(index).zfill(2)] = i['sn']
                        index += 1
                    del(modifiedObj['entitiesusrmentions'])
                    index = 1
                # Translated and unroll matchingrulestag array
                if 'matchingrulestag' in modifiedObj:
                    translatedTags = []
                    for i in modifiedObj['matchingrulestag']:
                        tag = self._invertedTags[int(i)]
                        modifiedObj['matchingrulestag' + str(index).zfill(2)] = tag
                        index += 1
                    del(modifiedObj['matchingrulestag'])
                    index = 1
                # Unroll matchingrulesvalue array and create a new field to hold all the translated matchingrulesvalues
                if 'matchingrulesvalue' in modifiedObj:
                    translatedRules = []
                    for i in modifiedObj['matchingrulesvalue']:
                        modifiedObj['matchingrulesvalue' + str(index).zfill(2)] = i
                        index += 1
                        rule = self._invertedRules[int(i)]
                        translatedRules.append(rule)
                    modifiedObj['matchingrulesvalues'] = ';'.join(translatedRules)
                    del(modifiedObj['matchingrulesvalue'])
                    index = 1
                # Add the record to the dataset list
                dataSet.append(modifiedObj)
                # If the size of the dataSet list exceeds a certain threshold, write to a file
                if asizeof.asizeof(dataSet) > 104857600:
                    print "\nWriting to File...\n"
                    # Create a new DataFrame and write to a csv file
                    df = pd.DataFrame(dataSet)
                    with open(self._dest + month + str(filterRule) + '_' + str(counter) + '.csv', 'wb') as csvfile:
                        df.to_csv(csvfile, sep=',', index=False)
                    counter += 1

        print "\nWriting to File...\n"
        # Create a new DataFrame and write to a csv file
        df = pd.DataFrame(dataSet)
        with open(self._dest + self.year + self.month + '_' + str(filterRule) + '_' + str(counter) + '.csv', 'wb') as csvfile:
            df.to_csv(csvfile, sep=',', index=False)
        counter += 1