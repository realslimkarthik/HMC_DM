import string
import json
import sys
import os
import random
import re
import Tkinter as tk
import ConfigParser
from pymongo import MongoClient, errors
from datetime import datetime
import time
import logging

# ========================================================================================
# Populates the CSV. Gets CSV's file handle from caller
def CSVfromTwitterJSON(jsonfilename, collName, DorP, errorfile=None, overwrite=False):
    if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
        jsonfile = open(jsonfilename, 'r')
        #Will track all variables seen across all tweets in the file
        #Will contain a dictionary for each processed tweet
        tweetList = []
        
        # Rule index mapping done via rules.json files
        r = open(conf_path.format("rules.json"))
        # Dict that stores the rules
        ruleConf = json.loads(r.read())
        # number of lines in the file to track the index of the additional rules
        num_lines = sum(1 for i in r)
        r.close()
        
        # Host and Port values for MongoDB connection
        host = conf.get(DorP, "host")
        port = int(conf.get(DorP, "port"))
        mongoConf = ConfigParser.ConfigParser()
        mongoConf.read(conf_path.format("fieldsToMongo.cfg"))
        configData = (host, port, mongoConf)
        mykeys = []

        for line in jsonfile:
            myline = string.strip(line)
            if myline != "":
                #For each tweet in the file, decode the weird characters without complaining
                myline = myline.decode("utf-8", "ignore")
                #print myline
                #Remove new lines from within the tweet
                mylines = myline.split("\\n")
                if len(mylines) > 1:
                    myline = " ".join(mylines)
                #Remove carriage returns from within the tweet
                mylines = myline.split("\\r")
                if len(mylines) > 1:
                    myline = " ".join(mylines)
                #Remove problematic \s
                mylines = myline.split("\\\\")
                if len(mylines) > 1:
                    myline = " ".join(mylines)
                mylines = myline.split("\\ ")
                if len(mylines) > 1:
                    myline = " ".join(mylines)
                #Create a dictionary using the JSON processor
                try:
                    tweet = json.loads(myline)
                except ValueError as e:
                    if errorfile != None:
                        write(jsonfilename+"\n"+myline+"\n"+e+"\n\n")
                    else:
                        print myline
                        print e
                else:
                    #Find the summary count
                    if "Replay Request Completed" in myline:
                        print tweet['info']['activity_count']
                    else:
                        #Create an empty dictionary
                        tweetObj = {}
                        #Send the JSON dictionary, the empty dictionary, and the list of all keys
                        extract(tweet, tweetObj, mykeys)
                        #Add the output dictionary to the list
                        populateMongo(tweetObj, collName, DorP, ruleConf, configData)
                        try:
                            print tweetObj['geocoordinates']
                        except KeyError:
                            # print "NA"
                            continue
        #Print the number of tweets processed
        jsonfile.close()
        print "Finished populating collection ", collName

# ========================================================================================
def removeKey(key):
    conf = ConfigParser.ConfigParser()
    # conf.read(conf_path.format("fields.cfg"))
    conf.read("config\\fields.cfg")
    if conf.has_option("fields", key):
        return conf.get("fields", key)
    else:
        return ""

# ========================================================================================
#Recursive function to process the input dictionary
def extract(DictIn, Dictout, allkeys, nestedKey=""):

    # Explicitly adding keys to Dictout
    if nestedKey == "object":
        try:
            Dictout["objlocdname"] = DictIn["location"]["displayName"]
        except KeyError:
            pass
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
    elif nestedKey == "generator":
        Dictout["generatordname"] = DictIn["displayName"]
    elif nestedKey == "geo":
        Dictout["geocoordinates"] = DictIn['coordinates']
        Dictout["geotype"] = DictIn["type"]
    elif nestedKey == "location":
        try:
            Dictout["locdname"] = DictIn["displayName"]
            Dictout["locname"] = DictIn["name"]
            Dictout["loccountrycode"] = DictIn["country_code"]
            if DictIn["geo"] is not None:
                Dictout["locgeocoordinates"] = DictIn["geo"]["coordinates"]
        except KeyError, TypeError:
            pass
    
    #If DictIn is a dictionary
    elif isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extract(value, Dictout, allkeys, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extract(value, Dictout, allkeys, nestedKey=mykey)
            else: #Value is just a string
                newKey = removeKey(mykey)
                if newKey == "":
                    return
                if isinstance(value, unicode) or isinstance(value, str):
                    value = value.strip()
                if value != "":
                    #If this is a new variable, add it to the list
                    if not newKey in allkeys:
                        allkeys.append(newKey)
                    #Add it to the output dictionary
                    if not newKey in Dictout:
                        Dictout[newKey] = value
                    else:
                        Dictout[newKey] = unicode(Dictout[newKey])+"; "+unicode(value)
                else:
                    if not newKey in allkeys:
                        allkeys.append(newKey)
                    if not newKey in Dictout:
                        Dictout[newKey] = ""

    #If DictIn is a list, call extract on each member of the list
    elif isinstance(DictIn, list):
        for value in DictIn:
            extract(value,Dictout,allkeys,nestedKey=nestedKey)
    #If DictIn is a string, check if it is a new variable and then add to dictionary
    else:
        if isinstance(DictIn, unicode) or isinstance(DictIn, str):
            newKey = removeKey(DictIn)
            if newKey == "":
                return
            if isinstance(DictIn, unicode) or isinstance(DictIn, str):
                DictIn = DictIn.strip()
            if DictIn != "":
                if not newKey in allkeys:
                    allkeys.append(newKey)
                if not newKey in Dictout:
                    Dictout[newKey] = DictIn
                else:
                    Dictout[newKey] = unicode(Dictout[newKey])+"; "+unicode(DictIn)
            else:
                if not newKey in allkeys:
                    allkeys.append(newKey)
                if not newKey in Dictout:
                    Dictout[newKey] = ""

# ========================================================================================
# def addRule(rule):
#     r = open(conf_path.format("rules.json", "a"))
#     num_lines += 1
#     count = 1
#     for i in r:
#         if count == num_lines:
#             i.replace('}', "\"" + rule + "\"" + ':' + "\"" + str(num_lines) + "\"" + '\n')
#         count += 1
#     r.write('}')
#     r.close()
#     num_lines += 1
#     ruleConf = json.loads(r.read())

# ========================================================================================
def populateMongo(inputTweet, collName, DorP, ruleConf, configData):
    host = configData[0]
    port = configData[1]
    # Creating a new MongoDB client
    client = MongoClient(host, port)
    db = client.twitter
    collection = db[collName]
    mongoConf = configData[2]
    #packing the entitieshtagstext field into an array
    if 'entitieshtagstext' not in inputTweet:
        inputTweet['entitieshtagstext'] = ''

    # Renaming id field
    inputTweet['_id'] = inputTweet['Idpost'].split(':')[2]
    logging.info('Started posting collection with id: ' + str(inputTweet['_id']) + ' into collection ' + collName)
    # Remove the former id key
    inputTweet.pop('Idpost', None)
    # packing the matchingrulesvalue field into an array
    inputTweet['matchingrulesvalue'] = inputTweet['matchingrulesvalue'].split(';')

    # Changing postedTime into ISO format for processing using JavaScript in Mongo
    dateFrag = inputTweet['postedTime'].split('T')
    dateFrag[1] = dateFrag[1].split('.')[0]
    dateStr = ''.join(' ' + d for d in dateFrag).strip()
    timeStruct = time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
    dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
    inputTweet['postedTime'] = dateObj
    
    # Mapping the rules into integers from the ruleConf
    inputTweet['ruleIndex'] = []
    for j in inputTweet['matchingrulesvalue']:
        try:
            # inputTweet['ruleIndex'].append(ruleConf[j.strip()])
            inputTweet['ruleIndex'].append(int(ruleConf[j.strip()]))
        except KeyError:
            # If the rule isn't found in the ruleConf, then add the rule to the rules.json file
            # addRule(j.strip())
            logging.warning("Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip())
            logging.debug(str(inputTweet))
            print "Invalid rule fetched via GNIP with _id=" + inputTweet['_id'] + " with rule=" + j.strip()

            # inputTweet['ruleIndex'].append(int(ruleConf[j.strip()]))
            # Add the new rule that was added in the ruleConf
    # Remove the former matchingrulesvalue key
    inputTweet.pop('matchingrulesvalue', None)
    print inputTweet['ruleIndex']
    # Initialize a new Dict to hold the record that needs to be uploaded into the corresponding Mongo Collection
    mongoRecord = {}
    
    # Iterate through each of the keys in the original Dict
    for (key, val) in inputTweet.iteritems():
        # Get the new translated key from the mongoConf dict
        newKey = mongoConf.get('fields', key)
        # create a new with translated field names to upload onto the corresponding Mongo Collection
        mongoRecord[newKey] = val

    # Attempt to insert record into the collection. If it fails, do an update
    try:
        collection.insert(mongoRecord)
    except errors.DuplicateKeyError:
        collection.save(mongoRecord)
        logging.debug("Updated tweet _id=" + str(inputTweet['_id']))
        print "Updated tweet _id=" + str(inputTweet['_id'])

# ========================================================================================
# Command to run the script
# python DM_TWITTER_K.py prod <Capitalized 3 letter month name> <Four digit year>
# Eg - python DM_TWITTER_K.py prod Aug 2014

monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

if __name__ == "__main__":
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    
    if choice == "dev":
        logs = conf.get("conf", "dev_log_path")
        op = sys.argv[2]
        if op == "transform":
            logging.basicConfig(filename=logs.format('prodUpload' + "dev" +'.log'), level=logging.DEBUG)
            CSVfromTwitterJSON(inputFile, "August_test", "mongo")
    elif choice =="prod":
        current_month = sys.argv[2]
        current_year = sys.argv[3]
        # Format the input month and year to form a a part of the Mongo Collection name
        collName = current_month[0:3] + current_year[2:]
        print collName
        # Get the path for the logs output
        logs = conf.get("conf", "prod_log_path")
        logging.basicConfig(filename=logs.format('prodUpload' + collName +'.log'), level=logging.DEBUG)
        # Get the path for the source Raw_data json files
        src_path = conf.get("twitter", "prod_src_path").format(current_year + monthToNames[current_month])
        # Get the list of files in the source directory
        fileList = os.listdir(src_path)
        # Iterate over every file in the source directory
        for j in fileList:
            # If it's a by-day file in the source directory it will have 3 parts around the '-'s
            if len(j.split('_')) == 3:
                # Extract the date of the corresponding file from it's name
                fileDate = int(j.split('_')[-1].split('.')[0])
                logging.info("Started uploading " + j)
                # Upload to the corresponding Mongo Collection based on the date extracted from the file
                if fileDate < 6:
                    CSVfromTwitterJSON(src_path + j, collName + "_1", "mongo")
                elif fileDate > 5 and  fileDate < 11:
                    CSVfromTwitterJSON(src_path + j, collName + "_2", "mongo")
                elif fileDate > 10 and fileDate < 16:
                    CSVfromTwitterJSON(src_path + j, collName + "_3", "mongo")
                elif fileDate > 15 and fileDate < 21:
                    CSVfromTwitterJSON(src_path + j, collName + "_4", "mongo")
                elif fileDate > 20 and fileDate < 26:
                    CSVfromTwitterJSON(src_path + j, collName + "_5", "mongo")
                elif fileDate > 25 and fileDate < 32:
                    CSVfromTwitterJSON(src_path + j, collName + "_6", "mongo")
