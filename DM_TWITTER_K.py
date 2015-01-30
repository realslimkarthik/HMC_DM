import string
import json
import sys
import os
import random
import re
import Tkinter as tk
import ConfigParser
from pymongo import MongoClient
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
        conf = ConfigParser.ConfigParser()
        conf.read("fields.cfg")
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
                        populateMongo(tweetObj, collName, DorP)
                        # tweetList.append(tweetObj)
        #Print the number of tweets processed
        jsonfile.close()
        # printCSV(csvfile, tweetList, mykeys)
        print "Finished populating collection ", collName

# ========================================================================================
def removeKey(key):
    conf = ConfigParser.ConfigParser()
    conf.read("fields.cfg")
    if conf.has_option("fields", key):
        return conf.get("fields", key)
    else:
        return ""

# ========================================================================================
#Recursive function to process the input dictionary
def extract(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
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
def populateMongo(inputTweet, collName, DorP):
    host = conf.get(DorP, "host")
    port = int(conf.get(DorP, "port"))
    fieldConf = ConfigParser.ConfigParser()
    fieldConf.read("fields.cfg")
    client = MongoClient(host, port)
    db = client.twitter
    collection = db[collName]
    r = open("rules.json")
    ruleConf = json.loads(r.read())
    mongoConf = ConfigParser.ConfigParser()
    mongoConf.read("fieldsToMongo.cfg")
    if 'entitieshtagstext' not in inputTweet:
        inputTweet['entitieshtagstext'] = []
    else:
        #packing the entitieshtagstext field into an array
        inputTweet['entitieshtagstext'] = inputTweet['entitieshtagstext'].split(';')

    # Renaming id field
    inputTweet['_id'] = "tw" + inputTweet['Idpost'].split(':')[2]
    logging.info('Started posting collection with id: ' + inputTweet['_id'] + ' into collection ' + collName)
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
    
    inputTweet['ruleIndex'] = []
    for j in inputTweet['matchingrulesvalue']:
        inputTweet['ruleIndex'].append(ruleConf[j.strip()])
    inputTweet.pop('matchingrulesvalue', None)
    print inputTweet['ruleIndex']
    mongoRecord = {}
    
    for (key, val) in inputTweet.iteritems():
        newKey = mongoConf.get('fields', key)
        mongoRecord[newKey] = val    

    try:
        collection.insert(mongoRecord)
    except KeyError:
        logging.debug("Duplicate tweet _id=" + inputTweet['_id'])
    # outputFile.write(json.dumps(inputTweet, ensure_ascii=False).encode('utf-8'))

# ========================================================================================

def daysInMonth(month):
    return 31 if month in exDays else (28 if month == 'feb' else 30)


blacklist = ["generator", "provider", "verified", "indices", "id$", "sizes", "display_url", "media_url$", "^url$", "url_https$", "inReplyTo", "twitter_filter_level", "^rel$"]
#path = "H:\Data\RawData\GNIP\TwitterHistoricalPowertrack\\"

if __name__ == "__main__":

    inputFile = "dummy_sample.json"
    outputFile = "dummy_sample_dropped.csv"
    # inputFile = "Sample of tw2014-09-02.json"
    # outputFile = "Sample of tw2014-09-02.csv"
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config.cfg")
    
    if choice == "dev":
        op = sys.argv[2]
        if op == "transform":
            outF = open(outputFile, 'w')
            CSVfromTwitterJSON(inputFile, outF, "August_test", "mongo_dev")
            outF.close()
        elif op == "count":
            flattenJSON(inputFile)
    elif choice =="prod":
        logging.basicConfig(filename='prodUpload.log', level=logging.DEBUG)
        op = sys.argv[2]
        if op == "transform":
            current_month = sys.argv[3]
            current_year = sys.argv[4]
            collName = current_month[0:3] + current_year[2:]
            src_path = conf.get("twitter", "prod_src_path").format(current_month + '-' + current_year + '-' + 'Master')
            fileList = os.listdir(src_path)
            current_month = current_month[0:2]
            for j in fileList:
                if len(j.split('-')) == 3:
                    fileName = j.split('-')[-1].split('.')[0]
                    logging.info("Started uploading " + j)
                    if int(fileName) < 6:
                        CSVfromTwitterJSON(src_path + j, collName + "_1", "mongo")
                    # elif int(fileName) < 11:
                    #     CSVfromTwitterJSON(src_path + j, collName + "_2", "mongo")
                    # elif int(fileName) < 16:
                    #     CSVfromTwitterJSON(src_path + j, collName + "_3", "mongo")
                    # elif int(fileName) < 21:
                    #     CSVfromTwitterJSON(src_path + j, collName + "_4", "mongo")
                    # elif int(fileName) < 26:
                    #     CSVfromTwitterJSON(src_path + j, collName + "_5", "mongo")
                    # elif int(fileName) < 32:
                    #     CSVfromTwitterJSON(src_path + j, collName + "_6", "mongo")

            # TO DO: Implement Time Frame based uploading