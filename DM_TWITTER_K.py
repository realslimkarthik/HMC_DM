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
        mykeys = []
        #Will contain a dictionary for each processed tweet
        tweetList = []
        conf = ConfigParser.ConfigParser()
        conf.read("fields.cfg")
        
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
                        a = {}
                        #Send the JSON dictionary, the empty dictionary, and the list of all keys
                        extract(tweet, a, mykeys)
                        #Add the output dictionary to the list
                        tweetList.append(a)
        #Print the number of tweets processed
        jsonfile.close()
        populateMongo(tweetList, mykeys, collName, DorP)
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
    if isinstance(DictIn, dict):
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
def populateMongo(inputJson, mykeys, collName, DorP):
    host = conf.get(DorP, "host")
    port = int(conf.get(DorP, "port"))
    fieldConf = ConfigParser.ConfigParser()
    fieldConf.read("fields.cfg")
    client = MongoClient(host, port)
    db = client.twitter
    collection = db[collName]
    # ruleConf = ConfigParser.ConfigParser()
    # ruleConf.read("rules.cfg")
    r = open("rules.json")
    ruleConf = json.loads(r.read())
    mongoConf = ConfigParser.ConfigParser()
    mongoConf.read("fieldsToMongo.cfg")

    for i in inputJson:
        if 'entitieshtagstext' not in i:
            i['entitieshtagstext'] = []
        else:
            #packing the entitieshtagstext field into an array
            i['entitieshtagstext'] = i['entitieshtagstext'].split(';')

        # Renaming id field
        i['_id'] = "tw" + i['Idpost'].split(':')[2]
        logging.info('Started posting collection with id: ' + i['_id'] + ' into collection ' + collName)
        i.pop('Idpost', None)
        # packing the matchingrulesvalue field into an array
        i['matchingrulesvalue'] = i['matchingrulesvalue'].split(';')

        # Changing postedTime into ISO format for processing using JavaScript in Mongo
        dateFrag = i['postedTime'].split('T')
        dateFrag[1] = dateFrag[1].split('.')[0]
        dateStr = ''.join(' ' + d for d in dateFrag).strip()
        timeStruct = time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
        dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
        i['postedTime'] = dateObj
        
        i['ruleIndex'] = []
        for j in i['matchingrulesvalue']:
            i['ruleIndex'].append(ruleConf[j.strip()])
        i.pop('matchingrulesvalue', None)   
        print i['ruleIndex']
        mongoRecord = {}
        for (key, val) in i.iteritems():
            newKey = mongoConf.get('fields', key)
            mongoRecord[newKey] = val

        try:
            collection.insert(mongoRecord)
        except KeyError:
            logging.debug("Duplicate tweet _id=" + i['_id'])
    # outputFile.write(json.dumps(inputJson, ensure_ascii=False).encode('utf-8'))

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
    elif choice == "interactive":
        print "Month is August for now"
        # mon = input("Enter the month you want:")[0:3].lower()
        day = input("Enter the timeframe that you want: (1, 7, 15, 30)")
        mon = "Aug"[0:3].lower()
        src_path = conf.get("twitter", "prod_src_path").format(current_month)
        dest_path = conf.get("twitter", "prod_dest_path").format(current_month)

        exDays = ['jan', 'mar', 'may', 'jul', 'aug', 'oct', 'dec']
        lastday = daysInMonth(mon)
        dayDict = {'1': {'ranges': range(lastday + 1), 'fileNames': map(lambda x: '_' + str(x), range(lastday + 1))},
                    '7': {'ranges': [8, 16, 24], 'fileNames': ['_1_7', '_8_15', '_16_23', '_24_' + str(lastday)]},
                    '15': {'ranges': [16], 'fileNames': ['_1_15', '_16_' + str(lastday)]},
                    '30': {'ranges': [], 'fileNames': ['']}
                    }
        csvFiles = [dest_path + dayDict[day]['fileNames'][0]]

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
                    elif int(fileName) < 11:
                        CSVfromTwitterJSON(src_path + j, collName + "_2", "mongo")
                    elif int(fileName) < 16:
                        CSVfromTwitterJSON(src_path + j, collName + "_3", "mongo")
                    elif int(fileName) < 21:
                        CSVfromTwitterJSON(src_path + j, collName + "_4", "mongo")
                    elif int(fileName) < 26:
                        CSVfromTwitterJSON(src_path + j, collName + "_5", "mongo")
                    elif int(fileName) < 32:
                        CSVfromTwitterJSON(src_path + j, collName + "_6", "mongo")


            # TO DO: Implement Time Frame based uploading

            # timeFrame = range(6)[1:]
            # for i in timeFrame:

            #     for j in fileList:
            #         if len(j.split('-') == 3):
            #             if int(j.split('-')[-1].split('.')[0]) == i:
            #                 CSVfromTwitterJSON(src_path + j, csvfile, 'mongo')
            #                 print j
