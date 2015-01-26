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

    print len(mykeys)
    for i in inputJson:
        if 'entitieshtagstext' not in i:
            i['entitieshtagstext'] = []
        else:
            #packing the entitieshtagstext field into an array
            i['entitieshtagstext'] = i['entitieshtagstext'].split(';')

        # for (key, val) in fieldConf.items('fields'):
        #     if val not in i:
        #         i[val] = ''

        print len(i)

        # Renaming id field
        i['_id'] = "tw" + i['Idpost'].split(':')[2]
        logging.info('Started posting collection with id: ' + i['_id'])
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
            # i['ruleIndex'].append(ruleConf.get("rules", j))
            # print ruleConf.get("rules", j)
        i.pop('matchingrulesvalue', None)   
        print i['ruleIndex']
        mongoRecord = {}
        for (key, val) in i.iteritems():
            newKey = mongoConf.get('fields', key)
            mongoRecord[newKey] = val

        collection.insert(mongoRecord)
    # outputFile.write(json.dumps(inputJson, ensure_ascii=False).encode('utf-8'))


# ========================================================================================

def printCSV(csvfile,resultList,mykeys):
    delim = ","
    print "Number of tweets processed: ", len(resultList)
    conf = ConfigParser.ConfigParser()
    conf.read("fields.cfg")

    f = conf.get("fields", "id")
    csvfile.write(f + delim)
    print mykeys
    for item in mykeys:
        if item == "Idpost":
            continue
        if item == "postedTime":
            csvfile.write("postedTime" + delim + "Year" + delim + "Month" + delim + "Day" + delim + "Time" + delim)
            continue
        csvfile.write(item + delim)

    #For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList:
        csvfile.write("\n")
        myid = result['Idpost']
        myids = myid.split(":")
        csvfile.write("\"tw"+myids[2]+"\""+delim)
        counter = 0
        for item in mykeys:
            if item in result:
                if item == "Idpost":
                    continue
                if item == "postedTime":
                    csvfile.write(result[item] + delim)
                    pTime = result[item].split('T')
                    date = pTime[0].split('-')
                    csvfile.write(date[1]+delim)
                    csvfile.write(date[2]+delim)
                    csvfile.write(date[0]+delim)
                    time = pTime[1].split('.')[0]
                    csvfile.write(time+delim)
                    continue
                if item  == "actorutcOffset":
                    if result['actorutcOffset'] is None:
                        csvfile.write('.' + delim)
                        continue
                entry = result[item]
                if type(entry) == unicode or type(entry) == str:
                    #entry = unicode(entry, "utf-8", errors="ignore")
                    entry = entry.strip()
                    entrys = entry.split(",")
                    if len(entrys) > 1:
                        entry = "".join(entrys)
                    entrys = entry.split("\"")
                    if len(entrys) > 1:
                        entry = "-".join(entrys)
                else:
                    entry = unicode(entry)
                #Override to avoid errors for weird characters
                temp = entry.splitlines()
                entry = "".join(temp)
                csvfile.write(entry.encode('ascii','ignore')+delim)
            else:
                csvfile.write(delim)
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
            current_month = "August-2014-Master"
            src_path = conf.get("twitter", "prod_src_path").format(current_month)
            fileList = os.listdir(src_path)
            
            for j in fileList:
                if len(j.split('-')) == 3:
                    fileName = j.split('-')[-1].split('.')[0]
                    logging.info("Started uploading " + j)
                    if int(fileName) < 6:
                        CSVfromTwitterJSON(src_path + j, "August_1", "mongo")
                    elif int(fileName) < 11:
                        CSVfromTwitterJSON(src_path + j, "August_2", "mongo")
                    elif int(fileName) < 16:
                        CSVfromTwitterJSON(src_path + j, "August_3", "mongo")
                    elif int(fileName) < 21:
                        CSVfromTwitterJSON(src_path + j, "August_4", "mongo")
                    elif int(fileName) < 26:
                        CSVfromTwitterJSON(src_path + j, "August_5", "mongo")
                    elif int(fileName) < 32:
                        CSVfromTwitterJSON(src_path + j, "August_6", "mongo")


            # TO DO: Implement Time Frame based uploading

            # timeFrame = range(6)[1:]
            # for i in timeFrame:

            #     for j in fileList:
            #         if len(j.split('-') == 3):
            #             if int(j.split('-')[-1].split('.')[0]) == i:
            #                 CSVfromTwitterJSON(src_path + j, csvfile, 'mongo')
            #                 print j
