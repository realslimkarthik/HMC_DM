import string
import json
import sys
import os
import random
import re
import Tkinter as tk
import ConfigParser
from pymongo import MongoClient

# ========================================================================================
#Recursive function to process the input dictionary
# def extract(DictIn, Dictout, allkeys, nestedKey=""):
#     #If DictIn is a dictionary
#     if isinstance(DictIn, dict):
#         #Process each entry
#         for key, value in DictIn.iteritems():
#             #If nested, prepend the previous variables
#             if nestedKey != "":
#                 mykey = nestedKey+"_"+key
#             else:
#                 mykey = key
#             if isinstance(value, dict): # If value itself is dictionary
#                 extract(value, Dictout, allkeys, nestedKey=mykey)
#             elif isinstance(value, list): # If value itself is list
#                 extract(value, Dictout, allkeys, nestedKey=mykey)
#             else: #Value is just a string
#                 if removeKey(mykey) == "":
#                     return
#                 if isinstance(value, unicode) or isinstance(value, str):
#                     value = value.strip()
#                 if value != "":
#                     #If this is a new variable, add it to the list
#                     if not mykey in allkeys:
#                         allkeys.append(mykey)
#                     #Add it to the output dictionary
#                     if not mykey in Dictout:
#                         Dictout[mykey] = value
#                     else:
#                         Dictout[mykey] = unicode(Dictout[mykey])+"; "+unicode(value)
#     #If DictIn is a list, call extract on each member of the list
#     elif isinstance(DictIn, list):
#         for value in DictIn:
#             extract(value,Dictout,allkeys,nestedKey=nestedKey)
#     #If DictIn is a string, check if it is a new variable and then add to dictionary
#     else:
#         if isinstance(DictIn, unicode) or isinstance(DictIn, str):
#             if removeKey(DictIn) == "":
#                 return
#             if isinstance(DictIn, unicode) or isinstance(DictIn, str):
#                 DictIn = DictIn.strip()
#             if DictIn != "":
#                 if not nestedKey in allkeys:
#                     allkeys.append(nestedKey)
#                 if not nestedKey in Dictout:
#                     Dictout[nestedKey] = DictIn
#                 else:
#                     Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)

# ========================================================================================
# Populates the CSV. Gets CSV's file handle from caller
def CSVfromTwitterJSON(jsonfilename, csvfile, errorfile=None, overwrite=False):
    if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
        jsonfile = open(jsonfilename, 'r')
        #Will track all variables seen across all tweets in the file
        mykeys = []
        #Will contain a dictionary for each processed tweet
        tweetList = []
        conf = ConfigParser.ConfigParser()
        conf.read("fields.txt")
        
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
        populateMongo(tweetList, mykeys, csvfile)
        # printCSV(csvfile, tweetList, mykeys)
        csvfile.write('\n')
        print "Finished... ", csvfile

# ========================================================================================
def removeKey(key):
    conf = ConfigParser.ConfigParser()
    conf.read("fields.txt")
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
                    print "hello"
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
                print "hello"
                if not newKey in allkeys:
                    allkeys.append(newKey)
                if not newKey in Dictout:
                    Dictout[newKey] = ""

# ========================================================================================
def populateMongo(inputJson, mykeys, outputFile):
    # host = conf.get("mongo", "host")
    # port = conf.get("mongo", "port")
    host = conf.get("mongo_dev", "host")
    port = int(conf.get("mongo_dev", "port"))
    client = MongoClient(host, port)
    db = client.twitter
    collection = db['smoking']
    ruleConf = ConfigParser.ConfigParser()
    ruleConf.read("rules.cfg")
    print len(mykeys)
    for i in inputJson:
        print len(i)
        # print i
        # collection.insert(json.dumps(i, ensure_ascii=False).encode("utf-8"))
        i['_id'] = "tw" + i['Idpost'].split(':')[2]
        i['matchingrulesvalue'] = i['matchingrulesvalue'].split(';')
        # i['entitieshtagstext'] = i['entitieshtagstext'].split(';')
        i['ruleIndex'] = []
        for j in i['matchingrulesvalue']:
            i['ruleIndex'].append(ruleConf.get("rules", j))
        i.pop('Idpost', None)
        print i['ruleIndex']
        collection.insert(i)
    # print inputJson
    # outputFile.write(json.dumps(inputJson, ensure_ascii=False).encode('utf-8'))



# ========================================================================================

def printCSV(csvfile,resultList,mykeys):
    delim = ","
    print "Number of tweets processed: ", len(resultList)
    conf = ConfigParser.ConfigParser()
    conf.read("fields.txt")

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

    # inputFile = "dummy_sample.json"
    # outputFile = "dummy_sample_dropped.csv"
    inputFile = "Sample of tw2014-09-02.json"
    outputFile = "Sample of tw2014-09-02.csv"
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config.cfg")
    
    if choice == "dev":
        op = sys.argv[2]
        if op == "transform":
            outF = open(outputFile, 'w')
            CSVfromTwitterJSON(inputFile, outF)
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
        op = sys.argv[2]
        if op == "transform":
            current_month = "August-2014-Master"
            src_path = conf.get("twitter", "prod_src_path").format(current_month)
            dest_path = conf.get("twitter", "prod_dest_path").format(current_month)
            fileList = os.listdir(src_path)
            timeFrame = range(6)[1:]
            for i in timeFrame:
                csvfile = open(dest_path + '_' + str(i) + '.csv', 'w')

                for j in fileList:
                    if j == "byminutes":
                        continue
                    if int(j.split('-')[-1].split('.')[0]) == i:
                        CSVfromTwitterJSON(src_path + j, csvfile)
                        print j
                csvfile.close()

            # # TO DO: Write another loop to go over all of the months

            # csvfile1 = open(dest_path + '1-7.csv', 'w')
            # csvfile2 = open(dest_path + '8-15.csv', 'w')
            # csvfile3 = open(dest_path + '16-23.csv', 'w')
            # csvfile4 = open(dest_path + '24-30.csv', 'w')
            # for i in fileList:
            #     if int(i.split('-')[-3]) < 8:
            #         CSVfromTwitterJSON(src_path + i, csvfile1)
            #     # else:
            #     #     CSVfromTwitterJSON(src_path + i, csvfile2)
            # csvfile1.close()
            # csvfile2.close()
            # csvfile3.close()
            # csvfile4.close()


    # TO DO: Write a function that prints only the fields that we want to throw away to show people why we're throwing them away