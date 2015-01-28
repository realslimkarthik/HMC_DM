import json
import sys
import os
import re
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time

def queryDB(window, filterRules):
    mongoConf = ConfigParser.ConfigParser()
    mongoConf.read("config.cfg")
    host = mongoConf.get("mongo", "host")
    port = int(mongoConf.get("mongo", "port"))
    client = MongoClient(host, port)
    db = client.twitter
    longestHtag = 0
    longestRule = 0
    coll_names = set()
    outputSet = []
    for i in window:
        for j in range(1, 7):
            coll_names.add(i + '_' + str(j))
    for i in coll_names:
        for j in filterRules:
            coll = db[i]
            data = coll.find({'mrv': {'$in': [str(j)]}})
            for k in data:
                if 'eumh' in k:
                    if len(k['eumh']) > longestHtag:
                        longestHtag = len(k['eumh'])
                if len(k['mrv']) > longestRule:
                    longestRule = len(k['mrv'])
                outputSet.append(k)
                print k['_id']

    return (outputSet, longestRule, longestHtag)

# ========================================================================================

def printCSV(csvfile, resultList):
    delim = ","
    print "Number of tweets processed: ", len(resultList[0])
    conf = ConfigParser.ConfigParser()
    conf.read("mongoToFields.cfg")

    f = conf.get("fields", "_id")
    csvfile.write(f + delim)
    for (key, val) in conf.items('fields'):
        if val == "Idpost":
            continue
        if val == "postedTime":
            csvfile.write("postedTime" + delim + "Year" + delim + "Month" + delim + "Day" + delim + "Time" + delim)
            continue
        if val == "matchingrulesvalue" and resultList[1] > 1:
            for i in range(1, resultList[1] + 1):
                csvfile.write("matchingrulesvalue" + str(i) + delim)
            continue
        if val == "entitieshtagstext" and resultList[2] > 1:
            for i in range(1, resultList[2] + 1):
                csvfile.write("entitieshtagstext" + str(i) + delim)
            continue
        csvfile.write(val + delim)

    #For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList[0]:
        for (key, val) in conf.items('fields'):
            if val not in result:
                result[val] = ''
        csvfile.write("\n")
        myid = result['_id']
        csvfile.write(myid + delim)
        for (key, val) in conf.items('fields'):
            if key in result:
                if key == "_id":
                    continue
                if key == "pT":
                    csvfile.write(result[key] + delim)
                    pTime = result[key].split('T')
                    date = pTime[0].split('-')
                    csvfile.write(date[1]+delim)
                    csvfile.write(date[2]+delim)
                    csvfile.write(date[0]+delim)
                    time = pTime[1].split('.')[0]
                    csvfile.write(time+delim)
                    continue
                if key  == "auO":
                    if result['auO'] is None:
                        csvfile.write('.' + delim)
                        continue
                if key == "mrv":
                    if resultList[1] > 0:
                        for j in result['mrv']:
                            csvfile.write(j + delim)
                        continue
                if key == "eumh":
                    if resultList[2] > 0:
                        for j in result['eumh']:
                            csvfile.write(j + delim)
                        continue
                entry = result[key]
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

if __name__ == "__main__":
    start_time = sys.argv[1]
    end_time = sys.argv[2]
    if start_time == end_time:
        window = [start_time]
        csvfile = open(start_time + '_' + '.csv', 'w')
    else:
        window = [start_time, end_time]
        csvfile = open(start_time + '_' + end_time + '.csv', 'w')
    rules = sys.argv[3:]
    outputSet = queryDB(window, rules)
    print outputSet[1]
    printCSV(csvfile, outputSet)
    csvfile.close()