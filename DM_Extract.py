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
    longestUMen = 0
    longestRTag = 0
    coll_names = set()
    # coll_names.add("Oct14_1")
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
                if 'eum' in k:
                    if len(k['eum']) > longestUMen:
                        longestUMen = len(k['eum'])
                if 'mrt' in k:
                    if len(k['mrt'].split(';')) > longestRTag:
                        longestRTag = len(k['mrt'].split(';'))
                if len(k['mrv']) > longestRule:
                    longestRule = len(k['mrv'])
                outputSet.append(k)
                print k['pt']

    return (outputSet, longestRule, longestHtag, longestUMen, longestRTag)

# ========================================================================================

def printCSV(csvfile, resultList):
    delim = ","
    print "Number of tweets processed: ", len(resultList[0])
    conf = ConfigParser.ConfigParser()
    conf.read("mongoToFields.cfg")
    keyList = []

    f = conf.get("fields", "_id")
    csvfile.write(f + delim)
    for (key, val) in conf.items('fields'):
        keyList.append(key)
        if val == "Idpost":
            continue
        if val == "postedTime":
            csvfile.write("postedTime" + delim + "Year" + delim + "Month" + delim + "Day" + delim + "Time" + delim)
            continue
        if val == "matchingrulesvalue" and resultList[1] > 1:
            csvfile.write("matchingrulesvalues" + delim)
            for i in range(1, resultList[1] + 1):
                csvfile.write("matchingrulesvalue" + str(i) + delim)
            continue
        elif val == "matchingrulesvalue" and resultList[1] <= 1:
            csvfile.write("matchingrulesvalues" + delim + "matchingrulesvalue" + delim)
            continue
        if val == "matchingrulestag" and resultList[4] > 1:
            for i in range(1, resultList[4] + 1):
                csvfile.write("matchingrulestag" + str(i) + delim)
            continue
        if val == "entitieshtagstext" and resultList[2] > 1:
            for i in range(1, resultList[2] + 1):
                csvfile.write("entitieshtagstext" + str(i) + delim)
            continue
        if val == "entitiesusrmentions" and resultList[3] > 1:
            for i in range(1, resultList[3] + 1):
                csvfile.write("entitiesusrmentionsidstr" + str(i) + delim + "entitiesusrmentionssname" + str(i) + delim + "entitiesusrmentionsname" + str(i) + delim)
            continue
        if val == "entitiesusrmentions" and resultList[3] <= 1:
            csvfile.write("entitiesusrmentionsidstr" + delim + "entitiesusrmentionssname" + delim + "entitiesusrmentionsname" + delim)
        csvfile.write(val + delim)

    ruleFile = open("rules.json")
    ruleJson = json.loads(ruleFile.read())
    # For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList[0]:
        csvfile.write("\n")
        for key in keyList:
            if key in result:
                if key == "_id":
                    csvfile.write("tw" + result[key] + delim)
                if key == "pt":
                    date = result[key]
                    csvfile.write(str(date) + delim)
                    
                    csvfile.write(date.strftime("%Y") + delim)
                    csvfile.write(date.strftime("%b") + delim)
                    csvfile.write(date.strftime("%d") + delim)
                    csvfile.write(date.strftime("%H:%M:%S") + delim)
                    continue
                if key  == "auo":
                    if result[key] is None:
                        csvfile.write('.' + delim)
                        continue
                if key == "mrv":
                    if resultList[1] > 0:
                        # translatedRules = []
                        # for j in range(0, resultList[1]):
                        #     try:
                        #         for (key, val) in ruleJson:
                        #             if val == result[key][j]:
                        #                 translatedRules.append(key)
                        #         # translatedRules.append(conf.get("fields", str(result[key][j])))
                        #         translatedRules.append(ruleJson)
                        #     except IndexError:
                        #         pass
                        # for j in translatedRules:
                        #     csvfile.write(j + ';')
                        csvfile.write("" + delim)
                        for j in range(0, resultList[1]):
                        # for j in result['mrv']:
                            try:
                                csvfile.write(result[key][j] + delim)
                            except IndexError:
                                csvfile.write('.' + delim)
                        continue
                if key == "mrt":
                    if resultList[4] > 0:
                        mrtList = result[key].split(';')
                        for j in range(0, resultList[4]):
                            try:
                                csvfile.write(mrtList[j] + delim)
                            except IndexError:
                                csvfile.write('' + delim)
                    continue
                if key == "eumh":
                    if resultList[2] > 0:
                        for j in range(0, resultList[2]):
                        # for j in result['eumh']:
                            try:
                                csvfile.write("\"" + result[key][j] + "\"" + delim)
                            except IndexError:
                                csvfile.write('' + delim)
                        continue
                if key == "eum":
                    if resultList[3] > 1:
                        for j in range(0, resultList[3]):
                        # for j in result['eumh']:
                            try:
                                csvfile.write((result[key][j]['is'] + delim + result[key][j]['sn'] + delim + result[key][j]['n'] + delim).encode('utf-8').strip())
                            except IndexError:
                                csvfile.write('' + delim + '' + delim + '' + delim)
                        continue
                    else:
                        csvfile.write('' + delim + '' + delim + '' + delim)
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
                csvfile.write("\"" + entry.encode('ascii','ignore') + "\"" + delim)
            else:
                csvfile.write('' + delim)
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
    print outputSet[3]
    printCSV(csvfile, outputSet)
    csvfile.close()