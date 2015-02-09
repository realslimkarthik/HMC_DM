import json
import sys
import os
import re
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time

#     coll_names = set()
#     dataSet = []
#     for i in window:
#         for j in range(1, 7):
#             coll_names.add(i + '_' + str(j))

# ========================================================================================
def queryDB(mongoConf, month, year, filterRule, path):
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
    collName = month + year[2:]
    dataSet = []
    for j in range(1, 7):
        coll_names.add(collName + '_' + str(j))
    for i in coll_names:
        coll = db[i]
        try:
            data = coll.find({'mrv': {'$in': [filterRule]}}, timeout=False)
        except pymongo.errors.CursorNotFound:
            continue
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
            dataSet.append(k)
            print k['_id']
            if sys.getsizeof(dataSet) > 106954752:
                print "\nWriting to File...\n"
                outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag)
                printCSV(outputSet, path, month, filterRule)
                outputSet = ()
                dataSet = []
                longestHtag = 0
                longestRule = 0
                longestUMen = 0
                longestRTag = 0
        data.close()

    print "\nWriting to File...\n"
    outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag)
    printCSV(outputSet, path, month, filterRule)

    # return (dataSet, longestRule, longestHtag, longestUMen, longestRTag)

# ========================================================================================
def printHead(csvfile, resultList, delim):
    conf = ConfigParser.ConfigParser()
    conf.read("mongoToFields.cfg")
    f = conf.get("fields", "_id")
    keyList = []
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
            continue
        csvfile.write(val + delim)
    return keyList

# ========================================================================================
def fileGenerator(path, month, rule):
    counter = 0
    while True:
        counter += 1
        f = open(path + month + rule + '_' + str(counter) + '.csv', 'w')
        yield f

# ========================================================================================

def printCSV(resultList, path, month, rule):
    delim = ","
    fileGen = fileGenerator(path, month, rule)
    csvfile = next(fileGen)
    keyList = printHead(csvfile, resultList, delim)
    ruleFile = open("rules.json")
    ruleFile.seek(0, 0)
    ruleLines = ruleFile.readlines()

    for result in resultList[0]:
        # if os.path.getsize(csvfile.name) / 1048576 > 100:
        #     csvfile.close()
        #     csvfile = next(fileGen)
        #     keyList = printHead(csvfile, resultList, delim)
        csvfile.write("\n")
        for key in keyList:
            if key in result:
                if key == "_id":
                    csvfile.write("tw" + result[key] + delim)
                    continue
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
                        translatedRules = []
                        for j in range(0, resultList[1]):
                            try:
                                rule = ':'.join(ruleLines[int(result[key][j])].split(':')[0:-1])
                                translatedRules.append(rule)
                            except IndexError:
                                pass
                        for j in translatedRules:
                            csvfile.write(j + ';')
                        csvfile.write(delim)
                        for j in range(0, resultList[1]):
                            try:
                                csvfile.write(result[key][j] + delim)
                            except IndexError:
                                csvfile.write('' + delim)
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
                    if resultList[2] > 1:
                        for j in range(0, resultList[2]):
                        # for j in result['eumh']:
                            try:
                                csvfile.write("\"" + result[key][j].encode('utf-8', 'ignore') + "\"" + delim)
                            except IndexError:
                                csvfile.write(delim)
                        continue
                    elif resultList[2] == 0:
                        csvfile.write(delim)
                if key == "eum":
                    if resultList[3] > 1:
                        for j in range(0, resultList[3]):
                        # for j in result['eumh']:
                            try:
                                csvfile.write((result[key][j]['is'] + delim + result[key][j]['sn'] + delim + result[key][j]['n'] + delim).encode('utf-8').strip())
                            except IndexError:
                                csvfile.write(delim + delim + delim)
                        continue
                    elif resultList[3] == 0:
                        csvfile.write(delim + delim + delim)
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
                csvfile.write("\"" + entry.encode('utf-8','ignore') + "\"" + delim)
            else:
                csvfile.write('' + delim)
    csvfile.close()

# ========================================================================================

monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

if __name__ == "__main__":
    month = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config.cfg")
    dest = conf.get("twitter", "prod_dest_path")
    path = dest.format(year + monthToNames[month], 'CSVRULES')
    # if start_time == end_time:
    #     window = [start_time]
    #     csvfile = open(start_time + '_' + '.csv', 'w')
    # else:
    #     window = [start_time, end_time]
    #     csvfile = open(start_time + '_' + end_time + '.csv', 'w')
    # rules = sys.argv[3:]
    rules = range(1, 521)
    for i in rules:
        print i
        outputSet = queryDB(conf, month, year, str(i), path)
        # print len(outputSet[0])
        # printCSV(outputSet, path, month, str(i))
