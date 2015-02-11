import json
import sys
import os
import re
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time
import csv, codecs, cStringIO

reload(sys)
sys.setdefaultencoding("utf-8")
class CSVUnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8-sig", **kwds):
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        '''writerow(unicode) -> None
        This function takes a Unicode string and encodes it to the output.
        '''
        self.writer.writerow([s.encode("utf-8", "ignore") for s in row])
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

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
        count = 1
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
                outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag, count)
                printCSV(outputSet, path, month, filterRule)
                outputSet = ()
                dataSet = []
                longestHtag = 0
                longestRule = 0
                longestUMen = 0
                longestRTag = 0
                count += 1
        data.close()

    print "\nWriting to File...\n"
    outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag, count)
    printCSV(outputSet, path, month, filterRule)


# ========================================================================================
def printHead(csvfile, resultList, delim, conf):
    # f = conf.get("fields", "_id")
    # csvfile.write(f + delim)
    keyList = []
    for (key, val) in conf.items('fields'):
        keyList.append(key)
        if val == "postedTime":
            csvfile.write(val + delim + "Year" + delim + "Month" + delim + "Day" + delim + "Time" + delim)
            continue
        if val == "matchingrulesvalue":
            if resultList[1] > 1:
                csvfile.write("matchingrulesvalues" + delim)
                for i in range(1, resultList[1] + 1):
                    csvfile.write(val + str(i) + delim)
                continue
            else:
                csvfile.write("matchingrulesvalues" + delim + val + delim)
                continue
        if val == "matchingrulestag" and resultList[4] > 1:
            for i in range(1, resultList[4] + 1):
                csvfile.write(val + str(i) + delim)
            continue
        if val == "entitieshtagstext" and resultList[2] > 1:
            for i in range(1, resultList[2] + 1):
                csvfile.write("entitieshtagstext" + str(i) + delim)
            continue
        if val == "entitiesusrmentions":
            if resultList[3] >= 1:
                for i in range(1, resultList[3] + 1):
                    csvfile.write("entitiesusrmentionsidstr" + str(i) + delim + "entitiesusrmentionssname" + str(i) + delim + "entitiesusrmentionsname" + str(i) + delim)
            else:
                csvfile.write("entitiesusrmentionsidstr" + delim + "entitiesusrmentionssname" + delim + "entitiesusrmentionsname" + delim)
            continue
        csvfile.write(val + delim)
    csvfile.write('\n')
    writer = CSVUnicodeWriter(csvfile, quoting=csv.QUOTE_ALL)
    return (writer, keyList)

# ========================================================================================

def printCSV(resultList, path, month, rule):
    delim = ","
    fileGen = fileGenerator(path, month, rule)
    csvfile = next(fileGen)
    counter = resultList[5]
    csvfile = open(path + month + rules + '_' + str(counter) + '.csv', 'wb')
    conf = ConfigParser.ConfigParser()
    conf.read("mongoToFields.cfg")
    (writer, keyList) = printHead(csvfile, resultList, delim, conf)
    ruleFile = open("rules.json")
    ruleFile.seek(0, 0)
    ruleLines = ruleFile.readlines()
    conf = ConfigParser.ConfigParser()
    conf.read("mongoToFields.cfg")

    for result in resultList[0]:
        if os.path.getsize(csvfile.name) / 1048576 > 100:
            csvfile.close()
            csvfile = next(fileGen)
            keyList = printHead(csvfile, resultList, delim)
        row = []
        for key in keyList:
            if key in result:
                if key == "_id":
                    row.append("tw" + result[key])
                    continue
                if key == "pt":
                    date = result[key]
                    row.append(str(date))
                    row.append(date.strftime("%Y"))
                    row.append(date.strftime("%b"))
                    row.append(date.strftime("%d"))
                    row.append(date.strftime("%H:%M:%S"))
                    continue
                if key  == "auo":
                    if result[key] is None:
                        row.append('.')
                        continue
                if key == "mrv":
                    if resultList[1] > 0:
                        translatedRules = []
                        rules = ""
                        for j in range(0, resultList[1]):
                            try:
                                rule = ':'.join(ruleLines[int(result[key][j])].split(':')[0:-1])
                                translatedRules.append(rule)
                            except IndexError:
                                pass
                        tempKey = conf.get("fields", "mrv") + 's'
                        rules = ""
                        for j in translatedRules:
                            rules += j + ';'
                        row.append(rules)
                        for j in range(0, resultList[1]):
                            try:
                                row.append(result[key][j])
                            except IndexError:
                                row.append("")
                        continue
                    else:
                        row.append("")
                        row.append("")
                if key == "mrt":
                    if resultList[4] > 0:
                        mrtList = result[key].split(';')
                        for j in range(0, resultList[4]):
                            try:
                                row.append(mrtList[j] + ';')
                            except IndexError:
                                row.append("")
                    else:
                        row.append("")
                    continue
                if key == "eumh":
                    if resultList[2] >= 1:
                        for j in range(0, resultList[2]):
                            try:
                                row.append(result[key][j])
                            except IndexError:
                                row.append("")
                        continue
                    elif resultList[2] == 0:
                        row.append("")
                    continue
                if key == "eum":
                    if resultList[3] >= 1:
                        for j in range(0, resultList[3]):
                            try:
                                for (k, v) in result[key][j].iteritems():
                                    row.append(v.strip())
                            except IndexError:
                                row.append("")
                                row.append("")
                                row.append("")

                        continue
                    elif resultList[3] == 0:
                        row.append("")
                        row.append("")
                        row.append("")
                    continue
                entry = result[key]
                row.append(unicode(result[key]))
            else:
                row.append("")
        writer.writerow(row)
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
        # printCSV(outputSet, path, month, str(