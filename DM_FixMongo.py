from pymongo import MongoClient, errors
import json
import sys
import os
import ConfigParser
import logging
import DM_TWITTER_K as dmt

# ========================================================================================
def parseJson(jsonFile):
    jsonFileHandler = open(jsonFile, 'r')
    dailyJson = []
    for i in jsonFileHandler.readlines():
        yield json.loads(i)
    # return dailyJson

# ========================================================================================
def fixMongo(collection, updatedRecord):
    # record = collection.find_one({'_id': updatedRecord['_id']})
    # if record != {}:
    # for (key, val) in updatedRecord.iteritems():
    #     if key in record:
    #         if updatedRecord[key] != record[key]:
    #             record[key] = updatedRecord[key]
    #     else:
    #         record[key]
    try:
        updatedRecord['_id'] = updatedRecord['_id'].split(':')[2]
    except KeyError:
        return
    collection.save(updatedRecord)
    logging.info("Updated record with _id=" + updatedRecord["_id"])
    print updatedRecord["_id"]

# ========================================================================================
def JSONtoMongo(fileName, collName):
    fieldConf = ConfigParser.ConfigParser()
    fieldConf.read("fieldstoMongo.cfg")
    mongoConf = ConfigParser.ConfigParser()
    mongoConf.read("config.cfg")
    host = mongoConf.get("mongo", "host")
    port = int(mongoConf.get("mongo", "port"))
    client = MongoClient(host, port)
    db = client.twitter
    collection = db[collName]
    inputFile = open(fileName, 'r')
    for i in inputFile.readlines():
        try:
            lineJson = json.loads(i)
        except ValueError:
            continue
        trimmedJson = {}
        mykeys = []
        dmt.extract(lineJson, trimmedJson, mykeys)
        updatedRecord = {}
        for (key, val) in trimmedJson.iteritems():
            # if fieldConf.has_option('fields', key):
            updatedRecord[fieldConf.get('fields', key)] = val
        fixMongo(collection, updatedRecord)

# ========================================================================================

if __name__ == "__main__":
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config.cfg")

    if choice == "dev":
        inputJson = "H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\tw2014-08-01.json"
        dailyJson = parseJson(inputJson)
        x = dailyJson.next()
        print x
        # print dailyJson[0]
    elif choice =="prod":
        op = sys.argv[2]
        conf = ConfigParser.ConfigParser()
        conf.read("config.cfg")
        current_month = sys.argv[3]
        current_year = sys.argv[4]
        collName = current_month[0:3] + current_year[2:]
        logging.basicConfig(filename='prodFix' + collName +'.log', level=logging.DEBUG)
        src_path = conf.get("twitter", "prod_src_path").format(current_month + '-' + current_year)
        fileList = os.listdir(src_path)
        current_month = current_month[0:2]
        if op == "transform":
            for j in fileList:
                if len(j.split('-')) == 3:
                    fileName = j.split('-')[-1].split('.')[0]
                    logging.info("Started fixing " + j)
                    if int(fileName) < 6:
                        JSONtoMongo(src_path + j, collName + "_1")
                    elif int(fileName) < 11:
                        JSONtoMongo(src_path + j, collName + "_2")
                    elif int(fileName) < 16:
                        JSONtoMongo(src_path + j, collName + "_3")
                    elif int(fileName) < 21:
                        JSONtoMongo(src_path + j, collName + "_4")
                    elif int(fileName) < 26:
                        JSONtoMongo(src_path + j, collName + "_5")
                    elif int(fileName) < 32:
                        JSONtoMongo(src_path + j, collName + "_6")
    elif choice == "fixRules":
        # fileName = 'prodUploadJul14.log'
        extraRules = set()
        f = open('prodUploadJul14.log')
        f.seek(0, 0)
        for i in f.readlines():
            # if i.split(':')[0] == "WARNING":
            line = i.split('=')
            if len(line) > 1:
                extraRules.add(line[-1])
        f.close()
        f = open('rules.json', 'a')
        f.write('\n')
        count = 521
        for i in extraRules:
            f.write("\"" + i.strip() + "\":" + "\"" + str(count) + "\"" + ',' + '\n')
            count += 1
        f.write('}')
        f.close()
        print len(extraRules)