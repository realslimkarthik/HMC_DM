from pymongo import MongoClient, errors
import json
import sys
import os
import ConfigParser
import DM_TWITTER_K as dmt

# ========================================================================================
def parseJson(jsonFile):
    jsonFileHandler = open(jsonFile, 'r')
    dailyJson = []
    for i in jsonFileHandler.readlines():
        dailyJson = json.loads(i)
    return dailyJson

# ========================================================================================
def fixMongo(collection, mongoRecord):
    record = collection.find_one({'_id': mongoRecord['_id']})
    if record != {}:
        record['_id'] = int(record['_id'])
    for (key, val) in mongoRecord:
        if key in record:
            if mongoRecord[key] != record[key]:
                record[key] = mongoRecord[key]

# ========================================================================================


if __name__ == "__main__":
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config.cfg")

    if choice == "dev":
        inputJson = "H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\tw2014-08-01"
        dailyJson = parseJson()
    elif choice =="prod":
        op = sys.argv[2]
        current_month = sys.argv[3]
        current_year = sys.argv[4]
        collName = current_month[0:3] + current_year[2:]
        logging.basicConfig(filename='prodUpload' + collName +'.log', level=logging.DEBUG)
        src_path = conf.get("twitter", "prod_src_path").format(current_month + '-' + current_year + '-' + 'Master')
        fileList = os.listdir(src_path)
        current_month = current_month[0:2]
        if op == "transform":
            for j in fileList:
                if len(j.split('_')) == 3:
                    fileName = j.split('_')[-1].split('.')[0]
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