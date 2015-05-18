import sys
import os
import json
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time
import pandas as pd
from utility import mkdir_p, CSVUnicodeWriter
from pympler import asizeof


# ========================================================================================
# Function to query Mongo Collection and retrieve all records matching a particular rule
def queryDB(mongoConf, month, year, filterRule, path, rtLines):
    host = mongoConf.get('mongo', 'host')
    port = int(mongoConf.get('mongo', 'port'))
    username = mongoConf.get('mongo', 'username')
    password = mongoConf.get('mongo', 'password')
    authDB = self._conf.get('mongo', 'authDB')
    # Create a new MongoDB client
    client = MongoClient(host, port)
    # User Authentication
    client.twitter.authenticate(username, password, source=authDB)
    db = client.twitter

    coll_names = set()
    # Generate Collection names, i.e. MonYY_x, where x is in range(1, 7)
    collName = month.title() + year[2:]
    # Initialize a list to hold all the data queried from the Database
    dataSet = []
    for j in range(1, 7):
        coll_names.add(collName + '_' + str(j))
    # Maintain count to track the file number of the corresponding rule file
    count = 1
    # Obtaining Mongo fields to CSV fields mapping as a dict
    with (conf_path.format('mongoToFields.json')) as fieldsFile:
        mongoFields = json.loads(fieldsFile.read())
    # For each of the collections
    for i in coll_names:
        coll = db[i]
        try:
            # Query to find all records of a particular rule
            data = coll.find({'mrv': {'$in': [int(filterRule)]}}, timeout=False)
        except pymongo.errors.CursorNotFound:
            continue
        # For each record returned by the query
        for k in data:
            modifiedObj = {}
            # Translate field names
            for (key, val) in k.iteritems():
                modifiedObj[mongoFields[key]] = val

            # Add extra fields for Posted Time
            if 'postedTime' in modifiedObj:
                modifiedObj['Year'] = date.strftime("%Y")
                modifiedObj['Month'] = date.strftime("%b")
                modifiedObj['Day'] = date.strftime("%d")
                modifiedObj['Time'] = date.strftime("%H:%M:%S")
            # If actorutcOffset is None, then initialize field with a '.'
            if modifiedObj['actorutcOffset'] is None:
                modifiedObj['actorutcOffset'] = '.'
            # Unroll all the arrays
            index = 1
            if 'entitieshtagstext' in modifiedObj:
                for i in modifiedObj['entitieshtagstext']:
                    modifiedObj['entitieshtagstext' + str(index)] = i
                    index += 1
                del(modifiedObj['entitieshtagstext'])
                index = 1
            if 'entitiesusrmentions' in modifiedObj:
                for i in modifiedObj['entitiesusrmentions']:
                    modifiedObj['entitiesusrmentionsidstr' + str(index)] = i['is']
                    modifiedObj['entitiesusrmentionsname' + str(index)] = i['n']
                    modifiedObj['entitiesusrmentionssname' + str(index)] = i['sn']
                    index += 1
                del(modifiedObj['entitiesusrmentions'])
                index = 1
            # Translated and unroll matchingrulestag array
            if 'matchingrulestag' in modifiedObj:
                translatedTags = []
                for i in modifiedObj['matchingrulestag']:
                    tag = (rtLines[0][int(t)].split(':')[0]).strip('"')
                    modifiedObj['matchingrulestag' + str(index)] = tag
                    index += 1
                del(modifiedObj['matchingrulestag'])
                index = 1
            # Unroll matchingrulesvalue array and create a new field to hold all the translated matchingrulesvalues
            if 'matchingrulesvalue' in modifiedObj:
                translatedRules = []
                for i in modifiedObj['matchingrulesvalue']:
                    modifiedObj['matchingrulesvalue' + str(index)] = i
                    index += 1
                    rule = ':'.join(rtLines[1][int(result[key][j])].split(':')[0:-1])
                    translatedRules.append(rule)
                modifiedObj['matchingrulesvalues'] = ';'.join(translatedRules)
                del(modifiedObj['matchingrulesvalue'])
                index = 1
            # Add the record to the dataset list
            dataSet.append(modifiedObj)
            # If the size of the dataSet list exceeds a certain threshold, write to a file
            if asizeof.asizeof(dataSet) > 104857600:
                print "\nWriting to File...\n"
                # Create a new DataFrame and write to a csv file
                df = pd.DataFrame(dataSet)
                with open(path + month + rule + '_' + str(counter) + '.csv', 'wb') as csvfile:
                    df.to_csv(csvfile, sep=',', index=False)
                count += 1
                

# ========================================================================================
# Command to run the script
# python DM_Extract.py <Capitalized 3 letter month name> <Four digit year>
# Eg - python DM_Extract.py Aug 2014



if __name__ == "__main__":
    month = sys.argv[1].lower()
    year = sys.argv[2]
    monthDict = {v.lower(): (str(k).zfill(2)) for (k, v) in (calendar.month_abbr)}
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    # Get unformatted path to the Config files
    conf_path = conf.get("conf", "conf_path")
    # Get the destination path
    dest = conf.get("twitter", "prod_dest_path")
    # Generate full path for the corresponding year and month
    path = dest.format(year + monthDict[month], 'CSVRULES')
    mkdir_p(path)
    # Get the total number of rules and the list of all the rules
    r = open(conf_path.format("tw_rules.json"))
    ruleLines = r.readlines()
    r.close()
    r = open(conf_path.format("tw_tags.json"))
    tagLines = r.readlines()
    r.close()
    rules = range(1, len(ruleLines))
    rtLines = (tagLines, ruleLines)
    # for each rule
    for i in rules:
        # Run queryDB for the corresponding month, year and rule
        queryDB(conf, month, year, str(i), path, rtLines)
        print i