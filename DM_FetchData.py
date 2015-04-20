import sys
import os
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time
from utility import mkdir_p, CSVUnicodeWriter
from pympler import asizeof

def fetch(mongoConf, collName, conditions, fields, fileName):
    host = mongoConf.get("mongo", "host")
    port = int(mongoConf.get("mongo", "port"))
    # Create a new MongoDB client
    client = MongoClient(host, port)
    db = client.twitter
    # Initialize integers to denote the length of the arrays to help unroll arrays in the CSV file
    longestHtag = 0
    longestRule = 0
    longestUMen = 0
    longestRTag = 0
    # Initialize a list to hold all the data queried from the Database
    dataSet = []
    # Initialize a list to hold all the collection names in the Database
    coll_names = set()
    for j in range(1, 7):
        coll_names.add(collName + '_' + str(j))
    # For each of the collections
    count = 1
    for i in coll_names:
        coll = db[i]
        try:
            # Query to find all records of a particular rule
            data = coll.find(conditions, fields, 0, 0, False)
        except pymongo.errors.CursorNotFound:
            continue
        for d in data:
            dataSet.append(d)
            # If the size of the dataSet list exceeds a certain threshold, write to a file
            if asizeof.asizeof(dataSet) > 104857600:
                print "\nWriting to File number " + str(count) + "...\n"
                # Generate the fileName
                csvfile = fileName + str(count) + '.csv'
                # Send the dataSet to put into a CSV file
                printCSV(dataSet, fields, csvfile)
                # Reset the dataSet
                dataSet = []
                # Increment the count to denote that the next file must be started when writing next
                count += 1
        # Close the cursor
        data.close()

    if dataSet != []:
        print "\nWriting to File number " + str(count) + "...\n"
        # Generate the fileName
        csvfile = fileName + str(count) + '.csv'
        # Send the dataSet to put into a CSV file
        printCSV(dataSet, fields, csvfile)
        # Reset the dataSet
        dataSet = []
        # Increment the count to denote that the next file must be started when writing next
        count += 1

def printCSV(resultSet, fields, fileName):
    csv = open(fileName, 'wb')
    keys = []
    csvwriter = CSVUnicodeWriter(csv)
    for (key, val) in fields.iteritems():
        keys.append(key)
    csvwriter.writerow(keys)

    for result in resultSet:
        row = []
        for key in keys:
            try:
                row.append(str(result[key]))
            except KeyError:
                row.append('')
        csvwriter.writerow(row)
    csv.close()

# ========================================================================================
# Command to run the script
# python DM_FetchData.py <Capitalized 3 letter month name> <Four digit year>
# Eg - python DM_FetchData.py Aug 2014

monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 
        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

if __name__ == "__main__":
    month = sys.argv[1]
    year = sys.argv[2]
    m = monthToNames[month]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    dest = conf.get("twitter", "prod_dest_path").format(year + m, 'special')
    mkdir_p(dest)
    # Generate Collection names, i.e. MonYY_x, where x is in range(1, 7)
    collName = month + year[2:]
    # Generate a part of the file name
    fileName = dest + month + '_'
    conditions = {}
    fields = {'_id': 1, 'bp': 1, 
        'ld': 1, 'ln': 1, 'ltc': 1, 'lcc': 1, 'lgc': 1, 'gpld': 1, 
        'gplco': 1, 'gplr': 1, 'gplc': 1, 'gpll': 1, 'gplg': 1}
    # Function that fetches and writes data to CSV
    fetch(conf, collName, conditions, fields, fileName)
