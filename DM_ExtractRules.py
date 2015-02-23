import sys
import os
import ConfigParser
from pymongo import MongoClient
from datetime import datetime
import time
import csv, codecs, cStringIO

# Set the default encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")

# Class given in Python 2.7 documentation for handling of unicode documents
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
# Function to query Mongo Collection and retrieve all records matching a particular rule
def queryDB(mongoConf, month, year, filterRule, path, ruleLines):
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
    coll_names = set()
    # Generate Collection names, i.e. MonYY_x, where x is in range(1, 7)
    collName = month + year[2:]
    # Initialize a list to hold all the data queried from the Database
    dataSet = []
    for j in range(1, 7):
        coll_names.add(collName + '_' + str(j))
    # For each of the collections
    count = 1
    for i in coll_names:
        coll = db[i]
        try:
            # Query to find all records of a particular rule
            data = coll.find({'mrv': {'$in': [int(filterRule)]}}, timeout=False)
        except pymongo.errors.CursorNotFound:
            continue
        # Maintain count to track the file number of the corresponding rule file
        # For each record returned by the query
        for k in data:
            # Track the longest of each of the array fields
            if 'eumh' in k:
                lenHtag = len(k['eumh'])
                if lenHtag > longestHtag:
                    longestHtag = lenHtag
            if 'eum' in k:
                lenUM = len(k['eumh'])
                if lenUM > longestUMen:
                    longestUMen = lenUM
            if 'mrt' in k:
                lenRTag = len(k['mrt'].split(';'))
                if lenRTag > longestRTag:
                    longestRTag = lenRTag
            if len(k['mrv']) > longestRule:
                longestRule = len(k['mrv'])
            # Add the record to the dataset list
            dataSet.append(k)
            print k['_id']
            # If the size of the dataSet list exceeds a certain threshold, write to a file
            if sys.getsizeof(dataSet) > 106954752:
                print "\nWriting to File...\n"
                # Package all the control information into a tuple
                outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag, count)
                # Send all the control information to the PrintCSV function to convert the Dict into a CSV
                printCSV(outputSet, path, month, filterRule, ruleLines)
                # Reset all the values and increment the count to denote that the next file must be started when writing next
                outputSet = ()
                dataSet = []
                longestHtag = 0
                longestRule = 0
                longestUMen = 0
                longestRTag = 0
                count += 1
        # Close the cursor
        data.close()

    # Once iteration over all the records is done, send all control info to PrintCSV to generate CSV file
    print "\nWriting to File...\n"
    outputSet = (dataSet, longestRule, longestHtag, longestUMen, longestRTag, count)
    printCSV(outputSet, path, month, filterRule, ruleLines)


# ========================================================================================
# Function to print the Header of the CSV file
def printHead(csvfile, resultList, delim, conf):
    # Initialize the keyList to an empty list to hold all the keys to iterate over while printing data
    keyList = []
    # Iterating over all the column headings to be printed
    for (key, val) in conf.items('fields'):
        # Adding heading to keyList
        keyList.append(key)
        # Break postedTime into multiple fields as shown below
        if val == "postedTime":
            csvfile.write(val + delim + "Year" + delim + "Month" + delim + "Day" + delim + "Time" + delim)
            continue
        # Unroll matchingrulesvalue into individual fields with a number next to it
        if val == "matchingrulesvalue":
            if resultList[1] > 1:
                csvfile.write("matchingrulesvalues" + delim)
                for i in range(1, resultList[1] + 1):
                    csvfile.write(val + str(i) + delim)
                continue
            else:
                csvfile.write("matchingrulesvalues" + delim + val + delim)
                continue
        # Unroll matchingrulestag into individual fields with a number next to it
        if val == "matchingrulestag" and resultList[4] > 1:
            for i in range(1, resultList[4] + 1):
                csvfile.write(val + str(i) + delim)
            continue
        # Unroll entitieshtagstext into individual fields with a number next to it
        if val == "entitieshtagstext" and resultList[2] > 1:
            for i in range(1, resultList[2] + 1):
                csvfile.write("entitieshtagstext" + str(i) + delim)
            continue
        # Unroll entitiesusrmentions into individual fields with a number next to it
        if val == "entitiesusrmentions":
            if resultList[3] >= 1:
                for i in range(1, resultList[3] + 1):
                    csvfile.write("entitiesusrmentionsidstr" + str(i) + delim + "entitiesusrmentionssname" + str(i) + delim + "entitiesusrmentionsname" + str(i) + delim)
            else:
                csvfile.write("entitiesusrmentionsidstr" + delim + "entitiesusrmentionssname" + delim + "entitiesusrmentionsname" + delim)
            continue
        # Else write the field name into the file
        csvfile.write(val + delim)
    # Write a new line character to denote the end of header line
    csvfile.write('\n')
    # Create a Unicode based CSV writer with the opened csvfile and quoting all the values in all the fields
    writer = CSVUnicodeWriter(csvfile, quoting=csv.QUOTE_ALL)
    # Return a writer, keyList tuple
    return (writer, keyList)

# ========================================================================================

def printCSV(resultList, path, month, rule, ruleLines):
    # Mention the delimiter
    delim = ","
    # Retrieve the count from the resultList parameter
    counter = resultList[5]
    # Create a new CSV file with the specified name in the specified path and open it as a binary file
    csvfile = open(path + month + rule + '_' + str(counter) + '.csv', 'wb')
    conf = ConfigParser.ConfigParser()
    conf.read(conf_path.format("mongoToFields.cfg"))
    # Retrieve the CSVUnicodeWriter and the list of all the field names after the header of the CSV file has been printed
    (writer, keyList) = printHead(csvfile, resultList, delim, conf)
    # ruleFile = open("rules.json")
    # ruleFile.seek(0, 0)
    # ruleLines = ruleFile.readlines()

    # Iterate over each record
    for result in resultList[0]:
        # if os.path.getsize(csvfile.name) / 1048576 > 100:
        #     csvfile.close()
        #     csvfile = next(fileGen)
        #     keyList = printHead(csvfile, resultList, delim)
        # Initialize an empty list to store values that are to be written into a row of the CSV file
        row = []
        # For each key in keyList
        for key in keyList:
            # If the key exists in the current record
            if key in result:
                if key == "_id":
                    # Edit the tweet id and append the new id to the row list
                    row.append("tw" + result[key])
                    continue
                if key == "pt":
                    # Edit the postedTime into the postedTime, Year, Month, Day and Time fields
                    date = result[key]
                    row.append(str(date))
                    row.append(date.strftime("%Y"))
                    row.append(date.strftime("%b"))
                    row.append(date.strftime("%d"))
                    row.append(date.strftime("%H:%M:%S"))
                    continue
                if key  == "auo":
                    # If the actorUtcOffset isn't present, add a '.'
                    if result[key] is None:
                        row.append('.')
                        continue
                # Unroll the matchingrulestag array into multiple fields
                if key == "mrt":
                    tag = result[key].split(';')
                    tag += [""] * (resultList[4] - len(tag))
                    row.extend(tag)
                    continue
                # Unroll the matchingrulesvalue array into multiple fields
                if key == "mrv":
                    translatedRules = []
                    rules = []
                    for j in range(0, resultList[1]):
                        try:
                            rule = ':'.join(ruleLines[int(result[key][j])].split(':')[0:-1])
                            translatedRules.append(rule)
                            rules.append(str(result[key][j]))
                        except IndexError:
                            pass
                    row.append(';'.join(translatedRules))
                    rules += [""] * (resultList[1] - len(result[key]))
                    row.extend(rules)
                    continue
                # Unroll the entitiesusrmentions array and its constituent fields into multiple fields
                if key == "eum":
                    usrMentions = []
                    for j in result[key]:
                        for (k, v) in j.iteritems():
                            usrMentions.append(v)
                    # usrMentions = [j for j in result[key]]
                    usrMentions += ["", "", ""] * (resultList[3] - len(result[key]))
                    row.extend(usrMentions)
                    continue
                # Unroll the entitieshtagstext array into multiple fields
                if key == "eumh":
                    htag = []
                    htag = result[key].split(';')
                    print resultList[2] - len(htag)
                    htag += [""] * (resultList[2] - len(htag))
                    row.extend(htag)
                    continue
                # Unroll the matchingrulesvalue array into multiple fields
                # if key == "mrv":
                #     if resultList[1] > 0:
                #         translatedRules = []
                #         rules = ""
                #         for j in range(0, resultList[1]):
                #             try:
                #                 rule = ':'.join(ruleLines[int(result[key][j])].split(':')[0:-1])
                #                 translatedRules.append(rule)
                #             except IndexError:
                #                 pass
                #         for j in translatedRules:
                #             rules += j + ';'
                #         row.append(';'.join(translatedRules))
                #         for j in range(0, resultList[1]):
                #             try:
                #                 row.append(str(result[key][j]))
                #             except IndexError:
                #                 row.append("")
                #         continue
                #     else:
                #         row.append("")
                #         row.append("")
                # # Unroll the matchingrulestag array into multiple fields
                # if key == "mrt":
                #     if resultList[4] > 0:
                #         mrtList = result[key].split(';')
                #         for j in range(0, resultList[4]):
                #             try:
                #                 row.append(mrtList[j] + ';')
                #             except IndexError:
                #                 row.append("")
                #     else:
                #         row.append("")
                #     continue
                # # Unroll the entitieshtagstext array into multiple fields
                # if key == "eumh":
                #     if resultList[2] >= 1:
                #         for j in range(0, resultList[2]):
                #             try:
                #                 row.append(result[key][j])
                #             except IndexError:
                #                 row.append("")
                #         continue
                #     elif resultList[2] == 0:
                #         row.append("")
                #     continue
                # # Unroll the entitiesusrmentions array and its constituent fields into multiple fields
                # if key == "eum":
                #     if resultList[3] >= 1:
                #         for j in range(0, resultList[3]):
                #             try:
                #                 for (k, v) in result[key][j].iteritems():
                #                     row.append(v.strip())
                #             except IndexError:
                #                 row.append("")
                #                 row.append("")
                #                 row.append("")

                #         continue
                #     elif resultList[3] == 0:
                #         row.append("")
                #         row.append("")
                #         row.append("")
                #     continue
                # # Add the field to the row in unicode format
                # row.append(unicode(result[key]))
                row.append(unicode(result[key]))
            else:
                # If the key doesn't exist, append an empty string into the row
                row.append("")
        # Writer writes the row list
        writer.writerow(row)
    # Close the CSV file
    csvfile.close()

# ========================================================================================
# Command to run the script
# python DM_Extract.py <Capitalized 3 letter month name> <Four digit year>
# Eg - python DM_Extract.py Aug 2014


monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

if __name__ == "__main__":
    month = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    # Get unformatted path to the Config files
    conf_path = conf.get("conf", "conf_path")
    # Get the destination path
    dest = conf.get("twitter", "prod_dest_path")
    # Generate full path for the corresponding year and month
    path = dest.format(year + monthToNames[month], 'CSVRULES')
    
    # Future work
    # If time frame based, generate files based on start and end month and rule
    # if start_time == end_time:
    #     window = [start_time]
    #     csvfile = open(start_time + '_' + '.csv', 'w')
    # else:
    #     window = [start_time, end_time]
    #     csvfile = open(start_time + '_' + end_time + '.csv', 'w')
    # rules = sys.argv[3:]
    
    # Get the total number of rules and the list of all the rules
    r = open(conf_path.format("rules.json"))
    ruleLines = r.readlines()
    r.close()
    rules = range(1, len(ruleLines))
    
    # for each rule
    for i in rules:
        # Run queryDB for the corresponding month, year and rule
        queryDB(conf, month, year, str(i), path, ruleLines)
        print i