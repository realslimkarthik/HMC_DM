import string
import json
import sys
import os
import random
import re
import ConfigParser
from pymongo import MongoClient, errors
from datetime import datetime
import time
import logging
import DM_Extract as DME
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

# ========================================================================================
# Populates the CSV. Gets CSV's file handle from caller
def CSVfromTwitterJSON(jsonfilename, mode=0, errorfile=None):
    delim = ','
    jsonfile = open(jsonfilename, 'r')
    #Will track all variables seen across all tweets in the file
    #Will contain a dictionary for each processed tweet
    tweetList = []
    # Rule index mapping done via rules.json files
    
    mykeys = []
    longest = {'lh': 0, 'lr': 0, 'lu': 0, 'lt': 0}
    

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
                    # print myline
                    print e
            else:
                #Find the summary count
                if "Replay Request Completed" in myline:
                    print tweet['info']['activity_count']
                else:
                    #Create an empty dictionary
                    tweetObj = {}
                    #Send the JSON dictionary, the empty dictionary, and the list of all keys
                    extract(tweet, tweetObj, mykeys, longest)
                    tweetList.append(tweetObj)
                    #Add the output dictionary to the list
                    print tweetObj['matchingrulesvalue']
                    try:
                        print tweetObj['geocoordinates']
                    except KeyError:
                        # print "NA"
                        continue
    jsonfile.close()
    resultList = (tweetList, longest['lr'], longest['lh'], longest['lu'], longest['lt'])
    if mode == 0:
        fields = ConfigParser.ConfigParser()
        fields.read(conf_path.format("fields.cfg"))
        csvfile = open(jsonfilename.split('.')[0] + '.csv', 'wb')
        writer = DME.printHead(csvfile, resultList, delim, fields)
        keyList = [val for (key, val) in fields.items('fields')]
        printCSV(csvfile, resultList, writer[0], keyList, delim)
        csvfile.close()
    elif mode == 1:
        return resultList
# ========================================================================================
def printCSV(csvfile, resultList, writer, keyList, delim):
    for result in resultList[0]:
        row = []
        for key in keyList:
            if key in result:
                if key == "Idpost":
                    # Edit the tweet id and append the new id to the row list
                    row.append("tw" + result[key].split(':')[2])
                    continue
                if key == "postedTime":
                    # Edit the postedTime into the postedTime, Year, Month, Day and Time fields
                    dateFrag = result['postedTime'].split('T')
                    dateFrag[1] = dateFrag[1].split('.')[0]
                    dateStr = ''.join(' ' + d for d in dateFrag).strip()
                    timeStruct = time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
                    dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
                    row.append(str(dateObj))
                    row.append(dateObj.strftime("%Y"))
                    row.append(dateObj.strftime("%b"))
                    row.append(dateObj.strftime("%d"))
                    row.append(dateObj.strftime("%H:%M:%S"))
                    continue
                if key  == "actorUtcOffset":
                    # If the actorUtcOffset isn't present, add a '.'
                    if result[key] is None:
                        row.append('.')
                        continue
                # Unroll the matchingrulesvalue array into multiple fields
                if key == "matchingrulesvalue":
                    if resultList[1] > 0:
                        translatedRules = []
                        rules = ""
                        row.append(';'.join(result[key]))
                        print result[key]
                        for j in range(0, resultList[1]):
                            try:
                                row.append(str(result[key][j]))
                            except IndexError:
                                row.append("")
                        continue
                    else:
                        row.append("")
                        row.append("")
                # Unroll the matchingrulestag array into multiple fields
                if key == "matchingrulestag":
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
                # Unroll the entitieshtagstext array into multiple fields
                if key == "entitieshtagstext":
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
                # Unroll the entitiesusrmentions array and its constituent fields into multiple fields
                if key == "entitiesusrmentions":
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
                # Add the field to the row in unicode format
                row.append(unicode(result[key]))
            else:
                # If the key doesn't exist, append an empty string into the row
                row.append("")
    # Writer writes the row list
        writer.writerow(row)

# ========================================================================================
def removeKey(key):
    conf = ConfigParser.ConfigParser()
    # conf.read(conf_path.format("fields.cfg"))
    conf.read("config\\fields.cfg")
    if conf.has_option("fields", key):
        return conf.get("fields", key)
    else:
        return ""

# ========================================================================================

#Recursive function to process the input dictionary
def extract(DictIn, Dictout, allkeys, longest, nestedKey=""):

    # Explicitly adding keys to Dictout
    if nestedKey == "object":
        try:
            Dictout["objlocdname"] = DictIn["location"]["displayName"]
        except KeyError:
            pass
    if nestedKey == "twitter_entities_user_mentions":
        Dictout["entitiesusrmentions"] = []
        mentionSet = set()
        inObj = {}
        for i in DictIn:
            inObj['is'] = i['id_str']
            inObj['n'] = i['name']
            inObj['sn'] = i['screen_name']
            Dictout["entitiesusrmentions"].append(inObj)
            inObj = {}
        longest['lu'] = len(DictIn) if len(DictIn) > longest['lu'] else longest['lu']
    elif nestedKey == "twitter_entities_hashtags":
        longestHtag = 0
        for i in DictIn:
            try:
                Dictout['entitieshtagstext'].append(i['text'])
                longestHtag += 1
            except KeyError:
                break
        longest['lh'] = longestHtag if longestHtag > longest['lh'] else longest['lh']

    elif nestedKey == "generator":
        Dictout["generatordname"] = DictIn["displayName"]
    elif nestedKey == "geo":
        Dictout["geocoordinates"] = DictIn['coordinates']
        Dictout["geotype"] = DictIn["type"]
    elif nestedKey == "location":
        try:
            Dictout["locdname"] = DictIn["displayName"]
            Dictout["locname"] = DictIn["name"]
            Dictout["loccountrycode"] = DictIn["country_code"]
            if DictIn["geo"] is not None:
                Dictout["locgeocoordinates"] = DictIn["geo"]["coordinates"]
        except KeyError, TypeError:
            pass
    elif nestedKey == "gnip":
        try:
            Dictout['languagevalue'] = DictIn['language']['value']
            Dictout['kloutscore'] = DictIn['klout_score']
        except KeyError:
            pass
        longestTag = 0
        longestRule = 0
        Tags = []
        Rules = []
        for i in DictIn['matching_rules']:
            if i['tag'] is not None:
                longestTag += 1
                Tags.append(i['tag'])
            longestRule += 1
            Rules.append(i['value'])
        Dictout['matchingrulestag'] = Tags
        Dictout['matchingrulesvalue'] = Rules
        longest['lr'] = longestRule if longestRule > longest['lr'] else longest['lr']
        longest['lt'] = longestTag if longestTag > longest['lt'] else longest['lt']
    #If DictIn is a dictionary
    elif isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extract(value, Dictout, allkeys, longest, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extract(value, Dictout, allkeys, longest, nestedKey=mykey)
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
                    if not newKey in allkeys:
                        allkeys.append(newKey)
                    if not newKey in Dictout:
                        Dictout[newKey] = ""

    #If DictIn is a list, call extract on each member of the list
    elif isinstance(DictIn, list):
        for value in DictIn:
            extract(value, Dictout, allkeys, longest, nestedKey=nestedKey)
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
                if not newKey in allkeys:
                    allkeys.append(newKey)
                if not newKey in Dictout:
                    Dictout[newKey] = ""

# ========================================================================================

monthToNames = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

if __name__ == "__main__":
    choice = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    current_month = sys.argv[2]
    current_year = sys.argv[3]
    proj_name = sys.argv[4]
    # Format the input month and year to form a a part of the Mongo Collection name
    # Get the path for the logs output
    logs = conf.get("conf", "prod_log_path")
    logging.basicConfig(filename=logs.format('(prodUpload' + current_month + current_year + proj_name +'.log'), level=logging.DEBUG)
    # Get the path for the source Raw_data json files
    src_path = conf.get("twitter", "prod_spl_src_path").format(current_year + monthToNames[current_month], proj_name)
    dest_path = conf.get("twitter", "prod_spl_dest_path").format(current_year + monthToNames[current_month], proj_name)
    # Get the list of files in the source directory
    fileList = os.listdir(src_path)
    delim = ','

    if choice == "byday":
        # Iterate over every file in the source directory
        for j in fileList:
            # If it's a by-day file in the source directory it will have 3 parts around the '-'s
            if len(j.split('_')) == 3:
                # Extract the date of the corresponding file from it's name
                logging.info("Started uploading " + j)
                CSVfromTwitterJSON(src_path + j, dest_path)
    elif choice == "full":
        tweetList = []
        longestRule = 0
        longestHtag = 0
        longestRTag = 0
        longestUMen = 0
        flag = True
        for j in fileList:
            # If it's a by-day file in the source directory it will have 3 parts around the '-'s
            tempResultList = None
            if len(j.split('_')) == 3:
                # Extract the date of the corresponding file from it's name
                logging.info("Started uploading " + j)
                tempResultList = CSVfromTwitterJSON(src_path + j, dest_path, 1)
                try:
					tweetList.append(tempResultList[0])
					if flag:
						longestRule = tempResultList[1]
						longestHtag = tempResultList[2]
						longestRTag = tempResultList[3]
						longestUMen = tempResultList[4]
						flag = False
					else:
						longestRule = tempResultList[1] if tempResultList[1] > longestRule else longestRule
						longestHtag = tempResultList[2] if tempResultList[2] > longestHtag else longestHtag
						longestRTag = tempResultList[3] if tempResultList[3] > longestRTag else longestRTag
						longestUMen = tempResultList[4] if tempResultList[4] > longestUMen else longestUMen
				except TypeError:
					continue
        csvfile = open(dest_path + current_year + monthToNames[current_month] + proj_name + '.csv', 'wb')
        fields = ConfigParser.ConfigParser()
        fields.read(conf_path.format("fields.cfg"))
        outputSet = (0, longestRule, longestHtag, longestRTag, longestUMen)
        writer = DME.printHead(csvfile, outputSet, delim, fields)
        keyList = [val for (key, val) in fields.items('fields')]
        resultList = (tweetList, longestRule, longestHtag, longestRTag, longestUMen)
        printCSV(csvfile, resultList, writer[0], keyList, delim)
        csvfile.close()
    elif choice == "dev":
        
        outputSet = CSVfromTwitterJSON('temp.json', 1)
        csvfile = open('temp.csv', 'wb')
        fields = ConfigParser.ConfigParser()
        fields.read(conf_path.format("fields.cfg"))

        writer = DME.printHead(csvfile, outputSet, delim, fields)
        keyList = [val for (key, val) in fields.items('fields')]
        # resultList = (tweetList, longestRule, longestHtag, longestRTag, longestUMen)
        printCSV(csvfile, outputSet, writer[0], keyList, delim)
        csvfile.close()