# Switched to XML on Feb 6th 2015



import os
import string
import dm_rules
import json
import sys
import re
from bs4 import BeautifulSoup
import ConfigParser
import csv
import pandas as pd
from CSVUnicodeWriter import CSVUnicodeWriter
from utility import mkdir_p

def getDatafromXMLfbStreams(jsonfile, fields):
    data = []
    line = ""
    jsonFile = open(jsonfile)
    for i in jsonFile.readlines():
        if '</entry>' in i:
            line += i
            dataLine = extractXML(line, fields)
            if dataLine is not None:
                data.append(dataLine)
            line = ""
        else:
            line += i
    jsonFile.close()
    return data


def extractXML(line, fields):
    soup = BeautifulSoup(line)
    data = {}
    for (key, val) in fields.iteritems():
        part_xml = soup.find(key)
        if isinstance(val, list):
            for j in val:
                try:
                    item = part_xml.find(j).get_text().strip()
                    if item == "":
                        try:
                            item = part_xml.find(j)['href']
                        except KeyError:
                            return None
                    newKey = key + j
                except AttributeError:
                    return None
                item = item.replace('\n', '').replace('\r', '')
                newKey = newKey.replace(':', '').replace('-', '')
                data[newKey] = item
        else:
            try:
                item = part_xml.get_text().strip()
                if item == "":
                    try:
                        item = part_xml['href']
                    except KeyError:
                        return None
                newKey = key
            except AttributeError:
                return None
            item = item.replace('\n', '').replace('\r', '')
            newKey = newKey.replace(':', '').replace('-', '')
            data[newKey] = item
    return data


#Creates the CSV
#makeCSVfromJSONfbStreams("h:/data/rawdata/gnip/facebook/2014/08/18/")
def makeCSVfromJSONfbStreams(jsonfile, op):
    currentInfo = ""
    currentComments = ""
    with open(conf_path.format("fb_comments_fields_json.json")) as fieldsFile:
        fields = json.loads(fieldsFile.read())

    keysInfo = []
    keysComments = []
    outInfo = []
    outComments = []
    myline = ""
    f = open(jsonfile, "r")
    for line in f:
        myline = string.strip(line)
        if myline != "":
            myline = myline.decode("utf-8", "ignore")
            #print myline
            #Remove new lines from within
            mylines = myline.split("\\n")
            if len(mylines) > 1:
                myline = " ".join(mylines)
            #Remove carriage returns from within
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
            if "info" in f.name:
                parseString(myline, outInfo, currentInfo, keysInfo, op)
            elif "comments" in f.name:
                parseString(myline, outComments, currentComments, keysComments, op, fields)
    f.close()
    if "info" in f.name:
        returnData = (keysInfo, outInfo)
    elif "comments" in f.name:
        keysComments.append('type')
        returnData = outComments
    return returnData

def parseString(myline, outList, current, outKeys, op, fields=[]):
    if current != "":
        myline = "".join([current,myline])
    try:
        elt = json.loads(myline)
    except ValueError as e:
        current = myline
    else:
        current = ""
        dataDict = {}
        msgTagLen = None
        if op == "info":
            extractInfo(elt, dataDict, outKeys)
        elif op == "comments":
            dataType = elt.keys()[0]
            extractComments(elt[dataType], dataDict, outKeys, fields, "")
            dataDict['type'] = dataType
            dataDict['id1'], dataDict['id2'] = dataDict['id'].split('_')
            if dataDict.get('messagetags') is not None:
                counter = 1
                for i in dataDict['messagetags']:
                    dataDict['messagetagsid' + str(counter)] = i['messagetagsid']
                    dataDict['messagetagsname' + str(counter)] = i['messagetagsname']
                    counter += 1
                del(dataDict['messagetags'])

        outList.append(dataDict)
        

def replaceKey(mykey, fields):
    if fields.get(mykey) is not None:
        return fields[mykey]
    else:
        return ""

def extractComments(DictIn, Dictout, keys, fields, nestedKey=""):
    # If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                mykey = nestedKey+"_"+key
            else:
                mykey = key
            # If value is a dictionary or a list
            if isinstance(value, dict) or isinstance(value, list):
                extractComments(value, Dictout, keys, fields, nestedKey=mykey)
            else: #Value is just a string
                newKey = replaceKey(mykey, fields)
                if newKey == "":
                    continue
                if isinstance(value, str) or isinstance(value, unicode):
                    value = value.strip()
                if value != "":
                    #If this is a new variable, add it to the list
                    if not newKey in keys:
                        keys.append(newKey)
                    #Add it to the output dictionary
                    if not newKey in Dictout:
                        Dictout[newKey] = value
                    else:
                        if isinstance(Dictout[newKey], str) or isinstance(Dictout[newKey], unicode):
                            Dictout[newKey] = [unicode(Dictout[newKey]), unicode(value)]
                        elif isinstance(Dictout[newKey], list):
                            Dictout[newKey].append(unicode(value))
                else:
                    if not newKey in keys:
                        keys.append(newKey)
                    if not newKey in Dictout:
                        Dictout[newKey] = ""

    #If DictIn is a list, call extractComments on each member of the list
    elif isinstance(DictIn, list):
        newKey = replaceKey(nestedKey, fields)
        if newKey != "":
            if not newKey in keys:
                keys.append(newKey)
            #Add it to the output dictionary
            if not newKey in Dictout:
                Dictout[newKey] = []
                for item in DictIn:
                    if isinstance(item, dict):
                        inObj = {}
                        for (k, v) in item.iteritems():
                            innerKey = replaceKey(nestedKey + '_' + k, fields)
                            if innerKey != "":
                                inObj[innerKey] = v
                        Dictout[newKey].append(inObj)
                    else:
                        Dictout[newKey] = item
        else:
            for value in DictIn:
                extractComments(value, Dictout, keys, fields, nestedKey=nestedKey)


#Recursive function to process the input dictionary
def extractInfo(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                if "tags" in nestedKey and isinstance(value,list):
                    mykey = nestedKey
                else:
                    mykey = nestedKey + '_' + key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extractInfo(value, Dictout, allkeys, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extractInfo(value, Dictout, allkeys, nestedKey=mykey)
            elif value != None and value != "": #Value is just a string
                #If this is a new variable, add it to the list
                if not mykey in allkeys:
                    allkeys.append(mykey)
                #Add it to the output dictionary
                if not mykey in Dictout:
                    Dictout[mykey] = value
                else:
                    Dictout[mykey] = unicode(Dictout[mykey])+"; "+unicode(value)
    #If DictIn is a list, call extractInfo on each member of the list
    elif isinstance(DictIn, list):
        for value in DictIn:
            extractInfo(value,Dictout,allkeys,nestedKey=nestedKey)
    #If DictIn is a string, check if it is a new variable and then add to dictionary
    else:
        if not nestedKey in allkeys:
            allkeys.append(nestedKey)
        if not nestedKey in Dictout:
            Dictout[nestedKey] = DictIn
        else:
            Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)




def printCSVInfo(csvfile, resultList, mykeys):
    delim = ','
    print len(resultList)
    
    for key in mykeys:
        with open(conf_path.format('facebook_fanpages.txt')) as f:
            fanpages = [i.strip() for i in f.readlines()]
        csvfile.write(key + delim)

    for result in resultList:
        csvfile.write("\n")
        for item in mykeys:
            if item in result:
                entry = result[item]
                if type(entry) == unicode:
                    #entry = unicode(entry, "utf-8", errors="ignore")
                    entrys = entry.split(",")
                    if len(entrys) > 1:
                        entry = "".join(entrys)
                else:
                    entry = unicode(entry)
                #Override to avoid errors for weird characters
                csvfile.write(entry.encode('ascii','ignore')+delim)
            else:
                csvfile.write(delim)





def aggregateByDay(year, conf):
    src_parts_path = conf.get("facebook", "prod_src_parts_path")
    src_path = conf.get("facebook", "prod_src_path")
    for i in range(7, 13):
        parts_src = src_parts_path.format(year, str(i).zfill(2))
        src = src_path.format(year, str(i).zfill(2))
        fileList = os.listdir(parts_src)
        curr_file = None
        fileList = sorted(fileList)
        for j in fileList:
            fileName = '_'.join(j.split('-')[:-2])
            if curr_file is None:
                curr_file = open(src + fileName + ".json", 'w')
            currFileName = curr_file.name.split('.')[0].split('\\')[-1]
            print fileName
            if currFileName == fileName:
                partFile = open(parts_src + j, 'r')
                for j in partFile.readlines():
                    curr_file.write(j)
                partFile.close()
            else:
                curr_file.close()
                curr_file = None


def getFiles(src, infoOrComments):
    fileList = os.listdir(src)
    outputFiles = []
    for i in fileList:
        if '.xml' in i or '.json' in i:
                if infoOrComments in i:
                    if len(i.split('_')) == 5 or len(i.split('-')) == 3:
                        if '_e.' not in i:
                            outputFiles.append(i)
                            continue
    return outputFiles


def processBackfill(backfillFile):
    fileType, _,s_date, e_date = backfillFile.split('.')[0].split('_')[1:]

    f = open(backfillFile)
    line = ""
    data = []
    rawData = {}
    processedData = {}

# python DM_facebook_k.py <info|comments> 2015 02

if __name__ == "__main__":
    op = sys.argv[1].lower()
    year = sys.argv[2]
    month = sys.argv[3]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    # aggregateByDay(year, conf)
    src = conf.get("facebook", "prod_src_path").format(year, str(month).zfill(2))
    dest = conf.get("facebook", "prod_dest_path").format(year, str(month).zfill(2))
    mkdir_p(dest)
    if op == "info":
        infoFileList = getFiles(src, 'info')
        for i in infoFileList:
            outputInfo = open(dest + i.split('\\')[-1].split('.')[0] + ".csv", "wb")
            outInfo = makeCSVfromJSONfbStreams(src + i, op)
            printCSVInfo(outputInfo, outInfo[1], outInfo[0])
            outputInfo.close()
    elif op == "comments":
        commentsFileList = getFiles(src, "comments")
        with open(conf_path.format("fb_comments_fields_xml.json")) as f:
            fields = json.loads(f.read())
        with open(conf_path.format('facebook_fanpages.txt')) as f:
            fanpages = [i.strip() for i in f.readlines()]
        comment_list_file = open(dest + 'fanpage_to_user_id.csv', 'wb')
        comment_list_file.write('Fanpage, Comment_Id\n')

        for i in commentsFileList:
            print i
            outputComments = open(dest + i.split('\\')[-1].split('.')[0] + '_comments.csv', "wb")
            if 'xml' in i:
                commentsData = getDatafromXMLfbStreams(src + i, fields)
            elif 'json' in i:
                commentsData = makeCSVfromJSONfbStreams(src + i, op)
            df = pd.DataFrame(commentsData)
            df.to_csv(outputComments, sep=',', index=False)
            outputComments.close()
        comment_list_file.close()
    # elif op == "backfill":
    #     backfillSrc = conf.get("facebook", "prod_backfill_path")
    #     fileList = os.listdir(backfillSrc)