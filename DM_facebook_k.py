import os
import string
import dm_rules
import json
import sys
import re
from bs4 import BeautifulSoup
import ConfigParser
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
                    newKey = re.sub('[^a-zA-Z0-9]', '', newKey)
                    data[newKey] = item
                except AttributeError:
                    return None
        else:
            try:
                item = part_xml.get_text().strip()
                if item == "":
                    try:
                        item = part_xml['href']
                    except KeyError:
                        return None
                newKey = key
                newKey = re.sub('[^a-zA-Z0-9]', '', newKey)
                data[newKey] = item
            except AttributeError:
                data = None
                break
    return data


def printCSVXMLComments(csvfile, data, fields):
    print len(data)
    writer = CSVUnicodeWriter(csvfile)
    keys = []
    for (key, val) in fields.iteritems():
        if isinstance(val, list):
            for i in val:
                keys.append(key + '_' + i)
        elif isinstance(val, dict):
            for i in val.iteritems():
                keys.append(key + '_' + i)
        else:
            keys.append(key)
    writer.writerow(keys)

    for item in data:
        row = []
        for key in keys:
            if key in item:
                row.append(item[key])
            else:
                row.append("")
        writer.writerow(row)


#Creates the CSV
#makeCSVfromJSONfbStreams("h:/data/rawdata/gnip/facebook/2014/08/18/")
def makeCSVfromJSONfbStreams(jsonfile, op, dest=""):
    currentInfo = ""
    currentComments = ""
    fieldsFile = open(conf_path.format("fb_comments_fields.txt"))
    fields = [line.strip() for line in fieldsFile.readlines()]
    fieldsFile.close()

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
    if op == "info":
        outputInfo = open(dest + jsonfile.split('\\')[-1].split('.')[0] + "info.csv", "wb")
        printCSVInfo(outputInfo, outInfo, keysInfo)
        outputFile = outputInfo
        outputInfo.close()
    elif op == "comments":
        outputComments = open(dest + jsonfile.split('\\')[-1].split('.')[0] + "comments.csv", "wb")
        keysComments.append('type')
        printCSVComments(outputComments, outComments, keysComments, fields)
        outputFile = outputComments
        outputComments.close()
    return outputFile

#Recursive function to process the input dictionary
def extractComments(DictIn, Dictout, allkeys, fields, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                if "tags" in nestedKey and isinstance(value,list):
                    mykey = nestedKey
                else:
                    mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extractComments(value, Dictout, allkeys, fields, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extractComments(value, Dictout, allkeys, fields, nestedKey=mykey)
            elif value != None and value != "": #Value is just a string
                #If this is a new variable, add it to the list
                if mykey not in allkeys and mykey in fields:
                    allkeys.append(mykey)
                #Add it to the output dictionary
                if mykey in fields:
                    if not mykey in Dictout:
                        Dictout[mykey] = value
                    else:
                        Dictout[mykey] = unicode(Dictout[mykey]) + "; " + unicode(value)

    #If DictIn is a list, call extractComments on each member of the list
    elif isinstance(DictIn, list):
        for value in DictIn:
            extractComments(value, Dictout, allkeys, fields, nestedKey=nestedKey)
    #If DictIn is a string, check if it is a new variable and then add to dictionary
    else:
        if mykey in fields:
            if nestedKey not in allkeys and nestedKey in fields:
                allkeys.append(nestedKey)
            if not nestedKey in Dictout:
                Dictout[nestedKey] = DictIn
            else:
                Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)


#Recursive function to process the input dictionary
def extractInfo(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        #print DictIn
        #print len(DictIn.items())
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if nestedKey != "":
                if "tags" in nestedKey and isinstance(value,list):
                    mykey = nestedKey
                else:
                    mykey = nestedKey+"_"+key
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
        if op == "info":
            extractInfo(elt, dataDict, outKeys)
        elif op == "comments":
            dataType = elt.keys()[0]
            extractComments(elt[dataType], dataDict, outKeys, fields, "")
            dataDict['type'] = dataType
        outList.append(dataDict)


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


def printCSVComments(csvfile, resultList, mykeys, fields):
    delim = ','
    writer = CSVUnicodeWriter(csvfile)
    map_writer = CSVUnicodeWriter(comment_list_file)
    print len(resultList)
    for key in fields:
        csvfile.write(key + delim)

    csvfile.write('\n')
    for result in resultList:
        ids = result['id'].split('_')
        row = []
        map_row = []
        if result['from_id'] in fanpages:
            map_row.append(result['from_id'])
            map_row.append(result['id'])
            map_writer.writerow(map_row)
        for key in fields:
            if key in result:
                #Override to avoid errors for weird characters
                entry = unicode(result[key])
                row.append(entry)
            else:
                if key == "id1":
                    row.append(ids[0])
                elif key == "id2":
                    try:
                        row.append(ids[1])
                    except IndexError:
                        row.append("")
                elif key == "threadid":
                    row.append(ids[0])
                elif key == "like_count":
                    row.append('0')
                else:
                    row.append('')
        writer.writerow(row)




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
            fileName = '-'.join(j.split('-')[:-2])
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


# TO DO: write code to automate this for a bunch of months so that we can leave it running over the weekend

if __name__ == "__main__":
    op = sys.argv[1]
    month = sys.argv[2]
    year = sys.argv[3]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    # aggregateByDay(year, conf)
    src = conf.get("facebook", "prod_src_path").format(year, month)
    dest = conf.get("facebook", "prod_dest_path").format(year, month)
    mkdir_p(dest)
    if op == "info":
        infoFileList = getFiles(src, 'info')
        for i in infoFileList:
            makeCSVfromJSONfbStreams(src + i, op, dest)
    elif op == "comments":
        commentsFileList = getFiles(src, "comments")
        with open(conf_path.format("fb_comments_fields.json")) as f:
            fields = json.loads(f.read())
        with open(conf_path.format('facebook_fanpages.txt')) as f:
            fanpages = [i.strip() for i in f.readlines()]
        comment_list_file = open(dest + 'fanpage_to_user_id.csv', 'wb')
        comment_list_file.write('Fanpage, Comment_Id\n')
        for i in commentsFileList:
            print i
            if 'xml' in i:
                data = getDatafromXMLfbStreams(src + i, fields)
                outputComments = open(dest + i.split('\\')[-1].split('.')[0] + "comments.csv", "wb")
                printCSVXMLComments(outputComments, data, fields)
                outputComments.close()
            elif 'json' in i:
                makeCSVfromJSONfbStreams(src + i, op, dest)
        comment_list_file.close()