import os
import string
import dm_rules
import json
import sys
import ConfigParser
from CSVUnicodeWriter import CSVUnicodeWriter
from utility import mkdir_p

def makeCSVfromXMLfbStreams(jsonfile, dest):
    currentComments = ""
    with open(conf_path.format("fb_comments_fields.json")) as f:
        fields = json.loads(f.read())

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
        if op == "info":
            outputInfo = open(dest + jsonfile.split('\\')[-1].split('.')[0] + "info.csv", "wb")
            printCSVInfo(outputInfo, outInfo, keysInfo)
            outputInfo.close()
        elif op == "comments":
            outputComments = open(dest + jsonfile.split('\\')[-1].split('.')[0] + "comments.csv", "wb")
            keysComments.append('type')
            printCSVComments(outputComments, outComments, keysComments, fields)
            outputComments.close()


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
    map_writer = CSVUnicodeWriter(f)
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
    for i in fileList:
        if 'xml' not in i and 'json' not in i and infoOrComments not in i and 'error' in i:
            fileList.remove(i)
            continue
        if i.split('_') != 3:
            if i.split('-') != 3:
                fileList.remove(i)

    return fileList


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
    infoFileList = getFiles(src, 'info')
    commentsFileList = getFiles(src, 'comments')
    mkdir_p(dest)
    if op == "info":
        for i in infoFileList:
            makeCSVfromJSONfbStreams(i, op, dest)
    elif op == "comments":
        f = open(conf_path.format('facebook_fanpages.txt'))
        f = open(dest + 'fanpage_to_user_id.csv', 'wb')
        for i in commentsFileList:
            fanpages = [i.strip() for i in f.readlines()]
            f.close()
            f.write('Fanpage, Comment_Id\n')
            if 'xml' in i:
                makeCSVfromXMLfbStreams(i, dest)
            elif 'json' in i:
                makeCSVfromJSONfbStreams(i, op, dest)
        f.close()