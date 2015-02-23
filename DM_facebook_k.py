import os
import string
import dm_rules
import json
import sys
import ConfigParser

#Creates the CSV
#makeCSVfromJSONfbStreams("h:/data/rawdata/gnip/facebook/2014/08/18/")
def makeCSVfromJSONfbStreams(jsondir, op, outfileprefix=""):
    if outfileprefix == "":
        outfileprefix = jsondir
    myfiles = []
    # for (dirpath, dirnames, filenames) in os.walk(jsondir):
    #     myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    fileList = os.listdir(jsondir)
    for i in fileList:
        if op in i:
            myfiles.append(jsondir + i)
    currentInfo = ""
    currentComments = ""
    fieldsFile = open("fb_comments_fields.txt")
    fields = [line.strip() for line in fieldsFile.readlines()]
    fieldsFile.close()
    for filename in myfiles:
        keysInfo = []
        keysComments = []
        outInfo = []
        outComments = []
        myline = ""
        if (".json" in filename) and (".csv" not in filename):
            #print filename
            f = open(filename, "r")
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
            outputInfo = open(outfileprefix + filename.split('\\')[-1].split('.')[0] + "info.csv", "w")
            printCSV(outputInfo, outInfo, keysInfo)
            outputInfo.close()
        elif op == "comments":
            outputComments = open(outfileprefix + filename.split('\\')[-1].split('.')[0] + "comments.csv", "w")
            keysComments.append('type')
            printCSV(outputComments, outComments, keysComments)
            outputComments.close()
            f.close()
    parseString("",outInfo, currentInfo, keysInfo)
    parseString("",outComments, currentComments, keysComments)



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




def printCSV(csvfile,resultList,mykeys):
    delim = ','
    print len(resultList)

    # createds = [s for s in mykeys if "created_time" in s]
    # updateds = [s for s in mykeys if "updated_time" in s]
    # csvfile.write("cdate"+delim)
    # csvfile.write("udate"+delim)

    for key in mykeys:
        csvfile.write(key + delim)

    for result in resultList:
        csvfile.write("\n")
        for key in mykeys:
            if key in result:
                entry = result[key]
                if type(entry) == unicode:
                    #entry = unicode(entry, "utf-8", errors="ignore")
                    entrys = entry.split(",")
                    if len(entrys) > 1:
                        entry = "".join(entrys)
                else:
                    entry = unicode(entry)
                #Override to avoid errors for weird characters
                csvfile.write(entry.encode('ascii','ignore') + delim)
            else:
                if key == "like_count":
                    like_count = 0
                    csvfile.write(str(like_count))
                csvfile.write(delim)
    
    #For each entry in the list, print the variables in the correct order (or "" if not present)
    # for result in resultList:
    #     csvfile.write("\n")
    #     mydate = ""
    #     for c in createds:
    #         if c in result:
    #             mydate = result[c]
    #     csvfile.write(mydate.encode('ascii','ignore')+delim)
    #     mydate = ""
    #     for c in updateds:
    #         if c in result:
    #             mydate = result[c]
    #     csvfile.write(mydate.encode('ascii','ignore')+delim)
    #     for item in mykeys:
    #         if item in result:
    #             entry = result[item]
    #             if type(entry) == unicode:
    #                 #entry = unicode(entry, "utf-8", errors="ignore")
    #                 entrys = entry.split(",")
    #                 if len(entrys) > 1:
    #                     entry = "".join(entrys)
    #             else:
    #                 entry = unicode(entry)
    #             #Override to avoid errors for weird characters
    #             csvfile.write(entry.encode('ascii','ignore')+delim)
    #         else:
    #             csvfile.write(delim)



#Recursive function to process the input dictionary
def extractComments(DictIn, Dictout, allkeys, fields, nestedKey=""):
    #If DictIn is a dictionary
    if nestedKey == "comments_data":
        for i in DictIn:
            print i.keys()
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


if __name__ == "__main__":
    op = sys.argv[1]
    month = sys.argv[2]
    year = sys.argv[3]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    # aggregateByDay(year, conf)
    src = conf.get("facebook", "prod_src_path").format(year, month)
    dest = conf.get("facebook", "prod_dest_path").format(year, month)
    if op == "info":
        makeCSVfromJSONfbStreams(src, op, dest)
    elif op == "comments":
        makeCSVfromJSONfbStreams(src, op, dest)
