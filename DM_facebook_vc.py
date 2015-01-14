import os
import string
import dm_rules
import json
import sys

#To process backfill data
#makeCSVfromJSONbackfillDir("h:\\Data\\RawData\\GNIP\\Facebook\\Backfill\\")

#get IDs from search files for fanpages
def getIDs(mypath):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    
    summary = open(mypath+"fb_fanpages_summary.txt","w")
    current = []
    temp=""

    for filename in myfiles:
        if (".json" in filename) and ("ids" not in filename) and ("fb_fanpages" in filename):
            print filename
            f = open(filename, "r")
            for line in f:
                if "page" in line or temp != "":
                    try:
                        myline0 = temp+line
                        myline = string.strip(myline0)
                        #myline.decode("utf-8", "ignore")
                        data = json.loads(myline)
                        x=data['page']['id']
                        current.append(x)
                        output = [data['page']['category'], data['page']['name'], x]
                        output2 = u"\t".join(output)+u"\n"
                        print output2
                        summary.write(output2)
                        temp = ""
                    except ValueError, e:
                        if "Unterminated" in e.message:
                            print line
                            temp = line
                        else:
                            print e
            f.close()

    summary.close()
    dm_rules.jsonCreateFromList(current,mypath+"ids.json")


def getIDsFromUrlSearch(mypath):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)   
    summary = open(mypath+"fb_fanpages_summary.txt","w")

    for filename in myfiles:
        if ("FacebookGraphKW" in filename):
            print filename
            f = open(filename, "r")
            f.readline()
            f.readline()
            myline = f.readline()
            line3 = string.strip(myline)
            keyword = line3.split(": ")[1]
#            print keyword
            f.readline()
            f.readline()
            mydata = f.read()
            try:
                data = json.loads(mydata)
                x=data['data']
                for entry in x:
                    category = entry['category'].encode('ascii','ignore')
                    name = entry['name'].encode('ascii','ignore')
                    myid = entry['id'].encode('ascii','ignore')
                    output = "\t".join([keyword, category, name, myid])
                    summary.write(output+"\n")
            except ValueError, e:
                    print e
            f.close()

    summary.close()





#Creates the CSV
#makeCSVfromJSONfbStreams("h:/data/rawdata/gnip/facebook/2014/08/18/")
def makeCSVfromJSONfbStreams(jsondir):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(jsondir):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    currentInfo = ""
    currentComments = ""
    keysInfo = []
    keysComments = []
    outInfo = []
    outComments = []

    for filename in myfiles:
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

                    if "info" in filename:
                        parseString(myline, outInfo, currentInfo, keysInfo)
                    elif "comments" in filename:
                        parseString(myline, outComments, currentComments, keysComments)
            f.close()
    parseString("",outInfo, currentInfo, keysInfo)
    parseString("",outComments, currentComments, keysComments)

    outputInfo = open(jsondir+"/info.csv","w")
    printCSV(outputInfo,outInfo,keysInfo)
    outputInfo.close()
    outputComments = open(jsondir+"/comments.csv","w")
    printCSV(outputComments,outComments,keysComments)
    outputComments.close()


def parseString(myline, outList, current, outKeys):
    if current != "":
        myline = "".join([current,myline])
    try:
        elt = json.loads(myline)
    except ValueError as e:
        current = myline
    else:
        current = ""
        a = {}
        extract(elt,a, outKeys)
        outList.append(a)


def makeCSVfromJSONbackfillDir(jsondir):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(jsondir):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    for f in myfiles:
        if ".json" in f and ".csv" not in f:
            makeCSVfromJSONbackfill(f)


def makeCSVfromJSONbackfill(jsonfilename):
    jsonfile = open(jsonfilename,"r")
    csvfile = open(jsonfilename+".csv","w")

    #Will track all variables seen across all results in the file
    mykeys = []
    #Will contain a dictionary for each processed result
    resultList = []

    line=jsonfile.read()
    myline = string.strip(line)
    if myline != "":
        #For each tweet in the file, decode the weird characters without complaining
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
        #Create a dictionary using the JSON processor
        try:
            results = json.loads(myline)
        except ValueError as e:
            print myline
            print e
        else:
            for elt in results['entry']:
                #Create an empty dictionary
                a = {}
                #Send the JSON dictionary, the empty dictionary, and the list of all keys
                extract(elt,a,mykeys)
                #Add the output dictionary to the list
                resultList.append(a)
    #Print the number of tweets processed
    printCSV(csvfile,resultList,mykeys)


def printCSV(csvfile,resultList,mykeys):
    delim = ','
    print len(resultList)

    for item in mykeys:
        csvfile.write(item+delim)

    #For each entry in the list, print the variables in the correct order (or "" if not present)
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

#Recursive function to process the input dictionary
def extract(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        #print DictIn
        #print len(DictIn.items())
        for key, value in DictIn.items():
            #If nested, prepend the previous variables
            if nestedKey != "":
                if "tags" in nestedKey and isinstance(value,list):
                    mykey = nestedKey
                else:
                    mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extract(value, Dictout, allkeys, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extract(value, Dictout, allkeys, nestedKey=mykey)
            elif value != None and value != "": #Value is just a string
                #If this is a new variable, add it to the list
                if not mykey in allkeys:
                    allkeys.append(mykey)
                #Add it to the output dictionary
                if not mykey in Dictout:
                    Dictout[mykey] = value
                else:
                    Dictout[mykey] = unicode(Dictout[mykey])+"; "+unicode(value)
    #If DictIn is a list, call extract on each member of the list
    elif isinstance(DictIn, list):
        for value in DictIn:
            extract(value,Dictout,allkeys,nestedKey=nestedKey)
    #If DictIn is a string, check if it is a new variable and then add to dictionary
    else:
        if not nestedKey in allkeys:
            allkeys.append(nestedKey)
        if not nestedKey in Dictout:
            Dictout[nestedKey] = DictIn
        else:
            Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)
