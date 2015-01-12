import sys
import os
import xml.etree.ElementTree as ET
import string
import datetime

def parseXMLbackfillToCSVdir(searchpath):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(searchpath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    for f in myfiles:
        if ".xml" in f and ".csv" not in f:
            print f
            parseYTbackfillToCSV(f)

def parseXMLbackfillToCSV(myfile):
    ytfile = open(myfile,"r")
    myline = ytfile.read()
    count = myline.count("<entry")
    mylines = myline.split("\\n")
    if len(mylines) > 1:
        myline = " ".join(mylines)
    mylines = myline.split("\\r")
    if len(mylines) > 1:
        myline = " ".join(mylines)
    mylines = myline.split("\\\\")
    if len(mylines) > 1:
        myline = " ".join(mylines)
    mylines = myline.split("\\ ")
    if len(mylines) > 1:
        myline = " ".join(mylines)
    print count
    ytfile.close()
    current = []
    allkeys = []
    entries = []
    try:
        eTree = ET.fromstring(myline)
    except ET.ParseError as e:
        print myline
        print e
    else:
        for k in range(count):
            a = {}
            extractTree(eTree[k],a,allkeys)
            entries.append(a)
            #print a
            
    output = open(myfile+".csv","w")
    printCSV(output,entries,allkeys)
    output.close()

def parseXMLstream(searchpath,keyword):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(searchpath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)

    isEntry = False
    current = []
    allkeys = []
    entries = []

    for filename in myfiles:
        if keyword in filename and ("xml" in filename) and ("error" not in filename) and ".csv" not in filename:
            print filename
            f = open(filename, "r")
            for line in f:
                #myline = string.lstrip(line)
                temp = line.splitlines()
                myline = "".join(temp)                
                myline = myline.decode("utf-8", "ignore")
                if "</entry>" in myline:
                    if not myline.endswith("</entry>"):
                        mylines = myline.split("</entry>")
                        current.append(mylines[0]+"</entry>")
                        mystring = "".join(current)
                        current = []
                        if "<entry" in myline:
                            mylines = myline.split("<entry")
                            myline = "<entry "+mylines[1]
                            current.append(myline)
                            isEntry = True
                    else:
                        current.append(myline)
                        mystring = "".join(current)
                        isEntry = False
                        current = []
                    try:
                        eTree = ET.fromstring(mystring.encode('ascii','ignore'))
                    except ET.ParseError as e:
                        print mystring
                        print e
                        print ""
                    else:
                        a = {}
                        extractTree(eTree,a,allkeys)
                        entries.append(a)
                elif "<entry" in myline:
                    isEntry = True
                    if current != []:
                        #print "Discarding: "+"\n".join(current)+"\n"
                        current = []
                    mylines = myline.split("<entry")
                        #print mylines
                    myline = "<entry "+mylines[1]
                    current.append(myline)
                    #print current
                elif isEntry:
                    current.append(myline)
                
            f.close()

##    if scratch:
##        scratchfile = open(scratchdir+"youtube3_0scratch.xml","w")
##        for line in current:
##            scratchfile.write(line+"\n")
##        scratchfile.close()
    output = open(searchpath+keyword+".csv","w")
    printCSV(output,entries,allkeys)
    output.close()

#Recursive function to process the input tree
def extractTree(eTree, Dictout, allkeys, nestedKey=""):
    for child in eTree:
        tag = child.tag.strip()
        tags = tag.split("}")
        if len(tags) > 1:
            tag = tags[len(tags)-1]
        if nestedKey != "":
            mykey = nestedKey+"_"+tag
        else:
            mykey = tag
        if child.text != None:
            text = child.text.strip()
            if text != "":
                if not mykey in allkeys:
                    allkeys.append(mykey)
                if not mykey in Dictout:
                    Dictout[mykey] = text
                else:
                    Dictout[mykey] = unicode(Dictout[mykey])+"; "+unicode(text)
        if child.attrib != {}:
            extract(child.attrib,Dictout,allkeys,nestedKey=mykey)
        extractTree(child,Dictout,allkeys,nestedKey=mykey)

def extract(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.items():
            #If nested, prepend the previous variables
            if nestedKey != "":
                mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extract(value, Dictout, allkeys, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extract(value, Dictout, allkeys, nestedKey=mykey)
            else: #Value is just a string
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



def printCSV(csvfile,resultList,mykeys):
    delim = ","
    print len(resultList)

    for item in mykeys:
        csvfile.write(item+delim)

    #For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList:
        csvfile.write("\n")
        for item in mykeys:
            if item in result:
                entry = result[item]
                if type(entry) == unicode or type(entry) == str:
                    #entry = unicode(entry, "utf-8", errors="ignore")
                    entrys = entry.split(",")
                    if len(entrys) > 1:
                        entry = "".join(entrys)                     
                else:
                    entry = unicode(entry)
                #Override to avoid errors for weird characters
                temp = entry.splitlines()
                entry = "".join(temp)
                csvfile.write(entry.encode('ascii','ignore')+delim)
            else:
                csvfile.write(delim)
