import os
import sys
import ConfigParser
import xml.etree.ElementTree as ET
import datetime


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

#    if scratch:
#        scratchfile = open(scratchdir+"youtube3_0scratch.xml","w")
#        for line in current:
#            scratchfile.write(line+"\n")
#        scratchfile.close()
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



def aggregateByDay(year, conf):
    src_parts_path = conf.get("youtube", "prod_src_parts_path")
    src_path = conf.get("youtube", "prod_src_path")
    for i in range(7, 13):
        for j in range(1, 32):
            parts_src = src_parts_path.format(year, str(i).zfill(2), str(j).zfill(2))
            src = src_path.format(year, str(i).zfill(2))
            try:
                fileList = os.listdir(parts_src)
            except IOError, WindowsError:
                continue
            curr_file = None
            fileList = sorted(fileList)
            for j in fileList:
                if "error" in j:
                    continue
                fileName = '-'.join(j.split('-')[:-2])
                if curr_file is None:
                    curr_file = open(src + fileName + ".xml", 'w')
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


def xmlParser(fileName):
    f = open(fileName)
    lines = f.readlines()
    xmlLine = ""
    for i in lines:
        if i.find("<entry>"):
            xmlLine = ""
        elif i.find("</entry>"):
            xmlLine += i
            try:
                eTree = ET.fromstring(xmlLine)
            except ET.ParseError:
                print "Invalid"
            else:
                continue
        xmlLine += i

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    aggregateByDay(year, conf)