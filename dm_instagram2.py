import os
import dm_xml
import string
import urllib
import urllib2
import xml.etree.ElementTree as ET

#To process backfill instagram files
#dm_xml.parseXMLbackfilltoCSVdir("h:\\Data\\RawData\\GNIP\\Instagram\\Backfill")
#Will create a .csv file for each .xml file

#To process instagram streaming files
#dm_xml.parseXMLstream("h:\\Data\\RawData\\GNIP\\Instagram\\2014\\","instagram")
#Will create an instagram.csv file in the specified directory with results from all subfolders and files

#To download instagram pictures if they do not already exist in pictures folder
def getPictures(csvFile, picturedir="h:/data/rawdata/gnip/instagram/pictures/"):
    pictureList = open(csvFile, "r")
    header = pictureList.readline()
    headers = string.split(header, ",")
    hrefs = []
    index = 0
    for name in headers:
        if "object_link_href" in name:
            hrefs.append(index)
        index += 1
    for item in pictureList:
        item = item.strip()
        columns = string.split(item, ",")
        links = columns[hrefs[0]]
        if links == "" and len(hrefs) > 1 and len(columns) > hrefs[1]:
            links = columns[hrefs[1]]
        splitItem = string.split(links, ";")
        try:
            myids = string.split(splitItem[0],"/")
            myid = myids[len(myids)-2]
            myurl = splitItem[1]
        except IndexError as e:
            print e
            print item
        else:
            myname = picturedir+myid+".jpg"
            if not os.path.isfile(myname):
                try:
                    response = urllib.urlretrieve(myurl, myname)
                except urllib2.HTTPError as e:
                    print e.read()
    pictureList.close()

#To download instagram pictures if they do not already exist in pictures folder
def getVideos(csvFile, picturedir="h:/data/rawdata/gnip/instagram/pictures/"):
    pictureList = open(csvFile, "r")
    header = pictureList.readline()
    headers = string.split(header, ",")
    hrefs = []
    index = 0
    for name in headers:
        if "object_link_href" in name:
            hrefs.append(index)
        index += 1
    for item in pictureList:
        if "VideoPosted" in item:
            item = item.strip()
            columns = string.split(item, ",")
            links = columns[hrefs[0]]
            if links == "" and len(hrefs) > 1 and len(columns) > hrefs[1]:
                links = columns[hrefs[1]]
            splitItem = string.split(links, ";")
            try:
                myids = string.split(splitItem[0],"/")
                myid = myids[len(myids)-2]
                #print myid
                myurl = splitItem[2]
            except IndexError as e:
                print e
                print item
            else:
                myname = picturedir+myid+".mp4"
                if not os.path.isfile(myname):
                    try:
                        response = urllib.urlretrieve(myurl, myname)
                    except urllib2.HTTPError as e:
                        print e.read()
    pictureList.close()


#To download instagram pictures if they do not already exist in pictures folder
def getBigPictures(csvFile, picturedir="h:/data/rawdata/gnip/instagram/pictures/"):
    pictureList = open(csvFile, "r")
    header = pictureList.readline()
    headers = string.split(header, ",")
    hrefs = []
    index = 0
    for name in headers:
        if "object_link_href" in name:
            hrefs.append(index)
        index += 1
    for item in pictureList:
        if "VideoPosted" not in item:
            item = item.strip()
            columns = string.split(item, ",")
            links = columns[hrefs[0]]
            if links == "" and len(hrefs) > 1 and len(columns) > hrefs[1]:
                links = columns[hrefs[1]]
            splitItem = string.split(links, ";")
            try:
                myids = string.split(splitItem[0],"/")
                myid = myids[len(myids)-2]
                #print myid
                myurl = splitItem[2]
            except IndexError as e:
                print e
                print item
            else:
                myname = picturedir+myid+".jpg"
                if not os.path.isfile(myname):
                    try:
                        response = urllib.urlretrieve(myurl, myname)
                    except urllib2.HTTPError as e:
                        print e.read()
    pictureList.close()

def getPicturesDir(csvDir, picturedir="h:/data/rawdata/gnip/instagram/pictures/"):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(csvDir):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    for f in myfiles:
        if "instagram" in f and ".csv" in f:
            print f
            getPictures(f)

#getPicturesDir("h:/data/rawdata/gnip/instagram/backfill/","h:/data/rawdata/gnip/instagram/pictures/")
#getPictures("h:/data/rawdata/gnip/instagram/backfill/instagram_backfill_20140722070000_20140725070000.xml.csv","h:/data/rawdata/gnip/instagram/pictures/")
#getPictures("h:/data/rawdata/gnip/instagram/2014/07/instagram.csv","h:/data/rawdata/gnip/instagram/pictures/")

def hasSelfieSpecific(mystring):
    return("smokeselfie" in mystring or "smokerselfie" in mystring or "cigaretteselfie" in mystring or "cigselfie" in mystring or "smokefreeselfie" in mystring or "smokingselfie" in mystring or "tobaccoselfie" in mystring or "tobaccofreeselfie" in mystring)

def hasSelfieStuff(mystring):
    return("selfie" in mystring and ("smok" in mystring or "cig" in mystring or "tobacco" in mystring))

#parseXMLforKeyword("h:/data/rawdata/gnip/instagram/2014/07/25/","selfie")
def parseXMLforKeyword(searchpath,keyword):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(searchpath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)

    isEntry = False
    current = []
    allkeys = []
    entries = []

    outfile = open(searchpath+keyword+".xml","w")
    for filename in myfiles:
        if ("xml" in filename) and ("error" not in filename) and ".csv" not in filename and keyword not in filename:
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
                    if keyword in mystring:
                        outfile.write(mystring.encode('ascii','ignore')+"\n")
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

    outfile.close()

#parseXMLwithFunction("h:/data/rawdata/gnip/instagram/2014/07/25/","selfie",hasSelfieStuff)
def parseXMLwithFunction(searchpath,keyword,function):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(searchpath):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)

    isEntry = False
    current = []
    allkeys = []
    entries = []

    outfile = open(searchpath+keyword+".xml","w")
    for filename in myfiles:
        if ("xml" in filename) and ("error" not in filename) and ".csv" not in filename and keyword not in filename:
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
                    if function(mystring):
                        outfile.write(mystring.encode('ascii','ignore')+"\n")
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

    outfile.close()

def parseXMLfileToCSV(myfile):
    allkeys = []
    entries = []

    xmlfile = open(myfile,"r")
    for myline in xmlfile:
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
        try:
            eTree = ET.fromstring(myline)
        except ET.ParseError as e:
            print myline
            print e
        else:
            a = {}
            dm_xml.extractTree(eTree,a,allkeys)
            entries.append(a)

    output = open(myfile+".csv","w")
    dm_xml.printCSV(output,entries,allkeys)
    output.close()
