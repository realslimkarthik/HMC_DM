import string
import json
import sys
import os
import random
import re

# ========================================================================================
#convertTwitterDir("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\December-19-2013-N-gram\\",True)
#Will call makeCSVfromTwitterJSON on all files in directory (and subdirectories) named .json
def convertTwitterDir(jsondirname,overwrite=False,keyword=""):
    myfiles = []
    errorfile = open(jsondirname+"errors.json","a")
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsondirname):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and keyword in myfile:
            print myfile
            makeCSVfromTwitterJSON(myfile,errorfile,overwrite)
# ========================================================================================
#Will count the number of tweets in all files in directory (and subdirectories) named .json
def countTweets(jsondirname):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(jsondirname):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    total = 0
    for myfile in myfiles:
        if ".json" in myfile and not ".csv" in myfile:
            total+=count(myfile)
    print total

# ========================================================================================
#countkwords("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\September-2014-Master\\jsonminutes\\","H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\ANALYSIS_\\SeptemberCount.txt")
#for month in ["August", "September", "October"]:
#   countkwords("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\"+month+"-2014-Master\\jsonminutes\\","H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\ANALYSIS_\\"+month+"Count.txt")
def countkwords(jsondirname,countFile):
    myfiles = []
    for (dirpath, dirnames, filenames) in os.walk(jsondirname):
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    total = 0
    rulesList ={}
    for myfile in myfiles:
        if ".json" in myfile and not ".csv" in myfile:
            curFile = open(myfile, "r")
            for line in curFile:
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
                            print myline
                            print e
                    else:
                        if "gnip" in myline:
                            myrules = tweet['gnip']['matching_rules']
                            for myrule in myrules:
                                rule = myrule['value']
                                if rule in rulesList:
                                    rulesList[rule] = rulesList[rule] + 1
                                else:
                                    rulesList[rule] = 1
    myCountFile = open(countFile,"w")
    for rule,count in rulesList.items():
        print rule+"\t"+str(count)
        myCountFile.write(rule+"\t"+str(count)+"\n")
    myCountFile.close()
    
# ========================================================================================
#Finds the entry with the activity count and returns the number
def count(jsonfilename):
    jsonfile = open(jsonfilename,"r")
    for line in jsonfile:
        myline = string.strip(line)
        if "Replay Request Completed" in myline:
                tweet = json.loads(myline)
                return tweet['info']['activity_count']
# ========================================================================================
# this fixes every 10 min files into by day files. 
#getLegacy("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\")

def getLegacy(jsonDirectory):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("-")
            filename = "legacy-"+"-".join(name[3:len(name)-2])+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            for tweet in onefile:
                if "Legacy" in tweet:
                    jsonfile.write(tweet)
            onefile.close()
    jsonfile.close()
# ========================================================================================
#fixByDay("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\September-2014-Master\\")
def fixByDay(jsonDirectory):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("-")
            filename = "tw"+"-".join(name[3:len(name)-2])+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            jsonfile.write(onefile.read())
            onefile.close()
    jsonfile.close()
# ========================================================================================
#fixByMonth("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\September-2014-Master\\")
def fixByMonth(jsonDirectory):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("-")
            filename = "tw"+"-".join(name[3:len(name)-3])+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            jsonfile.write(onefile.read())
            onefile.close()
    jsonfile.close()
# ========================================================================================
#fixBy15Days("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\")
def fixBy15Days(jsonDirectory):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("-")
            half = int(name[len(name)-2])/16
            filename = "tw"+"-".join(name[3:len(name)-3])+"-"+str(half)+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            jsonfile.write(onefile.read())
            onefile.close()
    jsonfile.close()
# ========================================================================================
def fixByXDays(jsonDirectory,x=15):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("-")
            frac = int(name[len(name)-2])/(x+1)
            filename = "tw"+"-".join(name[3:len(name)-3])+"-"+str(frac)+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            jsonfile.write(onefile.read())
            onefile.close()
    jsonfile.close()
# ========================================================================================
#Assumes files have been saved as legacy-2014-08-**.json
def LegacyAugCombine():
    set1 = range(1,16)
    set2 = range(16,32)
    name1 = "0115"
    name2 = "1631"
    for day in set1:
        num = str(day)
        if day < 10: num = "0"+num
        infile = open("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy-2014-08-"+num+".json", "r")
        outfile = open("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy-2014-08-"+name1+".json", "a")
        outfile.write(infile.read())
        infile.close()
        outfile.close()

    for day in set2:
        num = str(day)
        infile = open("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy-2014-08-"+num+".json", "r")
        outfile = open("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy-2014-08-"+name2+".json", "a")
        outfile.write(infile.read())
        infile.close()
        outfile.close()

#printJSONfraction(1400, "H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\jsonminutes")

def printJSONfraction(range, jsondirname, overwrite=False, keyword=""):
    myfiles = []
    myfilename = ""
    jsonfile = open(jsondirname+"UPennAugustTweets.json", "w")
    random.seed(2014)
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsondirname):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            for tweet in onefile:
                num = random.randint(1, range)
                if(num == range/2):
                    jsonfile.write(tweet)
            onefile.close()
    jsonfile.close()

#Creates the CSV
#makeCSVfromTwitterJSON("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy.json")
def makeCSVfromTwitterJSON(jsonfilename, errorfile=None, overwrite=False):
    if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
        jsonfile = open(jsonfilename,"r")
        csvfile = open(jsonfilename.split('.')[0] +"_dropped.csv","w")
        print jsonfilename.split('.')[0] + ".csv"
        #Will track all variables seen across all tweets in the file
        mykeys = []
        #Will contain a dictionary for each processed tweet
        tweetList = []
        
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
                        print myline
                        print e
                else:
                    #Find the summary count
                    if "Replay Request Completed" in myline:
                        print tweet['info']['activity_count']
                    else:
                        #Create an empty dictionary
                        a = {}
                        #Send the JSON dictionary, the empty dictionary, and the list of all keys
                        extract(tweet, a, mykeys)
                        #Add the output dictionary to the list
                        tweetList.append(a)
        #Print the number of tweets processed
        jsonfile.close()
        printCSV(csvfile,tweetList,mykeys)
        print "Finished... ", csvfile

# Populates the CSV. Gets CSV's file handle from caller
def CSVfromTwitterJSON(jsonfilename, csvfile, errorfile=None, overwrite=False):
    if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
        jsonfile = open(jsonfilename,"r")
        #Will track all variables seen across all tweets in the file
        mykeys = []
        #Will contain a dictionary for each processed tweet
        tweetList = []
        
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
                        print myline
                        print e
                else:
                    #Find the summary count
                    if "Replay Request Completed" in myline:
                        print tweet['info']['activity_count']
                    else:
                        #Create an empty dictionary
                        a = {}
                        #Send the JSON dictionary, the empty dictionary, and the list of all keys
                        extract(tweet, a, mykeys)
                        #Add the output dictionary to the list
                        tweetList.append(a)
        #Print the number of tweets processed
        jsonfile.close()
        printCSV(csvfile,tweetList,mykeys)
        print "Finished... ", csvfile

# ========================================================================================
#Function that finds out if a key needs to be kept or thrown away
def keyInBlacklist(key, nested):
    for i in blacklist:
        if nested == False:
            outerMatch = re.search('^id$', key)
            if outerMatch != None:
                continue
        match = re.search(i + '$', key)
        if match != None:
            return True
    return False

# ========================================================================================
#Function that finds out if a key needs to be kept or thrown away
def keyNotInBlacklist(key, nested):
    # print key
    for i in blacklist:
        # if nested == False:
        #     outerMatch = re.search('^id$', key)
        #     if outerMatch != None:
        #         return False
        match = re.search(i, key)
        if match != None:
            print match.group(0)
            print i
            return False
    return True

# ========================================================================================
#Recursive function to process the input dictionary
def extract(DictIn, Dictout, allkeys, nestedKey=""):
    #If DictIn is a dictionary
    if isinstance(DictIn, dict):
        #Process each entry
        for key, value in DictIn.iteritems():
            #If nested, prepend the previous variables
            if keyInBlacklist(key, True if nestedKey != "" else False):
            # if keyNotInBlacklist(key, True if nestedKey != "" else False):
                continue
            if nestedKey != "":
                mykey = nestedKey+"_"+key
            else:
                mykey = key
            if isinstance(value, dict): # If value itself is dictionary
                extract(value, Dictout, allkeys, nestedKey=mykey)
            elif isinstance(value, list): # If value itself is list
                extract(value, Dictout, allkeys, nestedKey=mykey)
            else: #Value is just a string
                if isinstance(value, unicode) or isinstance(value, str):
                    value = value.strip()
                if value != "":
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
        if isinstance(DictIn, unicode) or isinstance(DictIn, str):
            DictIn = DictIn.strip()
        if DictIn != "":
            if not nestedKey in allkeys:
                allkeys.append(nestedKey)
            if not nestedKey in Dictout:
                Dictout[nestedKey] = DictIn
            else:
                Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)
# ========================================================================================

def printCSV(csvfile,resultList,mykeys):
    delim = ","
    print "Number of tweets processed: ", len(resultList)
    #csvfile.write("\"ID\""+delim)

    for item in mykeys:
        csvfile.write(item+delim)

    #For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList:
        csvfile.write("\n")
        myid = result['id']
        myids = myid.split(":")
        csvfile.write("\"tw"+myids[2]+"\""+delim)
        for item in mykeys:
            if item in result:
                entry = result[item]
                if type(entry) == unicode or type(entry) == str:
                    #entry = unicode(entry, "utf-8", errors="ignore")
                    entry=entry.strip()
                    entrys = entry.split(",")
                    if len(entrys) > 1:
                        entry = "".join(entrys)
                    entrys = entry.split("\"")
                    if len(entrys) > 1:
                        entry = "-".join(entrys)
                else:
                    entry = unicode(entry)
                #Override to avoid errors for weird characters
                temp = entry.splitlines()
                entry = "".join(temp)
                csvfile.write(entry.encode('ascii','ignore')+delim)
            else:
                csvfile.write(delim)
# ========================================================================================

blacklist = ["generator", "provider", "verified", "indices", "id$", "sizes", "display_url", "media_url$", "^url$", "url_https$", "inReplyTo"]
path = "H:\Data\RawData\GNIP\TwitterHistoricalPowertrack\\"

if __name__ == "__main__":

    # TO DO: Write another loop to go over all of the months
    current_month = "August-2014-Master"
    fileList = os.listdir(path + current_month)
    csvfile = open("H:\Data\RawData_csv\GNIP\TwitterHistoricalPowertrack\\" + current_month + "\\" + current_month + ".csv", 'w')
    for i in fileList:
        print path + "August-2014-Master\\" + i
        CSVfromTwitterJSON(path + "August-2014-Master\\" + i, csvfile)
    csvfile.close()

    # TO DO: Write a function that prints only the fields that we want to throw away to show people why we're throwing them away