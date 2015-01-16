import string
import json
import sys
import os
import random
import re
import ConfigParser

# #Creates the CSV
# #makeCSVfromTwitterJSON("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\August-2014-Master\\legacy.json")
# def makeCSVfromTwitterJSON(jsonfilename, csvfile,errorfile=None, overwrite=False):
#     if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
#         jsonfile = open(jsonfilename,"r")
#         #Will track all variables seen across all tweets in the file
#         mykeys = []
#         #Will contain a dictionary for each processed tweet
#         tweetList = []
        
#         for line in jsonfile:
#             myline = string.strip(line)
#             if myline != "":
#                 #For each tweet in the file, decode the weird characters without complaining
#                 myline = myline.decode("utf-8", "ignore")
#                 #print myline
#                 #Remove new lines from within the tweet
#                 mylines = myline.split("\\n")
#                 if len(mylines) > 1:
#                     myline = " ".join(mylines)
#                 #Remove carriage returns from within the tweet
#                 mylines = myline.split("\\r")
#                 if len(mylines) > 1:
#                     myline = " ".join(mylines)
#                 #Remove problematic \s
#                 mylines = myline.split("\\\\")
#                 if len(mylines) > 1:
#                     myline = " ".join(mylines)
#                 mylines = myline.split("\\ ")
#                 if len(mylines) > 1:
#                     myline = " ".join(mylines)
#                 #Create a dictionary using the JSON processor
#                 try:
#                     tweet = json.loads(myline)
#                 except ValueError as e:
#                     if errorfile != None:
#                         write(jsonfilename+"\n"+myline+"\n"+e+"\n\n")
#                     else:
#                         print myline
#                         print e
#                 else:
#                     #Find the summary count
#                     if "Replay Request Completed" in myline:
#                         print tweet['info']['activity_count']
#                     else:
#                         #Create an empty dictionary
#                         a = {}
#                         #Send the JSON dictionary, the empty dictionary, and the list of all keys
#                         extractMod(tweet, a, mykeys, keyNotInBlacklist)
#                         #Add the output dictionary to the list
#                         tweetList.append(a)
#         #Print the number of tweets processed
#         jsonfile.close()
#         print len(mykeys)
#         print mykeys
#         printCSV(csvfile,tweetList,mykeys)
#         print "Finished... ", csvfile
# # ========================================================================================
# # Function that finds out if a key needs to be kept or thrown away
# def keyInBlacklist(key, nested):
#     for i in blacklist:
#         if nested == False:
#             outerMatch = re.search('^id$', key)
#             if outerMatch != None:
#                 continue
#         match = re.search(i + '$', key)
#         if match != None:
#             return True
#     return False

# # ========================================================================================
# # Function that finds keys that we think should be thrown away
# # To show the group that what we're throwing away isn't really required
# def keyNotInBlacklist(key, nestedKey, nested):
#     for i in blacklist:
#         # if nested == False:
#         #     outerMatch = re.search('^id$', key)
#         #     if outerMatch != None:
#         #         return True
#         nestedMatch = re.search(i, nestedKey)
#         if nestedMatch != None:
#             return False
        
#         match = re.search(i, key)
#         if match != None:
#             # print match.group(0)
#             # print i
#             return False
#     return True

# # ========================================================================================
# #Recursive function to process the input dictionary
# def extractMod(DictIn, Dictout, allkeys, filterFunction, nestedKey=""):
#     #If DictIn is a dictionary
#     if isinstance(DictIn, dict):
#         #Process each entry
#         for key, value in DictIn.iteritems():
#             #If nested, prepend the previous variables
#             if nestedKey != "":
#                 mykey = nestedKey+"_"+key
#                 print mykey
#             else:
#                 mykey = key
#             if isinstance(value, dict): # If value itself is dictionary
#                 extractMod(value, Dictout, allkeys, filterFunction, nestedKey=mykey)
#             elif isinstance(value, list): # If value itself is list
#                 extractMod(value, Dictout, allkeys, filterFunction, nestedKey=mykey)
#             else: #Value is just a string
#                 if filterFunction(key, nestedKey, True if nestedKey != "" else False):
#                     continue
#                 if isinstance(value, unicode) or isinstance(value, str):
#                     value = value.strip()
#                 if value != "":
#                     #If this is a new variable, add it to the list
#                     if mykey not in allkeys:
#                         allkeys.append(mykey)
#                     #Add it to the output dictionary
#                     if mykey not in Dictout:
#                         Dictout[mykey] = value
#                     else:
#                         Dictout[mykey] = unicode(Dictout[mykey])+"; "+unicode(value)
#     #If DictIn is a list, call extract on each member of the list
#     elif isinstance(DictIn, list):
#         for value in DictIn:
#             extract(value,Dictout,allkeys,nestedKey=nestedKey)
#     #If DictIn is a string, check if it is a new variable and then add to dictionary
#     else:
#         print DictIn
#         if isinstance(DictIn, unicode) or isinstance(DictIn, str):
#             DictIn = DictIn.strip()
#         if DictIn != "":
#             if not nestedKey in allkeys:
#                 allkeys.append(nestedKey)
#             if not nestedKey in Dictout:
#                 Dictout[nestedKey] = DictIn
#             else:
#                 Dictout[nestedKey] = unicode(Dictout[nestedKey])+"; "+unicode(DictIn)

# ========================================================================================
def flattenJSON(jsonFileName):
    jsonFile = open(jsonFileName, 'r')
    for line in jsonFile:
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
            tweet = json.loads(myline)
            print tweet
            # try:
            #     tweet = json.loads(myline)
            # except ValueError as e:
            #     if errorfile != None:
            #         write(jsonfilename+"\n"+myline+"\n"+e+"\n\n")
            #     else:
            #         print myline
            #         print e
            # else:
            #     #Find the summary count
            #     print "columbus"
            #     if "Replay Request Completed" in myline:
            #         print tweet['info']['activity_count']
    #return flattenedJSON
            # print tweet
    jsonFile.close()

# ========================================================================================
def countHashtags(jsonFileName):
    jsonFile = open(jsonFileName, 'r')
    for line in jsonFile:
        tweet = json.loads()

# ========================================================================================
# Populates the CSV. Gets CSV's file handle from caller
def CSVfromTwitterJSON(jsonfilename, csvfile, errorfile=None, overwrite=False):
    if (not os.path.isfile(jsonfilename+".csv")) or overwrite:
        jsonfile = open(jsonfilename, 'r')
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
# Function that finds out if a key needs to be kept or thrown away
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
    csvfile.write("\"ID\""+delim)

    for item in mykeys:
        if item == "id":
            continue
        if item == "postedTime":
            csvfile.write('month'+delim)
            csvfile.write('day'+delim)
            csvfile.write('year'+delim)
            csvfile.write('time'+delim)
        else:
            csvfile.write(item+delim)

    #For each tweet in the list, print the variables in the correct order (or "" if not present)
    for result in resultList:
        csvfile.write("\n")
        myid = result['id']
        myids = myid.split(":")
        csvfile.write("\"tw"+myids[2]+"\""+delim)
        for item in mykeys:
            if item in result:
                if item == "id":
                    continue
                if item == "postedTime":
                    pTime = result[item].split('T')
                    date = pTime[0].split('-')
                    csvfile.write(date[1]+delim)
                    csvfile.write(date[2]+delim)
                    csvfile.write(date[0]+delim)
                    time = pTime[1].split('.')[0]
                    csvfile.write(time+delim)
                    continue
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


blacklist = ["generator", "provider", "verified", "indices", "id$", "sizes", "display_url", "media_url$", "^url$", "url_https$", "inReplyTo", "twitter_filter_level"]
#path = "H:\Data\RawData\GNIP\TwitterHistoricalPowertrack\\"

if __name__ == "__main__":

    inputFile = "dummy_sample.json"
    outputFile = "dummy_sample_dropped.csv"
    # inputFile = "Sample of tw2014-09-02.json"
    # outputFile = "Sample of tw2014-09-02_dropped.csv"
    choice = sys.argv[1]
    op = sys.argv[2]
    if choice == "dev":
        if op == "transform":
            outF = open(outputFile, 'w')
            CSVfromTwitterJSON(inputFile, outF)
            outF.close()
        elif op == "count":
            flattenJSON(inputFile)
    elif choice =="prod":
        if op == "transform":
            conf = ConfigParser.ConfigParser()
            conf.read("config.cfg")
            # TO DO: Write another loop to go over all of the months
            current_month = "August-2014-Master"
            src_path = conf.get("twitter", "prod_src_path").format(current_month)
            fileList = os.listdir(src_path)
            dest_path = conf.get("twitter", "prod_dest_path").format(current_month)
            csvfile1 = open(dest_path + '1-15.csv', 'w')
            csvfile2 = open(dest_path + '16-30.csv', 'w')
            for i in fileList:
                if int(i.split('-')[-3]) < 16:
                    CSVfromTwitterJSON(src_path + i, csvfile1)
                else:
                    CSVfromTwitterJSON(src_path + i, csvfile2)
            csvfile1.close()
            csvfile2.close()


    # TO DO: Write a function that prints only the fields that we want to throw away to show people why we're throwing them away