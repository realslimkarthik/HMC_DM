import string

#jsonCreateFromFileUntagged("h:/data/code/Rules/youtube_ids_nocomments.txt","h:/data/code/Rules/youtube_ids_nocomments.json")

def splitRulesTabbed(ruleFileName, fileidprefix):
    ruleFile = open(ruleFileName, "r")

    #Put the new ids in files of no more than 4000 to be sent via curl to GNIP [GNIP's limit]
    outid = open("".join([fileidprefix,"0.txt"]), "w")
    div = 600

    for index,entry in enumerate(ruleFile):
        indexDiv = index/div
        if indexDiv*div==index and index != 0:
          outid.close()
          outid = open("".join([fileidprefix,str(indexDiv),".txt"]), "w")
        outid.write(entry)
    outid.close()

    #Create a file for the .bat to use to enumerate the filenames
    idnames = open("".join([fileidprefix,"names.txt"]), "w")
    for k in range(indexDiv+1):
        #Create a JSON file from the ids
        jsonCreateFromFileTabSep("".join([fileidprefix,str(k),".txt"]), "".join([fileidprefix,str(k),".json"]))
        idnames.write("".join([fileidprefix,str(k),".json","\n"]))
    idnames.close()

def splitRulesNoTags(ruleFileName, fileidprefix):
    ruleFile = open(ruleFileName, "r")

    #Put the new ids in files of no more than 4000 to be sent via curl to GNIP [GNIP's limit]
    outid = open("".join([fileidprefix,"0.txt"]), "w")
    div = 4000

    for index,entry in enumerate(ruleFile):
        indexDiv = index/div
        if indexDiv*div==index and index != 0:
          outid.close()
          outid = open("".join([fileidprefix,str(indexDiv),".txt"]), "w")
        outid.write(entry)
    outid.close()

    #Create a file for the .bat to use to enumerate the filenames
    idnames = open("".join([fileidprefix,"names.txt"]), "w")
    for k in range(indexDiv+1):
        #Create a JSON file from the ids
        jsonCreateFromFileUntagged("".join([fileidprefix,str(k),".txt"]), "".join([fileidprefix,str(k),".json"]))
        idnames.write("".join([fileidprefix,str(k),".json","\n"]))
    idnames.close()

def jsonCreateFromFileTabSep(infile, outfile):
    filein = open(infile, 'r')
    fileout = open(outfile, 'w')

    fileout.write("{\n\"rules\":\n[")
    firstline = 1

    #for each line in our input file
    for line in filein:
        x = string.strip(line)
        if(x != ""):
            if firstline == 0:
                fileout.write(",\n")
            else:
                fileout.write("\n")
                firstline=0
            myentry = string.split(x, "\t")
            tag = myentry[0]
            try:
                rule = myentry[1]
            except IndexError as e:
                rule = myentry[0]
            #Need to escape any " already in the rule or tag
            tag1 = string.split(tag, sep="\"")
            tag2 = string.join(tag1, sep="\\\"")
            rule1 = string.split(rule, sep="\"")
            rule2 = string.join(rule1, sep="\\\"")
            fileout.write(string.join(["{\"value\":\"" , rule2 ,"\",\"tag\":\"", tag2 ,"\"}"], ""))

    fileout.write("\n]\n}")

    filein.close()
    fileout.close()                  

def jsonCreateFromFileUntagged(infile, outfile):
    filein = open(infile, 'r')
    fileout = open(outfile, 'w')

    fileout.write("{\n\"rules\":\n[")
    firstline = 1

    #for each line in our input file
    for line in filein:
        if firstline == 0:
            fileout.write(",\n")
        else:
            fileout.write("\n")
            firstline=0
        x = string.strip(line)
        if(x != ""):
            #Need to escape any " already in the rule
            y = string.split(x, sep="\"")
            z = string.join(y, sep="\\\"")
            fileout.write("".join(["{\"value\":\"" , z ,"\"}"]))

    fileout.write("\n]\n}")

    filein.close()
    fileout.close()                  


def jsonCreateFromList(inlist, outfile):
    fileout = open(outfile, 'w')

    fileout.write("{\n\"rules\":\n[")
    firstline = 1

    #for each element in our input list
    for line in inlist:
        if firstline == 0:
            fileout.write(",\n")
        else:
            fileout.write("\n")
            firstline=0
        x = string.strip(line)
        if(x != ""):
            #Need to escape any " already in the rule or tag
            y = string.split(x, sep="\"")
            z = string.join(y, sep="\\\"")
            fileout.write(string.join(["{\"value\":\"" , z ,"\",\"tag\":\"", z ,"\"}"], ""))

    fileout.write("\n]\n}")
    fileout.close()                  

def jsonCreateFromListUntagged(inlist, outfile):
    fileout = open(outfile, 'w')

    fileout.write("{\n\"rules\":\n[")
    firstline = 1

    #for each element in our input list
    for line in inlist:
        if firstline == 0:
            fileout.write(",\n")
        else:
            fileout.write("\n")
            firstline=0
        x = string.strip(line)
        if(x != ""):
            #Need to escape any " already in the rule
            y = string.split(x, sep="\"")
            z = string.join(y, sep="\\\"")
            fileout.write("".join(["{\"value\":\"" , z ,"\"}"]))

    fileout.write("\n]\n}")
    fileout.close()                  

