import json
import general_functions as gf
import TwitterMongoUploader as TMU
import time
from datetime import datetime


def preprocessDTTweet(inputTweet):
        #Date format example: 3/20/2012  1:00:04 PM
        
        # Changing postedTime into ISO format for processing using JavaScript in Mongo
        dateStr = inputTweet['[M] posted_time: ']
        timeStruct = time.strptime(dateStr, "%m/%d/%Y %H:%M:%S")
        dateObj = datetime.fromtimestamp(time.mktime(timeStruct))
        inputTweet['[M] posted_time:'] = dateObj

        # Select the collectioin name from YYYYMM
        collName = dateObj.strftime("%Y%m")

        strRuleMatch = '[M] rule_match: '

        # Change mrv field into tw_rule.json ==> mrv
        rules = json.loads(open("config\\twitter\\tw_rules.json",'rb').read())
        #make sure rule exists
        if inputTweet[strRuleMatch] not in rules.keys():
            print "Could not find rule match " + inputTweet[strRuleMatch] + " in rules file"
            return
        inputTweet[strRuleMatch] = rules[inputTweet[strRuleMatch]]

        # Change mrv field into tw_rules_tags.json ==> tag
        rules_tags = json.loads(open("config\\twitter\\tw_rules_tags.json",'rb').read())
        if inputTweet[strRuleMatch] not in rules_tags.keys():
            print "Could not find rule tag match " + inputTweet[strRuleMatch] + " in rules tag file"
            return
        tempTags = rules_tags[inputTweet[strRuleMatch]]


        # Change tag into tw_tags_json ==> tag # (mrt)
        rules_tags_num = json.loads(open("config\\twitter\\tw_tags.json",'rb').read())
        if tempTags not in rules_tags_num.keys():
            print "Could not find rule tag number match " + inputTweet[strRuleMatch] + " in rules tag number file"
            return
        inputTweet['[M] rule_match_tag: '] = rules_tags_num[tempTags] 


        mongoConf = json.loads(open("config\\twitter\\tw_dt_fieldsToMongo.json",'rb').read())
        newRecord = {}
        # Iterate through each of the keys in the original Dict
        for (key, val) in inputTweet.iteritems():
            # Get the new translated key from the mongoConf dict
            newKey = mongoConf[key]
            # create a new with translated field names to upload onto the corresponding Mongo Collection
            newRecord[newKey] = val


        return(collName, newRecord)



def processDTTweet(filename):
    upload = TMU.TwitterMongoUploader(2015,3,False,"") #Dummy input parameters

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        adict = gf.XLSDictReader(filename)
    else:
        adict = gf.CSVDictReader(filename)
    for currow in adict:
        #Run row inputs through JSON name converters
        (colName,inputTweet) = preprocessDTTweet(currow)

        #Upload to Mango Database
        upload.populateMongo(colName,inputTweet)


