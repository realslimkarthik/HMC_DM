# This downloads from Mongo and converts to CSV or it uploads to Mongo
import TwitterClient as TC
import sys
import json
from datetime import datetime 
from general_functions import Simplemovingaverage

def upload(client,fixByMonth):
    if fixByMonth:
        client.fixByMonth()
    else:
        print "Skipping fixByMonth()"
    client.iterateOverFiles()

def download(client, rule):
    client.queryDB(rule)

# Eg python Twitter.py upload/download 2015 02 t/f <download:start_mrv_idx | upload:s/f> (LCC)
# t/f ==> t: running on server, f: running on a desktop machine
# s/f ==> s: skip fixByMonth, f: run fixByMonth
if __name__ == "__main__":
    startidx = 1  #default to start
    fixByMonth = True
    op = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    server = sys.argv[4].lower()
    if server == 'f':
        server = False
    elif server == 't':
        server = True
        
    try:
        idx_or_sf = sys.argv[5]
    except IndexError:
        idx_or_sf = ""
            
    try:
        proj = sys.argv[6]
    except IndexError:
        proj = ""

    client = TC.TwitterClient(year, month, server, proj)
    client.updateRules() #Only need to update once
    if op == 'upload':
        if idx_or_sf != "":
            if idx_or_sf == 's':
                fixByMonth = False
            elif idx_or_sf == 'f':
                fixByMonth = True
        upload(client,fixByMonth)
    elif op == 'download':
        if idx_or_sf != "":
            startidx = int(idx_or_sf) #allow custom start range
        startidx = int(sys.argv[5])
        rules = {}
        with open('config\\twitter\\tw_rules.json') as r:
            rules = json.loads(r.read())
        max_rule = max(v for k, v in rules.items())
        sma = Simplemovingaverage(50) #allow for 50 datapoints for running average
        for i in range(startidx, max_rule + 1):
            print "---------------->Starting rule: " + str(i)
            starttime = datetime.now()
            download(client, i)
            endtime = datetime.now()
            deltatime = endtime - starttime
            print "================>Download time for " + str(i) + ": " + str(deltatime) 
            print "================>Estimated completion in minutes: " + str(sma(deltatime.total_seconds())/60 * (max_rule - i))
