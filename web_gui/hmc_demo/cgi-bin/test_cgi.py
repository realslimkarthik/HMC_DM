#print "Content-type: text/html"
#print
#print "<title>Test CGI</title>"
#print "<p>Hello World!</p>"
# Perform Mongo transation
from pymongo import MongoClient, errors, ASCENDING
from datetime import datetime
import json
import sys


host='10.177.54.27'
port=27017
username='HMCDev'
password='KaThC0k3'
authDB='admin';
mongoClient = MongoClient(host, port)
mongoClient.twitter.authenticate(username, password, source=authDB)
db = mongoClient['twitter']

year = '2014'
month = '12'

#load input as json
myjson = json.load(sys.stdin)
#{'query','collection','increment','step'}

#Add hints to improve performance
hintstr = []
if('mrt' in myjson['query']):
    hintstr.append(('mrt', ASCENDING));
if('mrv' in myjson['query']):
    hintstr.append(('mrv', ASCENDING));


#NOT USED
#{"sTag":sTag,"sRule",sRule,"sTimeStart" :sTimeStart, "sTimeEnd":sTimeEnd, "sSplice":sSplice);
#sTag = [];
#sRule = [];
#sTimeStart = {Year,Month}
#sTimeEnd = {Year,Month}

#coll_names = set()
#for k in myjson['timeRange']:
#    collName = k
#    for j in range(1, 7):
#        coll_names.add(collName + '_' + str(j))


totalcount = 0
# For each of the collections
#for i in coll_names:
coll = db[myjson['collection']]
# Query to find all records of a particular rule
countdata = coll.find(myjson['query'],no_cursor_timeout=True).hint(hintstr).skip(myjson['increment']*myjson['step']).limit(myjson['increment']).count(with_limit_and_skip=True)
totalcount = totalcount + countdata
    
result = {"success":'true', "totalcount" : totalcount};
print "Content-type: application/json\n\n"
json.dump(result,sys.stdout)