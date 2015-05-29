# This downloads from Mongo and converts to CSV or it uploads to Mongo
import TwitterClient as TC
import sys
import json

def upload(client):
    client.fixByMonth()
    client.updateRules()
    client.iterateOverFiles()

def download(client, rule):
    client.updateRules()
    client.queryDB(rule)

# Eg python Twitter.py upload/download 2015 02 t/f (LCC)
# t/f ==> t: running on server, f: running on a desktop machine
if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    server = sys.argv[4].lower()
    if server == 'f':
        server = False
    elif server == 't':
        server = True
    try:
        proj = sys.argv[5]
    except IndexError:
        proj = ""

    client = TC.TwitterClient(year, month, server, proj)
    if op == 'upload':
        upload(client)
    elif op == 'download':
        rules = {}
        with open('config\\twitter\\tw_rules.json') as r:
            rules = json.loads(r.read())
        max_rule = max(v for k, v in rules.items())
        for i in range(1, max_rule + 1):
            download(client, i)
