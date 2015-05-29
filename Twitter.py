import TwitterClient as TC
import sys

def upload(client):
    client.updateRules()
    client.iterateOverFiles()

def download(client, rule):
    client.updateRules()
    client.queryDB(rule)

# Eg python TwitterUpload.py upload/download 2015 02 t/f
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
    if server != True:
        server = False

    client = TC.TwitterClient(year, month, server, proj)
    if op == 'upload':
        upload(client)
    elif op == 'download':
        for i in range(1, 848):
            download(client, i)
