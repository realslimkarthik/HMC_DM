# python InstaUploader.py [upload|count|backfill] [t/f] year month
# Example InstaUploader.py upload f 2015 03

import os
import sys
import InstagramClient as insta

if __name__ == "__main__":
    op = sys.argv[1].lower()
    server = sys.argv[2].lower()
    year = sys.argv[3]
    month = sys.argv[4].zfill(2)
    if server == 't':
        server = True
    elif server == 'f':
        server = False
    instaClient = insta.InstagramClient(year, month, server)
    if op == 'upload':
        instaClient.iterateOverFiles()
    elif op == 'count':
        instaClient.countPosts(month, year)
    elif op == 'backfill':
        backfillPath = 'H:\\Data\\RawData\\GNIP\\Instagram\\Backfill\\'
        file_list = [backfillPath + file_name for file_name in os.listdir(backfillPath)]
        for file_name in file_list:
            if file_name.endswith('.xml'):
                instaClient.processBackfill(file_name)