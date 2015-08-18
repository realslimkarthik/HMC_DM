# python InstaUploader.py 2015 03 t/f
# Example InstaUploader.py 2015 03 f

import sys
import InstagramClient as insta

if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    server = sys.argv[4].lower()
    if server == 't':
        server = True
    elif server == 'f':
        server = False
    uploader = insta.InstagramClient(year, month, server)
    if op == 'upload':
        uploader.iterateOverFiles()
    elif op == 'count':
        uploader.countPosts(month, year)