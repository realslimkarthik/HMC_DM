# python InstaUploader.py 2015 03 t/f
# Example InstaUploader.py 2015 03 f

import sys
import InstagramClient as insta

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    server = sys.argv[3].lower()
    if server == 't':
        server = True
    elif server == 'f':
        server = False
    uploader = insta.InstagramClient(year, month, server)
    uploader.iterateOverFiles()