import TwitterMongoUploader as Tmu
import sys

# Eg python TwitterUpload.py 2015 02

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    try:
        server = sys.argv[3]
    except IndexError:
        server = True
        
    try:
        proj = sys.argv[4]
    except IndexError:
        proj = ""

    if server != True:
        server = False
    uploader = Tmu.TwitterMongoUploader(year, month, server, proj)
    uploader.updateRules()
    uploader.iterateOverFiles()