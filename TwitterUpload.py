import TwitterMongoUploader as Tmu
import sys

# Eg python TwitterUpload.py 2015 02

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    uploader = Tmu.TwitterMongoUploader(year, month)
    uploader.updateRules()
    uploader.iterateOverFiles()