import sys
import InstagramClient as insta

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    uploader = insta.InstagramClient(year, month)
    uploader.iterateOverFiles()