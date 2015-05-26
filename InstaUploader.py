import sys
import InstagramClient as insta

if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    try:
        server = sys.argv[3]
    except IndexError:
        server = True

    if server != True:
        server = False
    uploader = insta.InstagramClient(year, month, server)
    uploader.iterateOverFiles()