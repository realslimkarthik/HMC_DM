import sys

if __name__ == "__main__":
    firstFile = sys.argv[1]
    #secondFile = sys.argv[2]
    f = open(firstFile)
    contents = f.readline()
    print len(contents)
    lastLine = contents[len(contents) - 1]
    print lastLine