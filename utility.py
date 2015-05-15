import os
import sys
import json
import ConfigParser
import os, errno
import csv, codecs, cStringIO

# =============================================================================================
# Class to create a unicode based CSV writer
# Set the default encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")

# Class given in Python 2.7 documentation for handling of unicode documents
class CSVUnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8-sig", **kwds):
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        '''writerow(unicode) -> None
        This function takes a Unicode string and encodes it to the output.
        '''
        self.writer.writerow([s.encode("utf-8", "ignore") for s in row])
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# =============================================================================================
# Function to emulate mkdir -p in Python
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

# =============================================================================================
# twAggregateByDay("H:\\Data\\RawData\\GNIP\\TwitterHistoricalPowertrack\\September-2014-Master\\")
def twAggregateByDay(jsonDirectory):
    myfiles = []
    myfilename = ""
    jsonfile = None
    #Use the os package to find all files in the directory and its subdirectories
    for (dirpath, dirnames, filenames) in os.walk(jsonDirectory):
        #Add the full name of the file to the list
        myfiles.extend(os.path.join(dirpath, filename) for filename in filenames)
    #For each file in the list
    outputDirectory = ''.join(jsonDirectory.split('\\')[0:-1])
    for myfile in myfiles:
        #Require .json and exclude .csv
        if ".json" in myfile and not ".csv" in myfile and not "errors" in myfile and "twitter" in myfile:
            onefile = open(myfile, "r")
            name = myfile.split("_")
            filename = "tw"+"_".join(name[3:len(name)-2])+".json"
            #Check if still same day file
            if filename != myfilename:
                if jsonfile != None:
                    jsonfile.close()
                jsonfile = open(jsonDirectory+filename,"a")
                myfilename = filename
                print filename
            jsonfile.write(onefile.read())
            onefile.close()
    jsonfile.close()

# =============================================================================================
# fbAggregateByDay(2014, 1, 12, conf)
def fbAggregateByDay(year, m1, m2, conf):
    src_parts_path = conf.get("facebook", "prod_src_parts_path")
    src_path = conf.get("facebook", "prod_src_path")
    for i in range(m1, m2 + 1):
        parts_src = src_parts_path.format(year, str(i).zfill(2))
        src = src_path.format(year, str(i).zfill(2))
        fileList = os.listdir(parts_src)
        curr_file = None
        fileList = sorted(fileList)
        for j in fileList:
            fileName = '-'.join(j.split('-')[:-2])
            if curr_file is None:
                curr_file = open(src + fileName + ".json", 'w')
            currFileName = curr_file.name.split('.')[0].split('\\')[-1]
            print fileName
            if currFileName == fileName:
                partFile = open(parts_src + j, 'r')
                for j in partFile.readlines():
                    curr_file.write(j)
                partFile.close()
            else:
                curr_file.close()
                curr_file = None

# =============================================================================================
# Prints python dict object in a nested manner
def dump(obj, nested_level=0, output=sys.stdout):
    spacing = '   '
    if type(obj) == dict:
        print >> output, '%s{' % ((nested_level) * spacing)
        for k, v in obj.items():
            if hasattr(v, '__iter__'):
                print >> output, '%s%s:' % ((nested_level + 1) * spacing, k)
                dump(v, nested_level + 1, output)
            else:
                print >> output, '%s%s: %s' % ((nested_level + 1) * spacing, k, v)
        print >> output, '%s}' % (nested_level * spacing)
    elif type(obj) == list:
        print >> output, '%s[' % ((nested_level) * spacing)
        for v in obj:
            if hasattr(v, '__iter__'):
                dump(v, nested_level + 1, output)
            else:
                print >> output, '%s%s' % ((nested_level + 1) * spacing, v)
        print >> output, '%s]' % ((nested_level) * spacing)
    else:
        print >> output, '%s%s' % (nested_level * spacing, obj)