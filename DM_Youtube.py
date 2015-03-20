import sys
import ConfigParser
import json
import os
from bs4 import BeautifulSoup
from CSVUnicodeWriter import CSVUnicodeWriter

def getData(filename):
    f = open(filename)
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    data = []
    line = ""
    for i in f.readlines():
        if '</entry>' in i:
            line += i
            dataLine = extract(line, fields)
            print dataLine
            if dataLine is not None:
                data.append(dataLine)
            line = ""
        else:
            line += i
    f.close()
    return data


def extract(line, fields):
    soup = BeautifulSoup(line)
    data = {}
    for (key, val) in fields.iteritems():
        part_xml = soup.find(key)
        if isinstance(val, list):
            for j in val:
                try:
                    item = part_xml.find(j).get_text().strip()
                    newKey = key + '_' + j
                except AttributeError:
                    return None
        else:
            try:
                item = part_xml.get_text().strip()
                newKey = key
            except AttributeError:
                return None
        data[newKey] = item
        print newKey + " " + item
    return data


if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    src_path = conf.get("youtube", "prod_src_path")
    dest_path = conf.get("youtube", "prod_dest_path")
    if op == "yearly":
        for i in range(1, 13):
            src = src_path.format(year, str(i).zfill(2))
            dest = dest_path.format(year, str(i).zfill(2)) + 'CSV\\'
            try:
                fileList = os.listdir(src)
            except IOError, WindowsError:
                print e
                continue
            for j in fileList:
                if "xml" in j and "error" not in j and ("v3" in j): # or "comments" in j):
                    print j
                    data = getData(src + j)
                    filename = j.split('.')[0]
                    
    elif op == "interval":
        start_month = sys.argv[3]
        end_month = sys.argv[4]
        for i in range(int(start_month), int(end_month) + 1):
            src = src_path.format(year, str(i).zfill(2))
            dest = dest_path.format(year, str(i).zfill(2)) + 'CSV\\'
            try:
                fileList = os.listdir(src)
            except IOError, WindowsError:
                print e
                continue
            for j in fileList:
                if "xml" in j and "error" not in j and ("v3" in j): # or "comments" in j):
                    print j
                    data = getData(src + j)
                    filename = j.split('.')[0]
                    