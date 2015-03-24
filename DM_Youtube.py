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
                    data[newKey] = item
                except AttributeError:
                    return None
        else:
            try:
                if key == "link":
                    try:
                        item = part_xml['href']
                    except KeyError:
                        return None
                else:
                    item = part_xml.get_text().strip()
                newKey = key
                data[newKey] = item
            except AttributeError:
                data = None
                break
    return data


def printCSV(filename, data):
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    csvfile = open(filename, 'wb')
    writer = CSVUnicodeWriter(csvfile)
    keys = []
    for (key, val) in fields.iteritems():
        if isinstance(val, list):
            for j in val:
                keys.append(key + '_' + j)
        else:
            keys.append(key)
    row = keys
    writer.writerow(row)
    for i in data:
        row = []
        for key in keys:
            if key in i:
                row.append(i[key])
            else:
                row.append("")
        writer.writerow(row)
    csvfile.close()


def iterate(src_path, dest_path, month):
    src = src_path.format(year, str(i).zfill(2))
    dest = dest_path.format(year, str(i).zfill(2)) + 'CSV\\'
    try:
        fileList = os.listdir(src)
    except IOError, WindowsError:
        print e
        return
    for j in fileList:
        if "xml" in j and "error" not in j:
            if "v3" in j:
                print j
                data = getData(src + j)
                filename = j.split('.')[0] + '.csv'
                printCSV(dest + filename, data)
            # elif "comments" in j:


# Command to run the script
# python DM_Youtube.py [yearly/interval] <Four digit year> <Two numbers denoting the two interval months (optional)>
# Example python DM_Youtube.py interval 2014 8 10

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
            iterate(src_path, dest_path, month)
                    
    elif op == "interval":
        start_month = sys.argv[3]
        end_month = sys.argv[4]
        for i in range(int(start_month), int(end_month) + 1):
            iterate(src_path, dest_path, i)
            