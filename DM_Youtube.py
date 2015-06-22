# Command to run the script
# python DM_Youtube.py [yearly/interval] <Four digit year> <Two numbers denoting the two interval months (optional)>
# Example python DM_Youtube.py interval 2014 8 10
# Example python DM_Youtube.py yearly 2014

import sys
import re
import ConfigParser
import json
import os, os.path
import pandas as pd
from bs4 import BeautifulSoup
from CSVUnicodeWriter import CSVUnicodeWriter


def getFileType(filename):
    if "comments" in filename:
        return "comments"
    elif "v3" in filename:
        return "info"
    else:
        return None


def getData(filename):
    fileType = getFileType(filename)
    f = open(filename)
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    data = []
    line = ""
    for i in f.readlines():
        if '</entry>' in i:
            line += i
            dataLine = extract(line, fields[fileType])
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
                    newKey = key + j
                    newKey = re.sub(':-', '', newKey)
                    item = re.sub('\r\n', ' ', item)
                    data[newKey] = item
                except AttributeError:
                    return None
        elif isinstance(val, dict):
            for (k, v) in val.iteritems():
                newKey = key + k
                try:
                    item = soup.find(key, rel=k)['href']
                except KeyError:
                    return None
                newKey = re.sub(':-', '', newKey)
                item = re.sub('\r\n', ' ', item)
                data[newKey] = item
        else:
            try:
                item = part_xml.get_text().strip()
                newKey = key
                newKey = re.sub(':-', '', newKey)
                item = re.sub('\r\n', ' ', item)
                data[newKey] = item
            except AttributeError:
                data = None
                break
    return data


def printCSV(filename, data, backfill=False):
    fileType = getFileType(filename)
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    keys = []
    if backfill:
        csvfile = open(filename, 'ab')
        csvfile.write('\n')
        writer = CSVUnicodeWriter(csvfile)
    else:
        csvfile = open(filename, 'wb')
        writer = CSVUnicodeWriter(csvfile)
        for (key, val) in fields[fileType].iteritems():
            if isinstance(val, list):
                for j in val:
                    keys.append(key + '_' + j)
            elif isinstance(val, dict):
                for (k, v) in val.iteritems():
                    keys.append(key + '_' + k)
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


def backfillRawFiles(backfillData, rawFile):
    xmlFile = open(rawFile, 'a')
    xmlFile.write('\n')
    for i in backfillData:
        xmlFile.write(i)
    xmlFile.close()


def processBackfill(backfillFile, raw_file, csv_file):
    fileType = backfillFile.split('.')[0].split('_')[1]
    
    f = open(backfillFile)
    line = ""
    data = []
    rawData = {}
    processedData = {}
    
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    date = ""

    for i in f.readlines():
        if '</entry>' in i:
            line += i
            dataLine = extract(line, fields[getFileType(fileType)])
            if dataLine is not None:
                date = dataLine['sourceupdated'].split('T')[0]
                if date in processedData:
                    processedData[date].append(dataLine)
                else:
                    processedData[date] = [dataLine]
            if date != "":
                if date in rawData:
                    rawData[date] += line
                else:
                    rawData[date] = line
            line = ""
        else:
            line += i

    for (key, val) in processedData.iteritems():
        y, m, d = key.split('-')
        xmlFileName = raw_file.format(y, m, d, fileType)
        csvFileName = csv_file.format(y, m, d, fileType)
        backfillRawFiles(rawData[key], xmlFileName)
        df = pd.DataFrame(processedData[key])
        with open(csvFileName, 'a') as csvfile:
            df.to_csv(csvfile, sep=',', index=False, header=False)
    
    print backfillFile + ' is done'


def iterate(src_path, dest_path, month, year):
    src = src_path.format(year, str(i).zfill(2))
    dest = dest_path.format(year, str(i).zfill(2)) + 'CSV\\'
    try:
        fileList = os.listdir(src)
    except IOError, WindowsError:
        print e
        return
    for j in fileList:
        if "xml" in j and "error" not in j and ("v3" in j or "comments" in j):
            print j
            # Returns XML data as a dictionary
            data = getData(src + j)
            df = pd.DataFrame(data)
            filename = j.split('.')[0] + '.csv'
            with open(dest + filename, 'w') as csvfile:
                df.to_csv(csvfile, sep=',', index=False)
            # printCSV(dest + filename, data)


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
            iterate(src_path, dest_path, month, year)
                    
    elif op == "interval":
        start_month = sys.argv[3]
        end_month = sys.argv[4]
        for i in range(int(start_month), int(end_month) + 1):
            iterate(src_path, dest_path, i, year)

    elif op == "backfill":
        backfill_src = conf.get("youtube", "prod_backfill_path")
        raw_file = conf.get("youtube", "prod_backfill_src")
        csv_file = conf.get("youtube", "prod_backfill_dest")
        try:
            fileList = os.listdir(backfill_src)
        except IOError, WindowsError:
            print e
            sys.exit(1)
        for j in fileList:
            if 'xml' in j and "error" not in j and ("v3" in j or "comments" in j):
                processBackfill(backfill_src + j, raw_file, csv_file)