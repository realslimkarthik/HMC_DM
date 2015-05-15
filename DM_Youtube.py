import sys
import ConfigParser
import json
import os, os.path
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
                    newKey = key + '_' + j
                    data[newKey] = item
                except AttributeError:
                    return None
        elif isinstance(val, dict):
            for (k, v) in val.iteritems():
                newKey = key + '_' + k
                try:
                    item = soup.find(key, rel=k)['href']
                except KeyError:
                    return None
                data[newKey] = item
        else:
            try:
                item = part_xml.get_text().strip()
                newKey = key
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


def getFileName(src_path, fileType, date, ext):
    inputFile = src_path + 'youtube_' + fileType + '_' + date[0] + '_' + date[1] + '_' + date[2] + '.' + ext
    return inputFile


def processBackfill(backfillFile):
    fileType, _,s_date, e_date = backfillFile.split('.')[0].split('_')[1:]
    start_date = (s_date[0:4], s_date[4:6], s_date[6:8])
    end_date = (e_date[0:4], e_date[4:6], e_date[6:8])
    sameDay = start_date == end_date

    src = src_path.format(start_date[0], start_date[1])
    dest = dest_path.format(end_date[0], end_date[1]) + 'CSV\\'

    start_inputFile = getFileName(src, fileType, start_date, 'xml')
    end_inputFile = ''  if sameDay else getFileName(src, fileType, end_date, 'xml')

    start_outputFile = getFileName(dest, fileType, start_date, 'csv')
    end_outputFile = '' if sameDay else getFileName(dest, fileType, end_date, 'csv')


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
                date = dataLine['updated'].split('T')[0]
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
        xmlFileName = getFileName(src, fileType, (y, m, d), 'xml')
        csvFileName = getFileName(src, fileType, (y, m, d), 'csv')
        # print xmlFileName, csvFileName, rawData[key]
        printCSV(xmlFileName, processedData[key], True)
        backfillRawFiles(rawData[key], xmlFileName)
    
    print backfillFile + ' is done'
    # print start_inputFile + '\n' + start_outputFile + '\n' + end_inputFile + '\n' + end_outputFile


def iterate(src_path, dest_path, month):
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
            data = getData(src + j)
            filename = j.split('.')[0] + '.csv'
            printCSV(dest + filename, data)


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

    elif op == "backfill":
        backfill_src = conf.get("youtube", "prod_backfill_path")
        try:
            fileList = os.listdir(backfill_src)
        except IOError, WindowsError:
            print e
            sys.exit(1)
        for j in fileList:
            if 'xml' in j and "error" not in j and ("v3" in j or "comments" in j):
                processBackfill(backfill_src + j)