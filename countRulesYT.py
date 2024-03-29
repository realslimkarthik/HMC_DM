import sys
import ConfigParser
import json
import os
from bs4 import BeautifulSoup
import re
from CSVUnicodeWriter import CSVUnicodeWriter


def countRules(filename, rules):
    f = open(filename)
    ruleCounts = {}
    tagCounts = {}
    line = ""
    for i in f.readlines():
        if '</entry>' in i:
            line += i
            counts = extractRule(line, rules)
            line = ""
            if counts[0] is not None:
                if counts[0] in tagCounts:
                    tagCounts[counts[0]] += 1
                else:
                    tagCounts[counts[0]] = 1
                if counts[1] in ruleCounts:
                    ruleCounts[counts[1]] += 1
                else:
                    ruleCounts[counts[1]] = 1
        else:
            line += i

    f.close()
    return (tagCounts, ruleCounts)


def extractRule(line, rules):
    soup = BeautifulSoup(line)
    try:
        rule = soup.find('gnip:matching_rule').get_text()
        tag = rules[rule]
    except (AttributeError, KeyError):
        try:
            rule = soup.find('gnip:rule').get_text()
            tag = rules[rule]
            print rule
        except (AttributeError, KeyError):
            tag = None
            rule = None
    return (tag, rule)


def writeCounts(counts, csvfile, header):
    writer = CSVUnicodeWriter(csvfile)
    writer.writerow([header, "Count"])
    for (key, val) in counts.iteritems():
        writer.writerow([key, str(val)])


def getStatsCounts(fileList, key, src, dest):
    outputFile = open(dest + key + '_stats.csv', 'wb')
    writer = CSVUnicodeWriter(outputFile)
    writer.writerow(["ID", "CommentCount", "ViewCount", "FavoriteCount", "DislikeCount", "LikeCount", "Month", "Day"])
    for i in fileList:
        print i
        f = open(src + i)
        for j in f.readlines():
            dataLine = re.split(r'\t+', j.strip())
            month = key[-2:]
            day = i.split('.')[0][-2:]
            dataLine.extend([month, day])
            try:
                int(dataLine[1])
            except ValueError:
                continue
            writer.writerow(dataLine)
        f.close()
    outputFile.close()

# python countRulesYT.py <yearly|interval|stats> year (start_month) (end_month)
# Example: python countRulesYT.py interval 2014 07 09

if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    src_path = conf.get("youtube", "prod_src_path")
    dest_path = conf.get("youtube", "prod_dest_path")
    r = open(conf_path.format('yt_rulesToTags.json'))
    rules = json.loads(r.read())
    r.close()
    if op == "yearly":
        for i in range(1, 13):
            src = src_path.format(year, str(i).zfill(2))
            dest = dest_path.format(year, str(i).zfill(2)) + 'COUNTS\\'
            try:
                fileList = os.listdir(src)
            except IOError, WindowsError:
                print e
                continue
            for j in fileList:
                if "xml" in j and "v3" in j and "error" not in j:
                    print j
                    overallCounts = countRules(src + j, rules)
                    filename = j.split('.')[0]
                    with open(dest + filename + "_tags" + ".csv", "wb") as tagCsvfile:
                        writeCounts(overallCounts[0], tagCsvfile, "Tags")
                    with open(dest + filename + "_rules" + ".csv", "wb") as ruleCsvfile:
                        writeCounts(overallCounts[1], ruleCsvfile, "Rules")
    elif op == "interval":
        start_month = sys.argv[3].zfill(2)
        end_month = sys.argv[4].zfill(2)
        for i in range(int(start_month), int(end_month) + 1):
            src = src_path.format(year, str(i).zfill(2))
            dest = dest_path.format(year, str(i).zfill(2)) + 'COUNTS\\'
            try:
                fileList = os.listdir(src)
            except IOError, WindowsError:
                print e
                continue
            for j in fileList:
                if "xml" in j and "v3" in j and "error" not in j:
                    print j
                    overallCounts = countRules(src + j, rules)
                    filename = j.split('.')[0]
                    with open(dest + filename + "_tags" + ".csv", "wb") as tagCsvfile:
                        writeCounts(overallCounts[0], tagCsvfile, "Tags")
                    with open(dest + filename + "_rules" + ".csv", "wb") as ruleCsvfile:
                        writeCounts(overallCounts[1], ruleCsvfile, "Rules")
    elif op == "stats":
        src = conf.get("youtube", "prod_src_stats_path")
        dest_path = conf.get("youtube", "prod_dest_stats_path")
        dest = dest_path.format(year)

        try:
            fileList = os.listdir(src)
        except IOError, WindowsError:
            print e
            sys.exit(1)
        outputFiles = {}
        for j in fileList:
            splitFile = j.split('-')
            if len(splitFile) != 4:
                splitFile = j.split('_')
                if len(splitFile) != 5:
                    continue
            month = splitFile[-2]
            year_on_file = splitFile[-3]
            if year_on_file != year:
                continue
            key = year + month
            if key not in outputFiles:
                outputFiles[key] = [j]
            else:
                outputFiles[key].append(j)
        for (key, val) in outputFiles.iteritems():
            getStatsCounts(val, key, src, dest)