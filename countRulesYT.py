import sys
import ConfigParser
import json
import os
from bs4 import BeautifulSoup
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
            if counts[0] != None:
                if counts[0] in tagCounts:
                    tagCounts[counts[0]] += 1
                else:
                    tagCounts[counts[0]] = 1
                if counts[0] in ruleCounts:
                    ruleCounts[counts[0]] += 1
                else:
                    ruleCounts[counts[0]] = 1
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

def writeCounts(counts, csvfile):
    writer = CSVUnicodeWriter(csvfile)
    writer.writerow(["Tag", "Count"])
    for (key, val) in counts.iteritems():
        writer.writerow([key, str(val)])
    

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
                    tagCsvfile = open(dest + filename + "_tags" + ".csv", "w")
                    writeCounts(overallCounts[0], tagCsvfile)
                    tagCsvfile.close()
                    ruleCsvfile = open(dest + filename + "_rules" + ".csv", "w")
                    writeCounts(overallCounts[1], ruleCsvfile)
                    ruleCsvfile.close()
    elif op == "interval":
        start_month = sys.argv[3]
        end_month = sys.argv[4]
        for i in range(start_month, end_month):
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
                    tagCsvfile = open(dest + filename + "_tags" + ".csv", "w")
                    writeCounts(overallCounts[0], tagCsvfile)
                    tagCsvfile.close()
                    ruleCsvfile = open(dest + filename + "_rules" + ".csv", "w")
                    writeCounts(overallCounts[1], ruleCsvfile)
                    ruleCsvfile.close()