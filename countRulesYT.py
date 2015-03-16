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
    soup = BeautifulSoup(f.read())
    f.close()
    for i in soup.find_all("gnip:matching_rule"):
        rule = i.get_text()
        if rule != "":
            try:
                tag = rules[rule]
            except KeyError:
                print rule
                print filename
                sys.exit(1)
                tag = rules[rule.strip("\"")]
            if tag in tagCounts:
                tagCounts[tag] += 1
            else:
                tagCounts[tag] = 1
            if rule in ruleCounts:
                ruleCounts[rule] += 1
            else:
                ruleCounts[rule] = 1
    del soup
    return (tagCounts, ruleCounts)


def writeCounts(counts, csvfile):
    tagWriter = CSVUnicodeWriter(csvfile)
    tagWriter.writerow(["Tag", "Count"])
    for (key, val) in counts.iteritems():
        tagWriter.writerow([key, str(val)])
    

if __name__ == "__main__":
    year = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    src_path = conf.get("youtube", "prod_src_path")
    dest_path = conf.get("youtube", "prod_dest_path")
    r = open(conf_path.format('yt_rulesToTags.json'))
    rules = json.loads(r.read())
    r.close()
    for i in range(10, 13):
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