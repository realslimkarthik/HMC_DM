# Hard coded months in line number 9. Add/Remove months and maintain the format of 'Month': 'zeroPaddedNumOfMonth'. Eg: 'March': '03'
# Change the year on lines 16 and 17 to the corresponding 4 digit year of that month.

import ConfigParser
import os
import json
import sys
import re
from utility import CSVUnicodeWriter, mkdir_p

def countTweetsIndividual(src_path, dest_path):
    fileList = os.listdir(src_path)
    for j in fileList:
        if len(j.split('_')) == 3:
            print j
            f = open(src_path + j)
            f.seek(0, 0)
            ruleCount = {}
            tagCount = {}
            for k in f.readlines():
                try:
                    lineJson = json.loads(k)
                except ValueError:
                    continue
                try:
                    for l in lineJson['gnip']['matching_rules']:
                        if l['value'] not in ruleCount:
                            ruleCount[l['value']] = 1
                        else:
                            ruleCount[l['value']] += 1
                        if l['tag'] not in tagCount:
                            tagCount[l['tag']] = 1
                        else:
                            tagCount[l['tag']] += 1
                        print l['value']
                        print l['tag']
                except KeyError:
                    continue
            f.close()
            ruleCSV = open(dest_path + 'ruleCount' + '_'.join(j.split('.')[0].split('_')) + '.csv', 'w')
            ruleCSV.write('Rule' + delim + 'Count' + '\n')
            for (key, val) in ruleCount.iteritems():
                ruleCSV.write(key + delim + str(val) + '\n')
            ruleCSV.close()
            tagCSV = open(dest_path + 'tagCount' + '_'.join(j.split('.')[0].split('_')) + '.csv', 'w')
            tagCSV.write('Tag' + delim + 'Count' + '\n')
            for (key, val) in tagCount.iteritems():
                tagCSV.write(key + delim + str(val) + '\n')
            tagCSV.close()


def countTweets(src_path, dest_path, month):
    fileList = os.listdir(src_path)
    ruleCount = []
    for j in fileList:
        if len(j.split('_')) == 3 and 'json' in j:
            print j
            f = open(src_path + j)
            f.seek(0, 0)
            rule = {}
            for k in f.readlines():
                try:
                    lineJson = json.loads(k)
                except ValueError:
                    continue
                try:
                    for l in lineJson['gnip']['matching_rules']:
                        if l['value'] not in rule:
                            rule[l['value']] = 1
                        else:
                            rule[l['value']] += 1
                except KeyError:
                    continue
            f.close()
            rule['date'] = j.split('.')[0].split('_')[-1]
            ruleCount.append(rule)
    
    printCSV(ruleCount, month)

def printCSV(ruleCount, month=-1, splString='ruleCount_fullMonth_'):
    with open(conf.get("twitter", "rules")) as r:
        rules = json.loads(r.read())
        
    with open(conf.get("twitter", "rules_tags")) as rt:
        rulesToTags = json.loads(rt.read())
    
    ruleCSV = open(dest_path + splString + month + '.csv', 'w')
    # writer = CSVUnicodeWriter(ruleCSV)
    # writer.writerow(['Rule', 'Rule Id', 'Tag', 'Count', 'Date', 'Month'])
    ruleCSV.write('Rule' + delim + 'Rule Id' + delim + "Tag" + delim + 'Count' + delim + 'Date' + delim + 'Month' + '\n')
    for i in ruleCount:
        # row = []
        date = i['date']
        # monVal = month if month != -1 else i['month']
        for (k, v) in i.iteritems():
            if k != 'date':
                try:
                    rId = rules[k]
                    rTag = rulesToTags[k]
                except KeyError:
                    print k
                    continue
                ruleCSV.write(k + delim + str(rId) + delim + rTag + delim + str(v) + delim + date + delim + month + '\n')
    ruleCSV.close()
            


def countTweetsSearch(src_path, dest_path):
    fileList = os.listdir(src_path)
    ruleCount = {}
    for j in fileList:
        if 'json' in j:
            print j
            with open(src_path + j) as f:
                ruleFreq = sum(1 for _ in f)
            try:
                rule = j.split('_')[0]
                month = j.split('_')[2]
                date = j.split('_')[3]
            except IndexError:
                try:
                    rule = j.split('-')[0]
                    month = j.split('-')[2]
                    date = j.split('-')[3]
                except IndexError:
                    continue
            rule = rule.replace('%20', ' ')
            rule = rule.replace('%40', '@')
            rule = rule.replace('%23', '#')
            f.close()
            key = rule + '_' + month + '_' + date
            if key in ruleCount:
                ruleCount[key] += ruleFreq
            else:
                ruleCount[key] = ruleFreq
            print key

    with open(conf.get("twitter", "rules")) as r:
        rules = json.loads(r.read())
    
    with open(conf.get("twitter", "rules_tags")) as rt:
        rulesToTags = json.loads(rt.read())

    csv = open(dest_path + 'goldilocks_search.csv', 'wb')
    writer = CSVUnicodeWriter(csv)
    writer.writerow(['Rule', 'Rule Id', 'Tag', 'Count', 'Date', 'Month'])
    for (key, count) in ruleCount.iteritems():
        rule, month, date = key.split('_')
        rId = rules[rule]
        rTag = rulesToTags[rule]
        writer.writerow([rule, str(rId), rTag, str(count), date, month])
    csv.close()



months = map(lambda x: str(x).zfill(2), range(1, 13))
delim = ','

# Command to execute this file
# python countTweets.py <year|month|special> <4 digit year> <2 digit month>
# Example: python countTweets.py month 2015 01

if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read('config\\config.cfg')
    conf_path = conf.get("conf", "conf_path")
    
    if op == "year":
        for i in months:
            src_path = conf.get('twitter', 'prod_src_path').format(year + i)
            dest_path = conf.get('twitter', 'prod_dest_path').format(year + i, 'COUNTS')
            mkdir_p(dest_path)
            countTweets(src_path, dest_path, i)
    elif op == "month":
        month = sys.argv[3]
        src_path = conf.get('twitter', 'prod_src_path').format(year + month)
        dest_path = conf.get('twitter', 'prod_dest_path').format(year + month, 'COUNTS')
        mkdir_p(dest_path)
        countTweets(src_path, dest_path, month)
    elif op == "special":
        proj_name = sys.argv[3]
        # src_path = conf.get('twitter', 'prod_src_path').format(year + month + '_' + proj_name)
        # dest_path = conf.get('twitter', 'prod_dest_path').format(year + month + '_' + proj_name, 'COUNTS')
        src_path = "H:\\Data\\RawData\\TwitterPublic\\TwitterSearch\\goldilocks\\"
        dest_path = "H:\\Data\\RawData\\TwitterPublic\\TwitterSearch\\goldilocks\\"
        mkdir_p(dest_path)
        countTweetsSearch(src_path, dest_path)
