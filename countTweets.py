# Command to execute this file
# python countTweets.py <year|month|special> t|f <4 digit year> <2 digit month>
# Example: python countTweets.py month t 2015 01

import ConfigParser
import os
import json
import sys
import re
from utility import CSVUnicodeWriter, mkdir_p


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
    ruleCSV.write('Rule' + delim + 'Rule Id' + delim + "Tag" + delim + 'Count' + delim + 'Date' + delim + 'Month' + '\n')
    for i in ruleCount:
        date = i['date']
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
            

def get_src_and_dest_paths(server, year, month, proj_name=None):
    if not proj_name:
        if server == 't':
            src_path = conf.get('twitter', 'prod_src_path').format(year + month)
            dest_path = conf.get('twitter', 'prod_dest_path').format(year + month, 'COUNTS')
        elif server == 'f':
            src_path = conf.get('twitter', 'src_path').format(year + month)
            dest_path = conf.get('twitter', 'dest_path').format(year + month, 'COUNTS')
        else:
            src_path = None
            dest_path = None
    else:
        if server == 't':
            src_path = conf.get('twitter', 'prod_spl_src_path').format(year + month, proj_name)
            dest_path = conf.get('twitter', 'prod_spl_dest_path').format(year + month, proj_name, 'COUNTS')
        elif server == 'f':
            src_path = conf.get('twitter', 'spl_src_path').format(year + month, proj_name)
            dest_path = conf.get('twitter', 'spl_dest_path').format(year + month, proj_name, 'COUNTS')
        else:
            src_path = None
            dest_path = None
    return src_path, dest_path


months = map(lambda x: str(x).zfill(2), range(1, 13))
delim = ','


if __name__ == "__main__":
    op = sys.argv[1]
    server = sys.argv[2]
    year = sys.argv[3]
    conf = ConfigParser.ConfigParser()
    conf.read('config\\config.cfg')
    conf_path = conf.get("conf", "conf_path")
    if op == "year":
        for month in months:
            src_path, dest_path = get_src_and_dest_paths(server, year, month)
            if src_path and dest_path:
                mkdir_p(dest_path)
                countTweets(src_path, dest_path, month)
            else:
                print 'invalid server parameter'
    elif op == "month":
        month = sys.argv[4]
        src_path, dest_path = get_src_and_dest_paths(server, year, month)
        if src_path and dest_path:
            mkdir_p(dest_path)
            countTweets(src_path, dest_path, month)
    elif op == "special":
        month = sys.argv[4]
        proj_name = sys.argv[5]
        src_path, dest_path = get_src_and_dest_paths(server, year, month, proj_name=proj_name)
        if src_path and dest_path:
            mkdir_p(dest_path)
            countTweets(src_path, dest_path, month)
