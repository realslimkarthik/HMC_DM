# Hard coded months in line number 9. Add/Remove months and maintain the format of 'Month': 'zeroPaddedNumOfMonth'. Eg: 'March': '03'
# Change the year on lines 16 and 17 to the corresponding 4 digit year of that month.

import ConfigParser
import os
import json
import sys
import re

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
    ruleCSV = open(dest_path + 'ruleCount_fullMonth_' + month + '.csv', 'w')
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
            
    r = open(conf_path.format("tw_rules.json"))
    rules = json.loads(r.read())
    r.close()
    r = open(conf_path.format("tw_rules_tags.json"))
    rulesToTags = json.loads(r.read())
    r.close()
    ruleCSV.write('Rule' + delim + 'Rule Id' + delim + "Tag" + delim + 'Count' + delim + 'Day' + delim + 'Month' + '\n')
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
                ruleCSV.write(k + delim + rId + delim + rTag + delim + str(v) + delim + date + delim + month + '\n')
    ruleCSV.close()
            


# def countTweetsPublic(src_path, dest_path):
#     fileList = os.listdir(src_path)
#     ruleCount = {}
#     for j in fileList:
#         print j
#         f = open(src_path + j)
#         rule = j.split('_')[0]
#         rule = re.sub('[0-9%]', '', rule)
#         ruleFreq = len(f.readlines())
#         f.close()
#         if rule in ruleCount:
#             ruleCount[rule] += ruleFreq
#         else:
#             ruleCount[rule] = ruleFreq
#     f = open(dest_path + 'goldilocks_counts.csv', 'w')

#     f.close()





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
            countTweets(src_path, dest_path, i)
    elif op == "month":
        month = sys.argv[3]
        src_path = conf.get('twitter', 'prod_src_path').format(year + month)
        dest_path = conf.get('twitter', 'prod_dest_path').format(year + month, 'COUNTS')
        countTweets(src_path, dest_path, month)
    elif op == "special":
        proj_name = sys.argv[4]
        # src_path = conf.get('twitter', 'prod_src_path').format(year + month + '_' + proj_name)
        # dest_path = conf.get('twitter', 'prod_dest_path').format(year + month + '_' + proj_name, 'COUNTS')
        src_path = "H:\\Data\\RawData\\GNIP\\Twitterhistoricalpowertrack\\201410_LCC\\second_run\\"
        dest_path = "H:\\Data\\RawData_csv\\GNIP\\Twitterhistoricalpowertrack\\201410_LCC\\COUNTS\\"
        countTweets(src_path, dest_path)
