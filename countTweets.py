# Hard coded months in line number 9. Add/Remove months and maintain the format of 'Month': 'zeroPaddedNumOfMonth'. Eg: 'March': '03'
# Change the year on lines 16 and 17 to the corresponding 4 digit year of that month.

# Command for running
# python countTweets <4 digit year>
# Eg: python countTweets 2014

import ConfigParser
import os
import json
import sys

def countTweets(src_path, dest_path):
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

months = map(lambda x: str(x).zfill(2), range(1, 13))
delim = ','

if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    conf = ConfigParser.ConfigParser()
    conf.read('config\\config.cfg')
    if op == "year":
        for i in months:
            print i
            src_path = conf.get('twitter', 'prod_src_path').format(year + i)
            dest_path = conf.get('twitter', 'prod_dest_path').format(year + i, 'COUNTS')
            countTweets(src_path, dest_path)
    elif op == "month":
        src_path = conf.get('twitter', 'prod_src_path').format(year + month)
        dest_path = conf.get('twitter', 'prod_dest_path').format(year + month, 'COUNTS')
        countTweets(src_path, dest_path)
    elif op == "special":
        proj_name = sys.argv[4]
        # src_path = conf.get('twitter', 'prod_src_path').format(year + month + '_' + proj_name)
        # dest_path = conf.get('twitter', 'prod_dest_path').format(year + month + '_' + proj_name, 'COUNTS')
        src_path = "H:\\Data\\RawData\\GNIP\\Twitterhistoricalpowertrack\\201410_LCC\\second_run\\"
        dest_path = "H:\\Data\\RawData_csv\\GNIP\\Twitterhistoricalpowertrack\\201410_LCC\\COUNTS\\"
        countTweets(src_path, dest_path)