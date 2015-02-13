# Hard coded months in line number 9. Add/Remove months and maintain the format of 'Month': 'zeroPaddedNumOfMonth'. Eg: 'March': '03'
# Change the year on lines 16 and 17 to the corresponding 4 digit year of that month.

# Command for running
# python countTweets <4 digit year>
# Eg: python countTweets 2014

import ConfigParser
import os
import json
import sys

months = map(lambda x: str(x).zfill(2), range(1, 13))
delim = ','

if __name__ == "__main__":
    year = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read('config\config.cfg')
    for i in months:
        src_path = conf.get('twitter', 'prod_src_path').format(year + i)
        dest_path = conf.get('twitter', 'prod_dest_path').format(year + i, 'COUNTS')
        ruleCount = {}
        tagCount = {}
        fileList = os.listdir(src_path)
        for i in fileList:
            if len(i.split('-')) == 3:
                print i
                f = open(src_path + i)
                f.seek(0, 0)
                for j in f.readlines():
                    try:
                        lineJson = json.loads(j)
                    except ValueError:
                        continue
                    try:
                        for k in lineJson['gnip']['matching_rules']:
                            if k['value'] not in ruleCount:
                                ruleCount[k['value']] = 1
                            else:
                                ruleCount[k['value']] += 1
                            if k['tag'] not in tagCount:
                                tagCount[k['tag']] = 1
                            else:
                                tagCount[k['tag']] += 1
                            print k['value'] + ' ' + k['tag']
                    except KeyError:
                        continue
                f.close()
                ruleCSV = open(dest_path + 'ruleCount' + '_'.join(i.split('.')[0].split('-')) + '.csv', 'w')
                ruleCSV.write('Rule' + delim + 'Count' + '\n')
                for (key, val) in ruleCount.iteritems():
                    ruleCSV.write(key + delim + str(val) + '\n')
                ruleCSV.close()
                tagCSV = open(dest_path + 'tagCount' + '_'.join(i.split('.')[0].split('-')) + '.csv', 'w')
                tagCSV.write('Tag' + delim + 'Count' + '\n')
                for (key, val) in tagCount.iteritems():
                    tagCSV.write(key + delim + str(val) + '\n')
                tagCSV.close()