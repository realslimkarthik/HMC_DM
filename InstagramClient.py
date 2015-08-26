import ConfigParser
import sys
import os
import json
import pandas as pd
import re
from csv import QUOTE_NONNUMERIC
from datetime import datetime
import calendar
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors, ASCENDING
from utility import mkdir_p, daterange, parseDateString



class InstagramClient(object):

    """
        Class that handles uploading data into Mongo, Object has the following properties:

        Attributes:
            year: Year in which the data was fetched
            month: Month in which data was fetched
            _conf: ConfigParser object for some control information
            _rules: Dictionary that provides Rule to Rule Index mapping
            _max_rule: Current largest Rule Index
            _tags: Dictionary that provides Tag to Tag Index mapping
            _max_tag: Current largest Tag Index
            _rules_tags: Dictionary that provides Rule to Tag mapping
            fields: List of fields that we keep from raw JSON and their corresponding mapping to new field names
            mongoConf: Mapping of fields from generated keys to optimized keys for Mongo Collections
            src: Path to where the input Source files are to be read from
            dest: Path to the Destination where the output files are to be written
            db: MongoDB Database in which to write into/read from

    """


    def __init__(self, year, month, server):
        self.year = str(year)
        self.month = str(month).zfill(2)
        
        self._conf = ConfigParser.ConfigParser()
        self._conf.read('config\\config.cfg')
        
        rules_file = open(self._conf.get('instagram', 'rules'))
        self._rules = json.loads(rules_file.read())
        rules_file.close()

        self._max_rule = max(val for val in self._rules.values())

        tags_file = open(self._conf.get('instagram', 'tags'))
        self._tags = json.loads(tags_file.read())
        tags_file.close()

        self._max_tag = max(val for val in self._tags.values())

        rules_tags_file = open(self._conf.get('instagram', 'rules_tags'))
        self._rules_tags = json.loads(rules_tags_file.read())
        rules_tags_file.close()

        with open(self._conf.get('instagram', 'fields')) as fieldsFile:
            self.fields = json.loads(fieldsFile.read())

        with open(self._conf.get('instagram', 'fields_mongo')) as fieldsToMongoFile:
            self.mongoConf = json.loads(fieldsToMongoFile.read())
        
        if server:
            self.src = self._conf.get('instagram', 'prod_src_path').format(self.year, self.month)
            self.dest = self._conf.get('instagram', 'prod_dest_path').format(self.year, self.month)
            self.parts = self._conf.get('instagram', 'prod_src_parts_path')
        else:
            self.src = self._conf.get('instagram', 'src_path').format(self.year, self.month)
            self.dest = self._conf.get('instagram', 'dest_path').format(self.year, self.month)
            self.parts = self._conf.get('instagram', 'src_parts_path')


        host = self._conf.get('mongo', 'host')
        port = int(self._conf.get('mongo', 'port'))
        username = self._conf.get('mongo', 'username')
        password = self._conf.get('mongo', 'password')
        authDB = self._conf.get('mongo', 'authDB')
        mongoClient = MongoClient(host, port)
        mongoClient.instagram.authenticate(username, password, source=authDB)
        self.db = mongoClient['instagram']


    def countPosts(self, month, year):
        month = str(month).zfill(2)
        rules = self._rules
        reverse_rules = dict((val, key) for key, val in rules.iteritems())
        rules_tags = self._rules_tags
        max_rule_index = self._max_rule + 1
        counts_data = []

        rule_file_name = 'H:\\Data\\RawData_csv\\GNIP\\Instagram\\{0}\\{1}\\COUNTS\\rule_count_full_month_{1}.csv'.format(year, month)
        mkdir_p('\\'.join(rule_file_name.split('\\')[:-1]))
        collection_name = str(year) + str(month)
        collection = self.db[collection_name]
        start_day = 1
        _, end_day = calendar.monthrange(int(year), int(month))
        start_date = datetime.strptime(year + '/' + month + '/' + str(start_day).zfill(2), '%Y/%m/%d')
        end_date = datetime.strptime(year + '/' + month + '/' + str(end_day).zfill(2), '%Y/%m/%d')

        for i in daterange(start_date, end_date):
            day = str(i.day).zfill(2)
            print day
            query = {'su': {'$gte': start_date, '$lt': end_date}}
            for rule_index in reverse_rules.keys():
                query['mrv'] = rule_index
                rule = reverse_rules[rule_index]
                rule_count = collection.count(query)
                tag = rules_tags[rule]
                if rule_count > 0:
                    counts_data.append([rule, rule_index, tag, rule_count, day, month])

        df = pd.DataFrame(counts_data, columns=['Rule', 'Rule Index', 'Tag', 'Count', 'Day', 'Month'])
        df.to_csv(rule_file_name, index=False, quoting=QUOTE_NONNUMERIC)


    def aggregate(self):
        for i in range(1, 31):
            dayPath = self.parts.format(self.year, self.month, str(i).zfill(2))
            try:
                fileList = os.listdir(dayPath)
            except WindowsError, IOError:
                continue
            fileName = ''
            groupFile = None
            for i in fileList:
                if 'instagram_' in i and '.xml' in i:
                    if groupFile is None:
                        fileName = "_".join(i.split('-')[:-2])
                        groupFile = open(self.src + fileName + '.xml', 'w')
                    with open(dayPath + i) as f:
                        groupFile.write(f.read())
            groupFile.close()


    def iterateOverFiles(self):
        mkdir_p(self.dest)
        fileList = os.listdir(self.src)
        for i in fileList:
            if len(i.split('_')) == 4:
                print i
                data = self.getData(self.src + i)
                # fileName = dest + i.split('.')[0] + '.csv'
                for i in data:
                    self.populateMongo(i)



    def getData(self, filename):
        f = open(filename)
        data = []
        line = ""
        for i in f.readlines():
            line += i
            if '</entry>' in i:
                dataLine = self.extract(line.decode('utf-8', 'ignore'))
                if dataLine is not None:
                    count = 1
                    # if isinstance(dataLine['activityobjectcategory'], list):
                    #     for i in dataLine['activityobjectcategory']:
                    #         dataLine['activityobjectcategory' + str(count)] = i
                    #         count += 1
                    # else:
                    #     dataLine['activityobjectcategory1'] = dataLine['activityobjectcategory']
                    # del(dataLine['activityobjectcategory'])
                    data.append(dataLine)
                line = ""
        f.close()
        return data


    def extract(self, line):
        soup = BeautifulSoup(line)
        data = {}
        for (key, val) in self.fields.iteritems():
            part_xml = soup.find(key)
            if isinstance(val, list):
                for i in val:
                    if isinstance(i, dict):
                        (k, v) = i.items()[0]
                        try:
                            if isinstance(v, int):
                                item = part_xml.find(k).get_text().strip()
                                newKey = key + k
                                newKey = re.sub('[:-]', '', newKey)
                                item = re.sub('[\r\n]', ' ', item)
                                data[newKey] = item
                            elif isinstance(v, dict):
                                item = [i[v.keys()[0]] for i in part_xml.find_all(k)]
                                item = item[0] if len(item) == 1 and isinstance(item, list) else item
                                newKey = key + k
                                newKey = re.sub('[:-]', '', newKey)
                                if isinstance(item, list):
                                    data[newKey] = [re.sub('[\r\n]', ' ', i) for i in item]
                                else:
                                    data[newKey] = re.sub('[\r\n]', ' ', item)
                        except AttributeError:
                            # print 'list --> dict None'
                            return None
                        except KeyError:
                            # print 'list --> dict KeyError None'
                            print k
                            print part_xml
                            return None
                    else:
                        try:
                            item = part_xml.find(i).get_text()
                            newKey = key + i
                            newKey = re.sub('[:-]', '', newKey)
                            item = re.sub('[\r\n]', ' ', item)
                            data[newKey] = item
                        except AttributeError:
                            # print 'list --> regular None'
                            return None
            elif isinstance(val, dict):
                for (k, v) in val.iteritems():
                    newKey = key + k
                    try:
                        item = soup.find(key, rel=k)['href']
                    except KeyError:
                        # print 'dict None'
                        return None
                    newKey = re.sub('[:-]', '', newKey)
                    item = re.sub('[\r\n]', ' ', item)
                    data[newKey] = item
            elif isinstance(val, str):
                item = [i.get_text() for i in part_xml.find_all(val)]
                newKey = key
                newKey = re.sub('[:-]', '', newKey)
                data[newKey] = [re.sub('[\r\n]', ' ', i) for i in item]
            else:
                try:
                    item = part_xml.get_text().strip()
                    newKey = key
                    newKey = re.sub('[:-]', '', newKey)
                    item = re.sub('[\r\n]', ' ', item)
                    data[newKey] = item
                except AttributeError:
                    # print 'Direct None'
                    return None
        return data


    def populateMongo(self, inputTweet):
        date = inputTweet['sourceupdated'].split('T')[0]
        collName = ''.join(date.split('-')[:2])
        collection = self.db[collName]
        collection.ensure_index([("mrv", ASCENDING)])
        collection.ensure_index([("mrt", ASCENDING)])

        with open(self._conf.get('instagram', 'fields_mongo')) as fieldsFile:
            fieldsToMongo = json.loads(fieldsFile.read())

        newRecord = {}
        for (key, val) in inputTweet.iteritems():
            newKey = fieldsToMongo[key]
            newRecord[newKey] = val

        newRecord['su'] = parseDateString(newRecord['su'])
        newRecord['p'] = parseDateString(newRecord['p'])
        newRecord['u'] = parseDateString(newRecord['u'])

        ruleIndex = set()
        tagIndex = set()
        if not isinstance(newRecord['mrv'], list):
            newRecord['mrv'] = newRecord['mrv'].split()
        for r in newRecord['mrv']:
            rule = r.lower()
            try:
                ruleIndex.add(self._rules[rule])
            except KeyError:
                print 'rule is ', rule
                return
            tag = self._rules_tags[rule]
            tagIndex.add(self._tags[tag])
        newRecord['mrv'] = list(ruleIndex)
        newRecord['mrt'] = list(tagIndex)

        try:
            collection.insert(newRecord)
        except errors.DuplicateKeyError:
            oldRecord = collection.find({'_id': newRecord['_id']})
            for i in oldRecord:
                mrv = set(newRecord['mrv'] + i['mrv'])
            newRecord['mrv'] = list(mrv)
            collection.save(newRecord)
        


    def backfillRawFiles(self, backfillData, rawFile):
        xmlFile = open(rawFile, 'a')
        xmlFile.write('\n')
        for i in backfillData:
            xmlFile.write(i)
        xmlFile.close()


    def processBackfill(self, backfillFile):
        print backfillFile.split('\\')[-1]
        raw_file = self._conf.get('instagram', 'backfill_src')
        csv_file = self._conf.get('instagram', 'backfill_dest')
        f = open(backfillFile)
        line = ""
        rawData = {}
        processedData = {}
        date = ""

        for i in f.readlines():
            line += i
            if '</entry>' in i:
                dataLine = self.extract(line)
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
        f.close()

        for (key, val) in processedData.iteritems():
            y, m, d = key.split('-')
            xmlFileName = raw_file.format(y, m, d)
            csvFileName = csv_file.format(y, m, d)
            print xmlFileName
            print csvFileName

            if not os.path.exists(os.path.dirname(xmlFileName)):
                os.makedirs(os.path.dirname(xmlFileName))
            if not os.path.exists(os.path.dirname(csvFileName)):
                os.makedirs(os.path.dirname(csvFileName))

            self.backfillRawFiles(rawData[key], xmlFileName)
            df = pd.DataFrame(processedData[key])
            with open(csvFileName, 'a') as csvfile:
                df.to_csv(csvfile, sep=',', index=False, header=False)

        print backfillFile + ' is done'
