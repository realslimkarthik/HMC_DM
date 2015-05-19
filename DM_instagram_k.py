import ConfigParser
import sys
import os
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
from utility import mkdir_p

def getData(filename):
    f = open(filename)
    fields_file = open(conf_path.format("insta_fields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    data = []
    line = ""
    for i in f.readlines():
        line += i
        if '</entry>' in i:
            dataLine = extract(line.decode('utf-8', 'ignore'), fields)
            if dataLine is not None:
                count = 1
                if isinstance(dataLine['activityobjectcategory'], list):
                    for i in dataLine['activityobjectcategory']:
                        dataLine['activityobjectcategory' + str(count)] = i
                        count += 1
                else:
                    dataLine['activityobjectcategory1'] = dataLine['activityobjectcategory']
                del(dataLine['activityobjectcategory'])
                data.append(dataLine)
            line = ""
    f.close()
    return data


def extract(line, fields):
    soup = BeautifulSoup(line)
    data = {}
    for (key, val) in fields.iteritems():
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
                        print 'list --> dict None'
                        return None
                    except KeyError:
                        print 'list --> dict KeyError None'
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
                        print 'list --> regular None'
                        return None
        elif isinstance(val, dict):
            for (k, v) in val.iteritems():
                newKey = key + k
                try:
                    item = soup.find(key, rel=k)['href']
                except KeyError:
                    print 'dict None'
                    return None
                newKey = re.sub('[:-]', '', newKey)
                item = re.sub('[\r\n]', ' ', item)
                data[newKey] = item
        else:
            try:
                item = part_xml.get_text().strip()
                newKey = key
                newKey = re.sub('[:-]', '', newKey)
                item = re.sub('[\r\n]', ' ', item)
                data[newKey] = item
            except AttributeError:
                print 'Direct None'
                return None
    return data



def aggregate(year, month, conf):
    parts_src = conf.get('instagram', 'src_parts_path')
    src = conf.get('instagram', 'src_path').format(year, month)
    for i in range(1, 10):
        dayPath = parts_src.format(year, month, str(i).zfill(2))
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
                    groupFile = open(src + fileName + '.xml', 'w')
                with open(dayPath + i) as f:
                    groupFile.write(f.read())
        groupFile.close()


if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read('config\\config.cfg')
    conf_path = conf.get('conf', 'conf_path')
    # aggregate(year, month, conf)
    src = conf.get('instagram', 'src_path').format(year, str(month).zfill(2))
    dest = conf.get('instagram', 'dest_path').format(year, str(month).zfill(2))
    mkdir_p(dest)
    fileList = os.listdir(src)
    for i in fileList:
        if len(i.split('_')) == 4:
            print i
            data = getData(src + i)
            df = pd.DataFrame(data)
            fileName = dest + i.split('.')[0] + '.csv'
            with open(fileName, 'w') as csvfile:
                df.to_csv(csvfile, sep=',', index=False)
