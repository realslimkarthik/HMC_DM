import ConfigParser
import sys
import os
import pandas as pd
import re


def getData(filename):
    fileType = getFileType(filename)
    f = open(filename)
    fields_file = open(conf_path.format("insta_fields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    data = []
    line = ""
    for i in f.readlines():
        if '</entry>' in i:
            line += i
            dataLine = extract(line, fields[fileType])
            if dataLine is not None:
                data.append(dataLine)
            line = ""
        else:
            line += i
    f.close()
    return data


def extract(line, fields):
    soup = BeautifulSoup(line)
    data = {}
    for (key, val) in fields.iteritems():
        part_xml = soup.find(key)
        if isinstance(val, list):
            for (k, v) in val:
                try:
                    if isinstance(v, int):
                        item = part_xml.find(k).get_text().strip()
                        newKey = key + k
                        newKey = re.sub(':-', '', newKey)
                        item = re.sub('\r\n', ' ', item)
                        data[newKey] = item
                    elif isinstance(v, dict):
                        item = part_xml.find_all(k)[v.keys()[0]]
                        newKey = key + k
                        newKey = re.sub(':-', '', newKey)
                        data[newKey] = [re.sub('\r\n', ' ', i) for i in item]
                except AttributeError:
                    return None
        elif isinstance(val, dict):
            for (k, v) in val.iteritems():
                newKey = key + k
                try:
                    item = soup.find(key, rel=k)['href']
                except KeyError:
                    return None
                newKey = re.sub(':-', '', newKey)
                item = re.sub('\r\n', ' ', item)
                data[newKey] = item
        else:
            try:
                item = part_xml.get_text().strip()
                newKey = key
                newKey = re.sub(':-', '', newKey)
                item = re.sub('\r\n', ' ', item)
                data[newKey] = item
            except AttributeError:
                data = None
                break
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
    aggregate(year, month, conf)
    src = conf.get('instagram', 'src_path').format(year, str(month).zfill(2))
    dest = conf.get('instagram', 'dest_path').format(year, str(month).zfill(2))
    fileList = os.listdir(src)
    # for i in fileList:
    #     if len(i.split('_')) == 4:
    #         data = getData(src + i)
    #         df = pd.DataFrame(data)
    #         fileName = i.split('.')[0] + '.csv'
    #         with open(fileName) as csvfile:
    #             df.to_csv(csvfile, sep=',', index=False)

