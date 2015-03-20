import sys
import ConfigParser
import json
import os
from bs4 import BeautifulSoup
from CSVUnicodeWriter import CSVUnicodeWriter

def getData(filename):
    f = open(filename)
    fields_file = open(conf_path.format("youtubeFields.json"))
    fields = json.loads(fields_file.read())
    fields_file.close()
    data = []
    line = ""
    for i in f.readlines():
        if '</entry>' in i:
            line += i
            counts = extract(line, fields)
            line = ""
        else:
            line += i
    f.close()
    return data


def extract(line, fields):
    soup = BeautifulSoup(line)
    data = {}
    for (key, val) in fields:
        part_xml = soup.find(key)
        if isinstance(val, list):
            for j in val:
                



if __name__ == "__main__":
    op = sys.argv[1]
    year = sys.argv[2]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    src_path = conf.get("youtube", "prod_src_path")
    dest_path = conf.get("youtube", "prod_dest_path")