import os
import sys
import ConfigParser

def aggregateByDay(year, conf):
    src_parts_path = conf.get("youtube", "prod_src_parts_path")
    src_path = conf.get("youtube", "prod_src_path")
    for i in range(7, 13):
        for j in range(1, 32):
            parts_src = src_parts_path.format(year, str(i).zfill(2), str(j).zfill(2))
            src = src_path.format(year, str(i).zfill(2))
            try:
                fileList = os.listdir(parts_src)
            except IOError, WindowsError:
                continue
            curr_file = None
            fileList = sorted(fileList)
            for j in fileList:
                if "error" in j:
                    continue
                fileName = '-'.join(j.split('-')[:-2])
                if curr_file is None:
                    curr_file = open(src + fileName + ".xml", 'w')
                currFileName = curr_file.name.split('.')[0].split('\\')[-1]
                print fileName
                if currFileName == fileName:
                    partFile = open(parts_src + j, 'r')
                    for j in partFile.readlines():
                        curr_file.write(j)
                    partFile.close()
                else:
                    curr_file.close()
                    curr_file = None
    

if __name__ == "__main__":
    year = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read("config\config.cfg")
    conf_path = conf.get("conf", "conf_path")
    aggregateByDay(year, conf)