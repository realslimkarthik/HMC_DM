import sys
import os

path = "C:\Users\kharih2\Work\DM_Karthik\HMC_Data\\"
platform = {'tw':{'dirName': "TwitterPowerTrack", 'format': "json"}, 
            'fb': {'dirName': "Facebook", 'format': "json"}
            }

def fixFilesbyDir(path, openBracket, closeBracket):
    files = os.listdir(path)
    for i in range(len(files)):
        try:
            f1 = open(files[i], 'w')
            f2 = open(files[i + 1], 'r')
            contentsf1 = f1.readlines()
            for j in contentsf1[-1]:
                if i == openBracket:
                    counter += 1
                elif i == closeBracket:
                    counter -= 1
            if counter != 0:
                contentsf2 = f2.readlines()
                for j in contentsf2[0]:
                    if i == openBracket:
                        counter += 1
                    elif i == closeBracket:
                        counter -= 1
                if counter != 0:
                    f1.write(contentsf2[0])
            f1.close()
            f2.close()
        except IndexError:
            # Repeat for the next dir
            print "Hello"
            f1.close()
            return path + files[i]

def fixFilesWrapper(path):
    

if __name__ == "__main__":
    sys.argv[1]
    path += platform[sys.argv[1]]['dirName'] + "\September-2014-Master\\"
    #print os.listdir(path)
    lastFile = fixFilesbyDir(path, '{', '}')
    print lastFile
    