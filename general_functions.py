import xlrd, mmap

'''
Parses an XLS or XLSX file into a dictionary with the first row as information
@return dictionary generator of dictionary row by row
'''
def XLSDictReader(filename, sheet_index=0):
    f = open(filename, 'rb')
    book    = xlrd.open_workbook(file_contents=mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ))
    sheet   = book.sheet_by_index(sheet_index)
    headers = dict( (i, sheet.cell_value(0, i) ) for i in range(sheet.ncols) ) 
    return (dict( (headers[j], sheet.cell_value(i, j)) for j in headers ) for i in range(1, sheet.nrows))


'''
Parses a CSV file into a dictionary with the first row as information
@return dictionary generator of dictionary row by row
'''
def CSVDictReader(filename):
    import csv
    return csv.DictReader(open(filename, 'rb'), delimiter=',', quotechar='"')
        


from collections import deque
 
##        sma = simplemovingaverage(period)
##        sma(i)  returns average
class Simplemovingaverage():
    def __init__(self, period):
        assert period == int(period) and period > 0, "Period must be an integer >0"
        self.period = period
        self.stream = deque()
 
    def __call__(self, n):
        stream = self.stream
        stream.append(n)    # appends on the right
        streamlength = len(stream)
        if streamlength > self.period:
            stream.popleft()
            streamlength -= 1
        if streamlength == 0:
            average = 0
        else:
            average = sum( stream ) / streamlength
 
        return average