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
        
