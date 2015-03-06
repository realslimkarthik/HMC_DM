import csv, codecs, cStringIO
import sys


# Set the default encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")

# Class given in Python 2.7 documentation for handling of unicode documents
class CSVUnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, **kwds):
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        '''writerow(unicode) -> None
        This function takes a Unicode string and encodes it to the output.
        '''
        self.writer.writerow([s.encode("utf-8", "ignore") for s in row])
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)