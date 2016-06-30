import sys

class ColumnFileReader:
    """iterator for data in column files"""
    def __init__(self, path):
        self.file_descriptor = open(path, 'r')
        self.bib_code_length = 19

    def __iter__(self):
        return self

    def next(self):
        line = self.file_descriptor.readline()
        if len(line) == 0:
            self.file_descriptor.close()
            raise StopIteration

        bib_code = line[:self.bib_code_length]
        value = line[self.bib_code_length+1 : -1]
        match = self._bib_code_match(bib_code)
        if match:
            value = [value]
        while match:
            line = self.file_descriptor.readline()
            value.append(line[self.bib_code_length+1:-1])
            match = self._bib_code_match(bib_code)
        # print 'returning', bib_code,
        return bib_code, value

    def _bib_code_match(self, bib_code):
        file_location = self.file_descriptor.tell()
        next_line = self.file_descriptor.readline()
        self.file_descriptor.seek(file_location)
        next_bib = next_line[:self.bib_code_length]
        if bib_code == next_bib:
            return True
        return False

bibcode_length = 19

reader = ColumnFileReader(sys.argv[1])
for bibcode, values in reader:
    if isinstance(values, basestring):
        values = [values]
    sql_value = ''
    for value in values:
        value = value.replace('\t', ' ')
        if len(sql_value) == 0:
            sql_value = '"' + value + '"'
        else:
            sql_value += ',' + '"' + value + '"' 
    sql_value = '{' + sql_value + '}'
    print bibcode + '\t' + sql_value


