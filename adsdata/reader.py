
from adsputils import setup_logging, load_config


class ADSClassicInputStream(object):
    """file like object used to read nonbib column data files

    provides a useful wrapper around python file object
    """

    def __init__(self, file_):
        self._file = file_
        self.read_count = 0   # needed for logging
        self.logger = setup_logging('AdsDataSqlSync', 'DEBUG')
        self.logger.info('nonbib file ingest, file {}'.format(self._file))
        self.config = {}
        self.config.update(load_config())

        self._iostream = open(file_, 'r')
        
        

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
    
    def __iter__(self):
        return self
    
    def next(self):
        return self._iostream.next()
    
    @classmethod
    def open(cls, file_):
        return cls(file_)

    def close(self):
        self._iostream.close()
        del self._iostream


    def read(self, size=-1):
        """called by iterators, use for column files where bibcodes are not repeated"""
        self.read_count += 1
        if self.read_count % 100000 == 0:
            self.logger.debug('nonbib file ingest, count = {}'.format(self.read_count))
            
        line = self._iostream.readline()
        if len(line) == 0 or (self.config['MAX_ROWS'] > 0 and self.read_count > self.config['MAX_ROWS']):
            self.logger.info('nonbib file ingest, processed {}, contained {} lines'.format(self._file, self.read_count))
            return ''
        return self.process_line(line)
    

    def readline(self):
        # consider changing to call read
        self.read_count += 1
        line = self._iostream.readline()
        return self.process_line(line)
    
    def process_line(self, line):
        return line
    
    
   
class BibcodeFileReader(ADSClassicInputStream):
    """add id field to bibcode"""
    
    def __init__(self, file_):
        super(BibcodeFileReader, self).__init__(file_)

        
    def process_line(self, line):
        bibcode = line[:-1]
        row = '{}\t{}\n'.format(bibcode, self.read_count)
        return row
    
 
class RefereedFileReader(ADSClassicInputStream):
    """adds default True value for reading refereed column data file"""
    def __init__(self, file_):
        super(RefereedFileReader, self).__init__(file_)
        
    def process_line(self, line):
        bibcode = line[:-1]
        row = '{}\t{}\n'.format(bibcode, 'T')
        return row
        
class StandardFileReader(ADSClassicInputStream):
    """reads most nonbib column files

    can read files where for a bibcode is on one line or on consecutive lines
    """
    def __init__(self, file_type_, file_):
        super(StandardFileReader, self).__init__(file_)
        self.file_type = file_type_
        
        # the following lists controls how they are processed

        # as_array: should values be read in as an array and output to sql as an array
        #  for example, downloads and grants are an array while relevance has several distinct values but isn't an array
        self.array_types = ('download', 'reads', 'author', 'reference', 'grants', 'citation', 'reader', 'simbad', 'ned', 'datalinks')
        # quote_value: should individual values sent to sql be in quotes.  
        #  for example, we don't quote reads, but we do names of authors
        self.quote_values = ('author','simbad','grants', 'ned', 'datalinks')
        # tab_separator: is the tab a separator in the input data, default is comma
        self.tab_separated_values = ('author', 'download', 'reads')
        
    def read(self, size=-1):
        """returns the data from the file for the next bibcode

        peeks ahead in file and concatenates data if its bibcode matches
        makes at least one and potentially multiple readline calls on iostream """
        self.read_count += 1
        if self.read_count % 100000 == 0:
            self.logger.debug('nonbib file ingest, processing {}, count = {}'.format(self.file_type, self.read_count))
        line = self._iostream.readline()
        if len(line) == 0  or (self.config['MAX_ROWS'] > 0 and self.read_count > self.config['MAX_ROWS']):
            self.logger.info('nonbib file ingest, processed {}, contained {} lines'.format(self._file, self.read_count))
            return ''

        bibcode = line[:19]
        while ' ' in bibcode or '\t' in bibcode:
            self.logger.error('invalid bibcode {} in file {}'.format(bibcode, self._file))
            line = self._iostream.readline()
            bibcode = line[:19]
        value = line[20:-1]

        # does the next line match the current bibcode?
        match = self._bibcode_match(bibcode)

        if self.file_type in (self.array_types):
            value = [value]
        while match:
            line = self._iostream.readline()
            value.append(line[20:-1])
            match = self._bibcode_match(bibcode)
        return self.process_line(bibcode, value)
    

    def readline(self):
        return self.read()

        
    def process_line(self, bibcode, value):
        as_array = self.file_type in self.array_types
        quote_value = self.file_type in self.quote_values
        tab_separator = self.file_type in self.tab_separated_values
        processed_value = self.process_value(value, as_array, quote_value, tab_separator)
        row = '{}\t{}\n'.format(bibcode, processed_value)
        return row
    
    def _bibcode_match(self, bibcode):
        """ peek ahead to next line for bibcode and check for mactch"""
        file_location = self._iostream.tell()
        next_line = self._iostream.readline()
        self._iostream.seek(file_location)
        next_bib = next_line[:19]
        if bibcode == next_bib:
            return True
        return False

        
    def process_value(self, value, as_array=False, quote_value=False, tab_separator=False):
        """convert value to what Postgres will accept"""
        if '\x00' in value:
            # postgres does not like nulls in strings
            self.logger.error('in columnFileIngest.process_value with null value in string: {}', value)
            value = value.replace('\x00', '')
    
        return_value = ''
        if tab_separator and isinstance(value, list) and len(value) == 1:
            value = value[0]
    
        output_separator = ','
        if (as_array == False):
            output_separator = '\t'
    
        if isinstance(value, str) and '\t' in value:
            # tab separator in string means we need to create a sql array
            values = value.split('\t')
            # should check for double quotes in names
            for v in values:
                if quote_value and v[0] != '"':
                    v = '"' + v + '"'
                if len(return_value) == 0:
                    return_value = v
                else:
                    return_value += output_separator + v
                
        elif isinstance(value, list):
            # array of values to conver to sql 
            for v in value:
                v = v.replace('\t', ' ')
                if quote_value and v[0] != '"':
                    v = '"' + v + '"'
                if len(return_value) == 0:
                    return_value = v 
                else:
                    return_value += output_separator + v
    
        elif isinstance(value, str):
            if quote_value and value[0] != '"':
                return_value = '"' + value + '"'
            else:
                return_value = value
    
        if as_array:
            # postgres array are contained within curly braces
            return_value = '{' + return_value + '}'
        return return_value

# for datalinks table entries that may or may not have a link_sub_type
# that includes ARTICLE types that do have sub_type and
# for example PRESENTATION, LIBRARYCATALOG, and INSPIRE	 that do not
# note that these entries do not have a title
class DataLinksFileReader(StandardFileReader):

    def __init__(self, file_type_, file_, link_type_, link_sub_type_):
        super(DataLinksFileReader, self).__init__(file_type_, file_)
        self.link_type = link_type_
        self.link_sub_type = link_sub_type_

    def process_line(self, bibcode, value):
        as_array = self.file_type in self.array_types
        quote_value = self.file_type in self.quote_values
        tab_separator = self.file_type in self.tab_separated_values
        processed_url = self.process_value(value, as_array, quote_value, tab_separator)
        row = '{}\t{}\t{}\t{}\t{}\n'.format(bibcode, self.link_type, self.link_sub_type, processed_url, "{""}")
        return row

# for datalinks table entries that have titles, but no link_sub_type
# right now only link_type = ASSOCIATED belongs to this category
class DataLinksWithTitleFileReader(StandardFileReader):

    def __init__(self, file_type_, file_, link_type_):
        super(DataLinksWithTitleFileReader, self).__init__(file_type_, file_)
        self.link_type = link_type_

    def split(self, value):
        # value is a list of strings with two fields,
        # find the first space and split on that
        # create two lists of url and titles and return them
        urlList = []
        titleList = []
        for v in value:
            [url, title] = v.split(' ', 1)
            urlList.append(url)
            titleList.append(title)
        return urlList, titleList

    def process_line(self, bibcode, value):
        as_array = self.file_type in self.array_types
        quote_value = self.file_type in self.quote_values
        tab_separator = self.file_type in self.tab_separated_values
        [urlList, titleList] = self.split(value)
        processed_url = self.process_value(urlList, as_array, quote_value, tab_separator)
        processed_title = self.process_value(titleList, as_array, quote_value, tab_separator)
        row = '{}\t{}\t{}\t{}\t{}\n'.format(bibcode, self.link_type, "", processed_url, processed_title)
        return row

# for datalinks table entries that have may or may not have title but they do have link_sub_type
# that we are calling target, right now only link_type = DATA belongs to this category
class DataLinksWithTargetFileReader(StandardFileReader):

    def __init__(self, file_type_, file_, link_type_):
        super(DataLinksWithTargetFileReader, self).__init__(file_type_, file_)
        self.link_type = link_type_

    def split(self, value):
        # SIMBAD	1	http://$SIMBAD$/simbo.pl?bibcode=1907ApJ....25...59C	SIMBAD Objects (1)
        # value is a list of strings with four elements,
        # split on tab, the second item is not useful for us at this time
        # create 3 lists and return them
        urlList = []
        titleList = []
        targetList = []
        for v in value:
            [target, notUsed, url, title] = v.split('\t')
            urlList.append(url)
            titleList.append(title)
            targetList.append(target)
        return urlList, titleList, targetList

    def process_line(self, bibcode, value):
        as_array = self.file_type in self.array_types
        quote_value = self.file_type in self.quote_values
        tab_separator = self.file_type in self.tab_separated_values
        [urlList, titleList, targetList] = self.split(value)
        processed_url = self.process_value(urlList, as_array, quote_value, tab_separator)
        # if no title
        if (len(''.join(titleList)) > 0):
            processed_title = self.process_value(titleList, as_array, quote_value, tab_separator)
        else:
            processed_title = '{}'
        processed_target = ''.join(targetList)
        row = '{}\t{}\t{}\t{}\t{}\n'.format(bibcode, self.link_type, processed_target, processed_url, processed_title)
        return row
