
"""
ingest code invoked by postgres copy from program command
it outputs column file data to standard for Postgres ingest
"""
import sys
import re
import argparse
from sqlalchemy.orm import sessionmaker
from adsputils import load_config, setup_logging



class ColumnFileReader:
    """iterator for data in column files that combines values for repeated bibcodes
       also works with column files that combine all data for bibcode on a single line
    """

    def __init__(self, path, as_array=False):
        """ use as_array to indicate the value for each line should be stored as an array """
        self.file_descriptor = open(path, 'r')
        self.bibcode_length = 19
        self.as_array = as_array
        self.regex = re.compile("^[0-9 \t]+$")

    def __iter__(self):
        return self

    def next(self):
        """ read next line and all following lines with the same bibcode"""
        line = self.file_descriptor.readline()
        if len(line) == 0:
            self.file_descriptor.close()
            raise StopIteration

        if ('\t' in line):
            bibcode = line.split('\t')[0]
        else:
            bibcode = line[:self.bibcode_length]
        value = line[len(bibcode)+1 : -1]
        if (self.as_array):
            if self.regex.match(value):
                # here on array of numbers, like reads or downloads, not grants
                value = value.split('\t')
            else:
                value = [value]
        
        # if bibcode on current line matches next line, combine values
        match = self._bibcode_match(bibcode)
        if match and isinstance(value, str):
            value = [value]
        while match:
            line = self.file_descriptor.readline()
            value.append(line[self.bibcode_length+1:-1])
            match = self._bibcode_match(bibcode)
        return bibcode, value

    def _bibcode_match(self, bibcode):
        """ peek ahead to next line for bibcode and check for mactch"""
        file_location = self.file_descriptor.tell()
        next_line = self.file_descriptor.readline()
        self.file_descriptor.seek(file_location)
        next_bib = next_line[:self.bibcode_length]
        if bibcode == next_bib:
            return True
        return False


class ColumnFileIngester:

    #all_types = ('canonical', 'author', 'refereed', 'simbad', 'grants', 'citation', 'relevance',
    #                  'reader', 'download', 'reference', 'reads', 'ned')

    def __init__(self, passed_config=None):
        """ passed_config available for test code """
        self.config = {}
        self.config.update(load_config())
        if passed_config:
            self.config.update(passed_config)
        self.logger = setup_logging('AdsDataSqlSync', 'INFO')

        # which lists the types are in controls how they are processed
        # as_array: should values be read in as an array and output to sql as an array
        #  for example, downloads and grants are an array while relevance has several distinct values but isn't an array
        self.array_types = ('download', 'reads', 'author', 'reference', 'grants', 'citation', 'reader', 'simbad', 'ned')
        # quote_value: should individual values sent to sql be in quotes.  
        #  for example, we don't quote bibcode, but we do names of authors
        self.quote_values = ('author','simbad','grants','ned')
        # tab_separator: is the tab a separator in the input data
        self.tab_separated_values = ('author')



    def process_file(self, passed_type):
        """process the column file for the provided type
        type to file name is specified in config file
        """
        as_array = passed_type in self.array_types
        quote_value = passed_type in self.quote_values
        tab_separator = passed_type in self.tab_separated_values
        count = 0
        max_rows = self.config['MAX_ROWS']
        filename = self.config['DATA_PATH'] + self.config[passed_type.upper()]
        with open(filename) as f:
            reader = ColumnFileReader(filename, as_array)
            for bibcode, value in reader:
                count += 1
                if max_rows > 0 and count > max_rows:
                    break
                clean_bibcode = bibcode.strip()
                if len(bibcode) == 0 or ' ' in clean_bibcode or '\t' in clean_bibcode:
                    self.logger.error('columnFileIngest read invalid bibcode {} at line {}'.format(clean_bibcode, count))
                    continue  # we have seen a bad line in the file
                if passed_type == 'canonical':
                    value = str(count)
                elif (passed_type == 'refereed'):
                    value = 'T'
                else:
                    value = self.process_value(value, as_array, quote_value, tab_separator)
                print clean_bibcode + '\t' + value
                if count % 100000 == 0:
                    self.logger.debug('columnFileIngest processing {}, count = {}'.format(passed_type, str(count)))

        self.logger.info('columnFileIngest, processed {}, contained {} lines'.format(filename, count))


    def process_value(self, value, as_array=False, quote_value=False, tab_separator=False):
        """ convert value to what Postgres will accept """
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
                if quote_value:
                    v = '"' + v + '"'
                if len(return_value) == 0:
                    return_value = v
                else:
                    return_value += output_separator + v
                
        elif isinstance(value, list):
            # array of values to conver to sql 
            for v in value:
                v = v.replace('\t', ' ')
                if quote_value:
                    v = '"' + v + '"'
                if len(return_value) == 0:
                    return_value = v 
                else:
                    return_value += output_separator + v

        elif isinstance(value, str):
            if quote_value:
                return_value = '"' + value + '"'
            else:
                return_value = value
    
        if as_array:
            # postgres array are contained within curly braces
            return_value = '{' + return_value + '}'
        return return_value

    def verify_file(self, passed_type, schema_name):
        """read lines in file and compare to rows in postgres 
        uses line combining ReadColumnFile"""
        sql_sync = SqlSync(schema_name)
        Session = sessionmaker()
        sess = Session(bind=sql_sync.connection)
        sql_table = sql_sync.get_table(passed_type)
        sql_count = sess.query(sql_table).count()
        filename = self.config['DATA_PATH'] + self.config[passed_type.upper()]
        file_count = self.count_lines_file(filename)
        print sql_count, file_count, filename
        if file_count == sql_count:
            self.logger.info('verify on {} succeeded, {} rows'.format(passed_type, sql_count))
        else:
            self.logger.info('verify on {} failed, {} sql rows, {} file rows'.format(passed_type, sql_count, file_count))



    def count_lines_file(self,filename):
        count = 0
        with open(filename, "r") as f:
            reader = ColumnFileReader(filename)
            for bibcode, value in reader:
                count += 1
        return count

            
