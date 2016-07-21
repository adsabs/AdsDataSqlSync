
import sys
import re
import argparse
import utils


config = {}
logger = None

class ColumnFileReader:
    """iterator for data in column files that combines values for repeated bibcodes"""

    def __init__(self, path, as_array=False):
        self.file_descriptor = open(path, 'r')
        self.bibcode_length = 19
        self.as_array = as_array
        self.regex = re.compile("^[0-9 \t]+$")

    def __iter__(self):
        return self

    def next(self):
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

        match = self._bibcode_match(bibcode)
        if match and isinstance(value, str):
            value = [value]
        while match:
            line = self.file_descriptor.readline()
            value.append(line[self.bibcode_length+1:-1])
            match = self._bibcode_match(bibcode)
        return bibcode, value

    def _bibcode_match(self, bibcode):
        file_location = self.file_descriptor.tell()
        next_line = self.file_descriptor.readline()
        self.file_descriptor.seek(file_location)
        next_bib = next_line[:self.bibcode_length]
        if bibcode == next_bib:
            return True
        return False

def init(passed_config=None):
    """ passed_config available for test code """
    config.update(utils.load_config())
    if passed_config:
        config.update(passed_config)

    global logger
    logger = utils.setup_logging(config['LOG_FILENAME'], 'ColumnFileIngest', config['LOGGING_LEVEL'])


def process_file(passed_type, as_array=False, quote_value=False, tab_separator=False):
    """procesed the column file for the provided type
    other parameters indicate how to process the data
    as_array: should values be read in as an array and output to sql as an array
      for example, downloads and grants are an array while relevance has several distinct values but isn't an array
    quote_value: should individual values sent to sql be in quotes.  
      for example, we don't quote bibcode, but we do names of authors
    tab_separator: is the tab a separator in the input data
      for example, yes for authors, no for grants
    """
    count = 0
    max_rows = config['MAX_ROWS']
    filename = config['DATA_PATH'] + config[passed_type.upper()]
    with open(filename) as f:
        reader = ColumnFileReader(filename, as_array)
        for bibcode, value in reader:
            count += 1
            if max_rows > 0 and count > max_rows:
                break
            clean_bibcode = bibcode.strip()
            if len(bibcode) == 0 or ' ' in clean_bibcode or '\t' in clean_bibcode:
                logger.error('columnFileIngest read invalid bibcode {} at line {}'.format(clean_bibcode, count))
                continue  # we have seen a bad line in the file
            if passed_type == 'canonical':
                value = str(count)
            elif (passed_type == 'refereed'):
                value = 'T'
            else:
                value = process_value(value, as_array, quote_value, tab_separator)
            print clean_bibcode + '\t' + value
            if count % 100000 == 0:
                logger.debug('columnFileIngest processing {}, count = {}'.format(passed_type, str(count)))

    logger.info('columnFileIngest, processed {}, contained {} lines'.format(filename, count))


def process_value(value, as_array=False, quote_value=False, tab_separator=False):
    if '\x00' in value:
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
        return_value = '{' + return_value + '}'
    return return_value

            

def main():
    parser = argparse.ArgumentParser(description='process column files into Postgres')
    parser.add_argument('--fileType', default=None, help='all,downloads,simbad,etc.')
    parser.add_argument('command', help='ingest|verify')

    array_types = ('download', 'reads', 'author', 'reference', 'grants', 'citation', 'reader', 'simbad')
    quote_values = ('author','simbad','grants')
    tab_separated_values = ('author')
    all_values = ('canonical', 'author', 'refereed', 'simbad', 'grants', 'citation', 'relevance',
                  'reader', 'download', 'reference', 'reads')
    
    args = parser.parse_args()
    init()

    logger.info('starting columnFileIngest with {}, {}'.format(args.command, args.fileType))
    
    if args.command == 'ingest' and args.fileType == 'all':
        # all is only useful for testing, sending output to the console
        for t in all_values:
            print
            print t
            process_file(t, t in array_types, t in quote_values, 
                         t in tab_separated_values)
    elif args.command == 'ingest' and args.fileType:
        process_file(args.fileType, args.fileType in array_types, args.fileType in quote_values, 
                     args.fileType in tab_separated_values)

    logger.info('completed columnFileIngest with {}, {}'.format(args.command, args.fileType))


if __name__ == "__main__":
    main()
