import sys
bibcode_length = 19

#with open('/Users/SpacemanSteve/tmp/columnFiles/facet_authors/sample100.links') as f:
with open(sys.argv[1]) as f:
    for line in f:
        bibcode = line[:bibcode_length]
        value = line[bibcode_length + 1:-1] # skip tab at column 19 and new line at end
        if '\x00' in value:
            value = value.replace('\x00', '');
        sql_value = ''
        authors = value.split('\t')
        # should check for double quotes in names
        for author in authors:
            if len(sql_value) == 0:
                sql_value = '"' + author + '"'
            else:
                sql_value += ',' + '"' + author + '"';

        sql_value = '{' + sql_value + '}'
        print bibcode + '\t' + sql_value

