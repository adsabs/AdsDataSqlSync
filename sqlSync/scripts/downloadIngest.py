import sys
bibcode_length = 19

#with open('/Users/SpacemanSteve/tmp/columnFiles/facet_authors/sample100.links') as f:
with open(sys.argv[1]) as f:
    for line in f:
        bibcode = line[:bibcode_length]
        if ' ' in bibcode or '\t' in bibcode: 
            continue  # we have seen a bad line in the file
        values = line[bibcode_length + 1:-1] # skip tab at column 19 and new line at end
        sql_value = ''
        values = values.split('\t')
        # should check for double quotes in names
        for value in values:
            if len(sql_value) == 0:
                sql_value = value;
            else:
                sql_value += ',' + value;

        sql_value = '{' + sql_value + '}'
        print bibcode + '\t' + sql_value

