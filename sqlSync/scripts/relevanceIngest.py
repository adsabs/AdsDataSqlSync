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
        values = value.split('\t')
        boost = float(values[0])
        citation_count = int(values[1])
        read_count = int(values[2])
        norm_cites = int(values[3])        

        print bibcode + '\t' + str(boost) + '\t' + str(citation_count) + '\t' + str(read_count) + '\t' + str(norm_cites)

