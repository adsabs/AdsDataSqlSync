import sys
bibcode_length = 19

id = 1
with open(sys.argv[1]) as f:
    for line in f:
        bibcode = line[:bibcode_length]
        print str(id) + '\t' + bibcode
        id += 1

