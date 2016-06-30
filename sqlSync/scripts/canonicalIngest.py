import sys
bibcode_length = 19

with open(sys.argv[1]) as f:
    for line in f:
        bibcode = line[:bibcode_length]
        print bibcode

