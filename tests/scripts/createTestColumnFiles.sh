#!/bin/bash

# generate a set of column files with data for the specified bibcodes
# the directory structure of the output directory matches the structure of the input data
# on adswhy, takes just under two minutes for an input file with 500 bibcodes

if [ "$#" -ne 3 ]; then
    echo 'create a set of column files containing data for the specified bibcodes'
    echo 'usage: createTestColumnFiles.sh fileOfTestBibcodes pathToInputColumnFiles pathToOutputDirectory'
    exit 1
fi

mkdir -p $3/alsoread_bib
mkdir -p $3/citation
mkdir -p $3/facet_authors
mkdir -p $3/grants
mkdir -p $3/reads
mkdir -p $3/refereed
mkdir -p $3/reference
mkdir -p $3/relevance
mkdir -p $3/simbad
mkdir -p $3/ned


grep -F -f $1 $2/bibcodes.list.can > $3/bibcodes.list.can.tmp
# also pick up papers that cite the given bibcodes so metrics can be computed
grep -F -f $1 $2/reference/all.links | cut -f 1 >> $3/bibcodes.list.can.tmp
sort --unique $3/bibcodes.list.can.tmp > $3/bibcodes.list.can
rm $3/bibcodes.list.can.tmp

grep -F -f $3/bibcodes.list.can $2/alsoread_bib/all.links > $3/alsoread_bib/all.links
grep -F -f $3/bibcodes.list.can $2/citation/all.links > $3/citation/all.links
grep -F -f $3/bibcodes.list.can $2/facet_authors/all.links > $3/facet_authors/all.links
grep -F -f $3/bibcodes.list.can $2/grants/all.links > $3/grants/all.links
grep -F -f $3/bibcodes.list.can $2/reads/all.links > $3/reads/all.links
grep -F -f $3/bibcodes.list.can $2/reads/downloads.links > $3/reads/downloads.links
grep -F -f $3/bibcodes.list.can $2/refereed/all.links > $3/refereed/all.links
grep -F -f $3/bibcodes.list.can $2/reference/all.links > $3/reference/all.links
grep -F -f $3/bibcodes.list.can $2/relevance/docmetrics.tab > $3/relevance/docmetrics.tab
grep -F -f $3/bibcodes.list.can $2/simbad/simbad_objects.tab > $3/simbad/simbad_objects.tab
grep -F -f $3/bibcodes.list.can $2/ned/ned_objects.tab > $3/ned/ned_objects.tab
