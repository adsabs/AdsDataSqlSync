set -e

INPUT_BASE=/proj/ads/abstracts/config/
TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
OUTPUT_BASE=./logs/input/input.$TIMESTAMP/

# path and min sizes for files, size is set to ~90% of current size
FILES_INFO=(
    bibcodes.list.can:13000000
    links/facet_authors/all.links:13000000
    links/citation/all.links:110000000 
    links/reads/downloads.links:13000000 
    links/grants/all.links:50000 
    links/ned/ned_objects.tab:4500000 
    links/nonarticle/all.links:1500000
    links/ocr/all.links:21000 
    links/private/all.links:56000 
    links/openaccess/pub.dat:900000 
    links/alsoread_bib/all.links:7000000 
    links/reads/all.links:12000000 
    links/refereed/all.links:9000000 
    links/reference/all.links:13000000 
    links/relevance/docmetrics.tab:7500000 
    links/simbad/simbad_objects.tab:18000000 
    links/facet_datasources/datasources.links:570000 
    links/electr/all.links:9500000 
    links/eprint_html/all.links:2000000 
    links/pub_pdf/all.links:1000000 
    links/ads_pdf/all.links:600000 
    links/eprint_pdf/all.links:2200000 
    links/author_html/all.links:3500 
    links/author_pdf/all.links:3500 
    links/ads_scan/all.links:60000 
    links/associated/all.links:400000 
    links/video/all.links:2200 
    links/library/all.links:11000 
    links/spires/all.links:400000 
    links/toc/all.links:1000000
)

# Delete old input files
if [ -d ./logs/input ]; then
    find ./logs/input/ -name "input.20*-*-*_*-*-*" -type d -mtime +7 -exec rm -rf '{}' \;
fi

# create local copies of files
echo hey $FILES_INFO
for FILE_INFO in ${FILES_INFO[@]} ; do
    FILE=${FILE_INFO%%:*}
    mkdir -p $(dirname "$OUTPUT_BASE$FILE")
    echo INFO: `date` copying $INPUT_BASE$FILE to $OUTPUT_BASE$FILE
    cp -v $INPUT_BASE$FILE $OUTPUT_BASE$FILE
done

# validate local files
for FILE_INFO in ${FILES_INFO[@]} ; do
    FILE=${FILE_INFO%%:*}
    MIN_LINES=${FILE_INFO##*:}
    echo INFO: `date` validating $OUTPUT_BASE$FILE is at least $MIN_LINES lines long
    if [ $(wc -l < $OUTPUT_BASE$FILE) -lt ${MIN_LINES} ]; then
	echo "ERROR: file $OUTPUT_BASE$FILE has less than ${MIN_LINES} lines, processing aborted"
	exit 1
    fi
done

# ingest code expects latest files in directory named current
echo INFO: `date` linking $PWD/logs/input/current to $PWD/$OUTPUT_BASE
rm -fv ./logs/input/current
ln -fsv $PWD/$OUTPUT_BASE $PWD/logs/input/current
