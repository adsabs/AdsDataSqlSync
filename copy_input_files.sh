set -e

INPUT_BASE=/proj/ads/abstracts/config/
TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
OUTPUT_BASE=./logs/input/input.$TIMESTAMP/
MIN_LINES=2400

# Delete old input files
if [ -d ./logs/input ]; then
    find ./logs/input/ -name "input.20*-*-*_*-*-*" -type d -mtime +7 -exec rm -rf '{}' \;
fi

for FILE in bibcodes.list.can \
		links/facet_authors/all.links \
		links/citation/all.links \
		links/reads/downloads.links \
		links/grants/all.links \
		links/ned/ned_objects.tab \
		links/nonarticle/all.links \
		links/ocr/all.links \
		links/private/all.links \
		links/openaccess/pub.dat \
		links/alsoread_bib/all.links \
		links/reads/all.links \
		links/refereed/all.links \
		links/reference/all.links \
		links/relevance/docmetrics.tab \
		links/simbad/simbad_objects.tab \
		links/facet_datasources/datasources.links \
		links/electr/all.links \
		links/eprint_html/all.links \
		links/pub_pdf/all.links \
		links/ads_pdf/all.links \
		links/eprint_pdf/all.links \
		links/author_html/all.links \
		links/author_pdf/all.links \
		links/ads_scan/all.links \
		links/associated/all.links \
		links/video/all.links \
		links/library/all.links \
		links/spires/all.links \
		links/toc/all.links ; do

    mkdir -p $(dirname "$OUTPUT_BASE$FILE")
    cp -v $INPUT_BASE$FILE $OUTPUT_BASE$FILE
    if [ $(wc -l < $OUTPUT_BASE$FILE) -lt ${MIN_LINES} ]; then
	echo "ERROR: File $OUTPUT_BASE$FILE has less than ${MIN_LINES} lines, processing aborted"
	exit 1
    fi
done

# ingest code expects latest files in directory named current
ln -fs $PWD/logs/input/$OUTPUT_BASE ./logs/input/current
