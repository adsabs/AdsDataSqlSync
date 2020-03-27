#!/bin/bash
set -e
# test nonbib data ingest using historical data
# - data1 directory contains historical data
# - data2 directory is a copy of data1 with some changes
#   changed bibcodes:
#   - 1057wjlf.book.....C
#   - 1905PhRvI..21..247N
#   - 1950RPPh...13...24G
#   - 2008arXiv0802.0143H

echo "Do you wish to remove the test schemas and their content? (recommended: Yes)"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) python ../run.py dropIngestTables --rowViewSchemaName nonbibtest2 && python ../run.py dropIngestTables --rowViewSchemaName baselinetest2; break;;
        No ) exit;;
    esac
done
echo -e "\n"

export DATA_PATH='./data/data1/'

echo "Proceed to load 'data1' and execute a delta with itself? (recommended: Yes)"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) python ../run.py --rowViewSchemaName nonbibtest2 runRowViewPipeline && python ../run.py --rowViewSchemaName nonbibtest2 --rowViewBaselineSchemaName baselinetest2 runRowViewPipelineDelta; break;;
        No ) exit;;
    esac
done

echo ">> The above number of differences should all be 0"
echo -e "\n"

export DATA_PATH='./data/data2/'

echo "Proceed to load 'data2' and execute a delta with 'data1'? (recommended: Yes)"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) python ../run.py --rowViewSchemaName nonbibtest2 --rowViewBaselineSchemaName baselinetest2 runRowViewPipelineDelta; break;;
        No ) exit;;
    esac
done

echo ">> the above number of differences should be 4 for bibcodes and 1 for everything else"
echo -e "\n"

echo "Do you wish to remove the test schemas and their content? (recommended: Yes)"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) python ../run.py dropIngestTables --rowViewSchemaName nonbibtest2&& python ../run.py dropIngestTables --rowViewSchemaName baselinetest2; break;;
        No ) exit;;
    esac
done
