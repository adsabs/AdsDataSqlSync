
# test nonbib data ingest using historical data

# data1 directory contains historical data
# data2 directory is a copy of data1 with some changes

# de novo ingest data1 to use as baseline
# delta ingest data2 and verify sql table of changed bibcodes is correct

# de novo
python ../app.py --dataPath `pwd`/data/data1/ --rowViewSchemaName nonbibtest2 runRowViewPipeline

# delta with same data, should be no differences
python ../app.py --dataPath `pwd`/data/data1/ --rowViewSchemaName nonbibtest2 --rowViewBaselineSchemaName baselinetest2 runRowViewPipelineDelta

echo the above number of differences should all be 0
echo -e "\n"

# delta with slightly different data
python ../app.py --dataPath `pwd`/data/data2/ --rowViewSchemaName nonbibtest2 --rowViewBaselineSchemaName baselinetest2 runRowViewPipelineDelta
echo the above number of differences should be 3 for bibcodes and 1 for everything else

# verify table of changed bibcodes is correct

# finally, cleanup
#python../app.py --dropIngestTables --rowViewSchemaName nonbibtest2
#python../app.py --dropIngestTables --rowViewSchemaName baselinetest2
