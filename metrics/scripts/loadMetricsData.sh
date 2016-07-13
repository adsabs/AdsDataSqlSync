#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo 'compute metrics database from row view data'
    echo 'usage: loadMetricsData sqlSyncSchemaName metricsSchemaName dataPath pythonPath'
    exit 1
fi

echo $(date);
num_bibcodes="$(wc -l $3/bibcodes.list.can | cut -f 1 -d ' ')"

for i in `seq 1 10000 $num_bibcodes`; do
    echo $i;
    echo psql -h localhost -U postgres -f loadMetricsData.sql -v pythonFilename=$4/metrics/metrics.py -v metricsSchema=$2 -v startOffset=$i -v endOffset=$((i + 10000)) -v rowViewSchema=$1;
done

echo $(date);
