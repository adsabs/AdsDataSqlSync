#!/bin/bash

echo $(date);
for i in `seq 1 10000 11520000`; do
    echo $i;
    psql -h localhost -U postgres -f loadMetricsData.sql -v pythonFilename=/SpacemanSteve/code/cfa/AdsDataSqlSync/AdsDataSqlSync/metrics/metrics.py -v metricsSchema=metrics -v startOffset=$i -v endOffset=$((i + 10000)) -v rowViewSchema=ingest;

done
echo $(date);
