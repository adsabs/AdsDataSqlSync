#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo 'create database schema, load column files, create row view and populate metrics database'
    echo 'usage: fromScratch sqlSyncSchemaName metricsSchemaName'
    exit 1
fi

# create database tables and load column files
psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/createSqlSyncTables.sql
psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/loadColumnFiles.sql

# create row view
psql -h localhost -U postgres -f ../sqlSync/scripts/createSqlSyncRowView.sql

psql -h localhost -U postgres -v v1=$2 -f ../metrics/scripts/createMetricsTable.sql

# update all bibcodes in metrics database
python ../metrics/metrics.py metricsCompute -rowViewSchema $1 -metricsSchema $2
