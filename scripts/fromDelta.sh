
if [ "$#" -ne 5 ]; then
    echo 'create database schema, load column files, create row view and populate metrics database'
    echo 'usage: fromDelta sqlSyncSchemaName metricsSchemaName dataPath pythonPath baselineSchema'
    exit 1
fi

# create database tables and load column files
echo "creating database tables"
#psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/createSqlSyncTables.sql
#psql -h localhost -U postgres -v rowViewSchema=$1 -v dataRoot=$3 -v pythonRoot=$4 -f ../sqlSync/scripts/loadColumnFiles.sql

echo "loading column files"
#psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/loadColumnFiles.sql

# create row view
echo "creating row view"
#psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/createSqlSyncRowView.sql

echo "creating changed view"
psql -h localhost -U postgres -v v1=$1 -v v2=$5  -f ../sqlSync/scripts/createSqlSyncChangedView.sql

#psql -h localhost -U postgres -v v1=$2 -f ../metrics/scripts/createMetricsTable.sql

# update all bibcodes in metrics database
#python ../metrics/metrics.py metricsCompute -rowViewSchema $1 -metricsSchema $2 --deltaSchema $4
