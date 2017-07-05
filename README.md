[![Build Status](https://travis-ci.org/adsabs/AdsDataSqlSync.svg)](https://travis-ci.org/adsabs/AdsDataSqlSync)
[![Coverage Status](https://coveralls.io/repos/adsabs/AdsDataSqlSync/badge.svg)](https://coveralls.io/r/adsabs/AdsDataSqlSync)

# AdsDataSqlSync
For non-publisher ADS data

This library:
  * Imports column/flat files into SQL column tables
  * Creates a row oriented view of this data using a SQL materialized view
  * Uses the row view to populate a Metrics database
  * And, will include several RabbitMQ workers for further processing

This library processes mostly non-publisher data.  This includes
citations, 90-day readershp (called alsoreads), historical readership
(called reads), simbad object ids, refereed status, downloads, etc.

Ingest operations are controled by app.py.  It supports the following
commands:
  * help
  * createIngestTables: create sql column tables
  * dropIngestTables: drop sql column tables
  * ingest: load column files into column tables
  * verify: verify rows in column files matches rows in column tables
  * createJoinedRows: create sql join across all column tables 
  * createMetricsTable: create table for metrics data
  * dropMetricsTable: drop table for metrics data
  * populateMetricsTable: write Metrics records
  * ingestMeta: execute sql copy from program statement to run ingest command for all file types
  * populateMetricsTableMeta: execute sql copy from program statement to run populateMetricsTable command

and the following arguments:
  * --fileType: used by ingest to specify column file (reads, downloads, etc.) to load.  Can be 'all'.
  * --rowViewSchemaName:
  * --metricsSchemaName:

The config file supports:
  * DATA_PATH: root directory for column files
  * INGEST_DATABASE: database connection string
  * METRICS_DATABASE: database connection string file name for each type of file: for example READS = 'reads/all.links'
  * MAX_ROWS: set to positive number to ingest only part of the column files

## Typical Usage
The following series of commands creates tables to hold the column
files, ingests the files into Postgres, creates a row view, creates
the metrics table and finally populates it.  
```
python app.py createIngestTables --rowViewSchemaName ingestc
python app.py ingestMeta --rowViewSchemaName ingestc
python app.py createJoinedRows --rowViewSchemaName ingestc
python app.py createMetricsTable --rowViewSchemaName metricsc
python app.py populateMetricsTableMeta --metricsSchemaName metricsc --rowViewSchemaName ingestc
```
all of this can be preformed with the `runPipeline` command.

We use Postgres database schemas to hold separate versions of the
data.  In the above example, data goes into the schems named IngestC
and MetricsC.  Data could be loaded into separate databases, but that
would make comparing data sets more difficult.  

The 'Meta' suffix indicates a command that runs a SQL command like: 
```
copy ingestc.author from program 'python app.py --ingest --fileType author';
```
That is, Metas invoke a Postgres SQL command that runs a
python program to obtain new rows to populate a table.  This python
program is app.py.  In this example, 'python app.py --ingest --fileType
author' will output pairs of the form bibcode, array of authors to
standard out.  Postgres will take these pairs and make new rows in the
author table.  This self-referential nature may be confusing, but
copy from program is the fastest way to bulk ingest data into Postgres.  

Python code in metrics.py reads converts each row in the materialized view
into a row in the metrics database.  

This repository also contains a script to generate test data:
test/scripts/createTestColumnFiles.sh.  It takes thress arguments.
The first argument is a file containing a list of bibcodes (one per
line).  The second argument is the directory containing all the column
files in their expected places (bibcodes.list.can at the top level,
author info in facet_authors/all.links, etc.).  The third parameter is
an output directory.  The script pulls all the data from the flat
files and puts the data in the output directory.  This includes data
for all the provided bibcodes as well as data for papers that cite
them.  The data in the output directory is laid out just like the data
from the source column files (a subset of bibcodes in
bibcodes.list.can at top level, a subset of author info in
facet_authors/all.links, etc.). The script is useful for generating a
coherent subset of data that, for example, can be used to populated a
test metrics database.   



 
