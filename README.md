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

Ingest operations are controled by app.py.  It supports the follow
commands:
    * createIngestTables: create sql column tables
    * dropIngestTables: drop sql column tables
    * ingest: load column files into column tables
    * verify: verify rows in column files matches rows in column tables
    * createJoinedRows: create sql join across all column tables 
    * createMetricsTable: create table for metrics data
    * dropMetricsTable: drop table for metrics data
    * populateMetricsTable: write Metrics records
    * ingestMeta: execute sql copy from program statement to run ingest command
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


Each column file is initially read via a Postgres command into
a separate table using the 'copy from program' command.  Code in
app.py runs this Postgres command.  That command includes another
invocation of the app.py script (with different arguments) to perform
some minor but necessary formatting of the column files.  After all
the column files are populated, a join across all the tables (via a
materialized view) creates a unified row view is available for
additional processing steps.  For example, the Python code
metrics/metrics.py reads converts each row in the materialized view
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



 
