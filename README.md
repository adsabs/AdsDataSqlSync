# AdsDataSqlSync
For non-publisher ADS data

This library:
    * Imports column/flat files into SQL column tables
    * Creates a row oriented view of this data using a SQL materialized view
    * Uses the row view to populate a Metrics database
    * And, will include several RabbitMQ workers for further processing

This library processes mostly data that does not come from publishers.
This includes citations, 90-day readershp (called alsoreads),
historical readership (called reads), simbad object ids, refereed
status, downloads, etc.




 