
create or replace function process_file(schema_table text, python_filename text, data_filename text) returns void as $$
declare
    python_command text := 'python ' || python_filename || ' ingest ' || datafilename;

begin
    execute 'copy ' || schema_table || ' from program ' || quote_literal(python_command);
end;
$$ LANGUAGE plpgsql;

create or replace function process_file2(schema_table text, python_command text) returns void as $$
declare
begin
    execute 'copy ' || schema_table || ' from program ' || quote_literal(python_command);
end;
$$ LANGUAGE plpgsql;


delete from :rowViewSchema.Canonical;
delete from :rowViewSchema.Author;
delete from :rowViewSchema.Refereed;
delete from :rowViewSchema.Simbad;
delete from :rowViewSchema.Grants;
delete from :rowViewSchema.Citation;
delete from :rowViewSchema.Relevance;
delete from :rowViewSchema.Reader;
delete from :rowViewSchema.Download;
delete from :rowViewSchema.Reads;
delete from :rowViewSchema.Reference;


\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType canonical ingest' '\''
\set schema_table '\'' :rowViewSchema '.canonical' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType author ingest' '\''
\set schema_table '\'' :rowViewSchema '.author' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType refereed ingest' '\''
\set schema_table '\'' :rowViewSchema '.refereed' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType simbad ingest' '\''
\set schema_table '\'' :rowViewSchema '.simbad' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType grants ingest' '\''
\set schema_table '\'' :rowViewSchema '.grants' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType citation ingest' '\''
\set schema_table '\'' :rowViewSchema '.citation' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType relevance ingest' '\''
\set schema_table '\'' :rowViewSchema '.relevance' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType reader ingest' '\''
\set schema_table '\'' :rowViewSchema '.reader' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType download ingest' '\''
\set schema_table '\'' :rowViewSchema '.download' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType reads ingest' '\''
\set schema_table '\'' :rowViewSchema '.reads' '\''
select process_file2(:schema_table, :python_command);

\set python_command '\'' 'python ' :pythonRoot 'app.py --fileType reference ingest' '\''
\set schema_table '\'' :rowViewSchema '.reference' '\''
select process_file2(:schema_table, :python_command);



--copy :v1.Canonical from program 'python /SpacemanSteve/tmp/code/canonicalIngest.py /SpacemanSteve/tmp/dataFiles/bibcodes.list.can';
--copy :v1.Author from program 'python /SpacemanSteve/tmp/code/authorIngest.py /SpacemanSteve/tmp/dataFiles/facet_authors/all.links';
--copy :v1.Refereed from program 'python /SpacemanSteve/tmp/code/refereedIngest.py /SpacemanSteve/tmp/dataFiles/refereed/all.links';
--copy :v1.Simbad from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/simbad/simbad_objects.tab';
--copy :v1.Grants from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/grants/all.links';
--copy :v1.Citation from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/citation/all.links';
--copy :v1.Relevance from program 'python /SpacemanSteve/tmp/code/relevanceIngest.py /SpacemanSteve/tmp/dataFiles/relevance/docmetrics.tab';
--copy :v1.Reader from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/alsoread_bib/all.links';
--copy :v1.Download from program 'python /SpacemanSteve/tmp/code/downloadIngest.py /SpacemanSteve/tmp/dataFiles/reads/downloads.links';
--copy :v1.Reads from program 'python /SpacemanSteve/tmp/code/downloadIngest.py /SpacemanSteve/tmp/dataFiles/reads/all.links';
--copy :v1.Reference from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/reference/all.links';
