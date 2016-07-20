
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



\set python_command '\'' 'python ' :pythonRoot '/columnFileIngest.py --fileType canonical ingest' '\''
\set schema_table '\'' :rowViewSchema '.Canonical' '\''
select process_file2(:schema_table, :python_command);

---------------------------------------------------------------------------
-- \set python_filename '\'' :python_root '/canonicalIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/bibcodes.list.can' '\''		 --
-- \set schema_table '\'' :row_view_schema '.Canonical' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
--
-- \set python_filename '\'' :python_root '/authorIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/facet_authors/all.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Author' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/refereedIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/refereed/all.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Refereed' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/simpleIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/simbad/simbad_objects.tab' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Simbad' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/simpleIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/grants/all.links' '\''		 --
-- \set schema_table '\'' :row_view_schema '.Grants' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/simpleIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/citation/all.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Citation' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/relevanceIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/relevance/docmetrics.tab' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Relevance' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/simpleIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/alsoread_bib/all.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Reader' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/downloadIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/reads/downloads.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Download' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/downloadIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/reads/all.links' '\''		 --
-- \set schema_table '\'' :row_view_schema '.Reads' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
-- 									 --
-- \set python_filename '\'' :python_root '/simpleIngest.py' '\''	 --
-- \set data_filename '\'' :data_root '/reference/all.links' '\''	 --
-- \set schema_table '\'' :row_view_schema '.Reference' '\''		 --
-- select process_file(:schema_table, :python_filename, :data_filename); --
---------------------------------------------------------------------------




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
