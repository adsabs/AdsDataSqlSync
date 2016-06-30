

delete from :v1.Canonical;
delete from :v1.Author;
delete from :v1.Refereed;
delete from :v1.Simbad;
delete from :v1.Grants;
delete from :v1.Citation;
delete from :v1.Relevance;
delete from :v1.Reader;
delete from :v1.Download;
delete from :v1.Reads;

copy :v1.Canonical from program 'python /SpacemanSteve/tmp/code/canonicalIngest.py /SpacemanSteve/tmp/dataFiles/bibcodes.list.can';
copy :v1.Author from program 'python /SpacemanSteve/tmp/code/authorIngest.py /SpacemanSteve/tmp/dataFiles/facet_authors/all.links';
copy :v1.Refereed from program 'python /SpacemanSteve/tmp/code/refereedIngest.py /SpacemanSteve/tmp/dataFiles/refereed/all.links';
copy :v1.Simbad from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/simbad/simbad_objects.tab';
copy :v1.Grants from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/grants/all.links';
copy :v1.Citation from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/citation/all.links';
copy :v1.Relevance from program 'python /SpacemanSteve/tmp/code/relevanceIngest.py /SpacemanSteve/tmp/dataFiles/relevance/docmetrics.tab';
copy :v1.Reader from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/alsoread_bib/all.links';
copy :v1.Download from program 'python /SpacemanSteve/tmp/code/downloadIngest.py /SpacemanSteve/tmp/dataFiles/reads/downloads.links';
copy :v1.Reads from program 'python /SpacemanSteve/tmp/code/downloadIngest.py /SpacemanSteve/tmp/dataFiles/reads/all.links';
copy :v1.Reference from program 'python /SpacemanSteve/tmp/code/simpleIngest.py /SpacemanSteve/tmp/dataFiles/reference/all.links';
