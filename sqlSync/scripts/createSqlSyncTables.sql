
drop schema if exists :v1 cascade;

create schema :v1

       create table Canonical (bibcode text primary key, id SERIAL)
       create table Author (bibcode text primary key, authors text[])
       create table Refereed (bibcode text primary key, refereed boolean)
       create table Simbad (bibcode text primary key, simbad_objects text[])
       create table Grants (bibcode text primary key, grants text[])
       create table Citation (bibcode text primary key, citations text[])
       create table Relevance (bibcode text primary key, boost float, citation_count int, read_count int, norm_cites int)
       create table Reader (bibcode text primary key, readers text[])
       create table Download (bibcode text primary key, downloads int[])
       create table Reference (bibcode text primary key, reference text[])
       create table Reads (bibcode text primary key, reads int[])

;





