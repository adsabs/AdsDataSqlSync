
drop schema if exists :v1 cascade;

create schema :v1


       create table Metrics
       	      (id SERIAL,
	       bibcode character varying unique,
	       refereed boolean,
	       rn_citations real,
	       rn_citation_data json,
	       downloads integer[],
	       reads integer[],
	       an_citations real,
	       refereed_citation_num integer,
	       citation_num integer,
	       reference_num integer,
	       citations character varying[],
	       refereed_citations character varying[],
	       author_num integer,
	       an_refereed_citations real,
	       	modtime timestamp without time zone);


