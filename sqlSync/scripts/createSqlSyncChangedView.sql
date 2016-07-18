
drop materialized view if exists :v1.ChangedRowsM;

create materialized view :v1.ChangedRowsM as 
       select :v1.RowViewM.bibcode, :v1.RowViewM.id
       from :v1.RowViewM,:v2.RowViewM
       where :v1.RowViewM.bibcode=:v2.RowViewM.bibcode 
          and :v1.RowViewM.authors!=:v2.RowViewM.authors 
	  or :v1.RowViewM.refereed!=:v2.RowViewM.refereed 
	  or :v1.RowViewM.simbad_objects!=:v2.RowViewM.simbad_objects 
	  or :v1.RowViewM.grants!=:v2.RowViewM.grants 
	  or :v1.RowViewM.citations!=:v2.RowViewM.citations 
	  or :v1.RowViewM.boost!=:v2.rowViewM.boost 
	  or :v1.RowViewM.norm_cites!=:v2.RowViewM.norm_cites 
	  or :v1.RowViewM.citation_count!=:v2.RowViewM.citation_count 
	  or :v1.RowViewM.read_count!=:v2.RowViewM.read_count 
	  or :v1.RowViewM.readers!=:v2.RowViewM.readers 
	  or :v1.RowViewM.downloads!=:v2.RowViewM.downloads 
	  or :v1.RowViewM.reads!=:v2.RowViewM.reads;




