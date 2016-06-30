
create view ChangedRows as select :v1.RowView.bibcode from :v1.RowView,:v2.RowView where :v1.RowView.bibcode=:v2.RowView.bibcode and :v1.RowView.authors!=:v2.RowView.authors or :v1.RowView.refereed!=:v2.RowView.refereed or :v1.RowView.simbad_objects!=:v2.RowView.simbad_objects or :v1.RowView.grants!=:v2.RowView.grants or :v1.RowView.citations!=:v2.RowView.citations or :v1.RowView.boost!=:v2.rowView.boost or :v1.RowView.norm_cites!=:v2.RowView.norm_cites or :v1.RowView.citation_count!=:v2.RowView.citation_count or :v1.RowView.read_count!=:v2.RowView.read_count or :v1.RowView.readers!=:v2.RowView.readers or :v1.RowView.downloads!=:v2.RowView.downloads or :v1.RowView.reads!=:v2.RowView.reads;



