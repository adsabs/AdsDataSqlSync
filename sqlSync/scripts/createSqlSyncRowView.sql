

    drop materialized view if exists :v1.RowViewM;


    CREATE MATERIALIZED VIEW :v1.RowViewM AS
         select bibcode,
	      id,
              coalesce(authors, ARRAY[]::text[]) as authors,
              coalesce(refereed, FALSE) as refereed,
              coalesce(simbad_objects, ARRAY[]::text[]) as simbad_objects,
              coalesce(grants, ARRAY[]::text[]) as grants,
              coalesce(citations, ARRAY[]::text[]) as citations,
              coalesce(boost, 0) as boost,
              coalesce(citation_count, 0) as citation_count,
              coalesce(read_count, 0) as read_count,
              coalesce(norm_cites, 0) as norm_cites,
              coalesce(readers, ARRAY[]::text[]) as readers,
              coalesce(downloads, ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]) as downloads,
              coalesce(reference, ARRAY[]::text[]) as reference,
              coalesce(reads, ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]) as reads
       from :v1.Canonical natural left join :v1.Author natural left join :v1.Refereed
       natural left join :v1.Simbad natural left join :v1.Grants natural left join :v1.Citation
       natural left join :v1.Relevance natural left join :v1.Reader
       natural left join :v1.Download natural left join :v1.Reads natural left join :v1.Reference;

     create index on :v1.RowViewM (bibcode);
     create index on :v1.RowViewM (id);
