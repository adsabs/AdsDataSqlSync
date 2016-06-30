
drop materialized view if exists ingestTest.RowViewM;

    CREATE MATERIALIZED VIEW ingestTest.RowViewM AS
         select bibcode,
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
       from ingestTest.Canonical natural left join ingestTest.Author natural left join ingestTest.Refereed
       natural left join ingestTest.Simbad natural left join ingestTest.Grants natural left join ingestTest.Citation
       natural left join ingestTest.Relevance natural left join ingestTest.Reader
       natural left join ingestTest.Download natural left join ingestTest.Reads natural left join ingestTest.Reference;

       create index on ingestTest.RowViewM (bibcode);
