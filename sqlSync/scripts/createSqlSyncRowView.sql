
drop materialized view if exists ingesttest.RowViewM;

    CREATE MATERIALIZED VIEW ingesttest.RowViewM AS
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
       from ingesttest.Canonical natural left join ingesttest.Author natural left join ingesttest.Refereed
       natural left join ingesttest.Simbad natural left join ingesttest.Grants natural left join ingesttest.Citation
       natural left join ingesttest.Relevance natural left join ingesttest.Reader
       natural left join ingesttest.Download natural left join ingesttest.Reads natural left join ingesttest.Reference;

       create index on ingesttest.RowViewM (bibcode);
