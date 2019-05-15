DROP TABLE IF EXISTS datalinks;
CREATE TABLE datalinks(
      bibcode VARCHAR ,
      link_type VARCHAR,
      link_sub_type VARCHAR,
      url VARCHAR[],
      title VARCHAR[],
      item_count INTEGER
      );


INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'ESOURCE', 'ADS_PDF', '{"http://articles.adsabs.harvard.edu/pdf/1825AN......4..241B"}', '{}', 0);
INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'ASSOCIATED', 'NA', '{"1825AN......4..241B","2010AN....331..852K"}', '{"Main Paper","Translation"}', 0);
INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'INSPIRE', 'NA', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('1891opvl.book.....N', 'LIBRARYCATALOG', ' ', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2016Atoms...4...18I', 'ESOURCE', 'EPRINT_HTML', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2016Atoms...4...18I', 'ESOURCE', 'EPRINT_PDF', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1496E', 'ESOURCE', 'PUB_PDF', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ESOURCE', 'EPRINT_HTML', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ESOURCE', 'EPRINT_PDF', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ESOURCE', 'PUB_PDF', '{}', '{}', '0');
INSERT INTO public.datalinks VALUES ('1903BD....C......0A', 'DATA', 'CDS', '{}', '{}', '1');
INSERT INTO public.datalinks VALUES ('1903BD....C......0A', 'DATA', 'Vizier', '{}', '{}', '1');
INSERT INTO public.datalinks VALUES ('2018LPI....49.2177B', 'ESOURCE', 'PUB_PDF', '{}', '{}', '0');

