DROP TABLE IF EXISTS datalinks;
CREATE TABLE datalinks(
      bibcode VARCHAR ,
      link_type VARCHAR,
      link_sub_type VARCHAR,
      url VARCHAR,
      title VARCHAR,
      item_count INTEGER
      );


INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'ARTICLE', ' ', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'ASSOCIATED', ' ', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'INSPIRE', ' ', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2004MNRAS.354L..31M', 'INSPIRE', ' ', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('1891opvl.book.....N', 'LIBRARYCATALOG', ' ', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2016Atoms...4...18I', 'ARTICLE', 'EPRINT_HTML', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2016Atoms...4...18I', 'ARTICLE', 'EPRINT_PDF', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1496E', 'ARTICLE', 'PUB_PDF', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ARTICLE', 'EPRINT_HTML', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ARTICLE', 'EPRINT_PDF', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('2014MNRAS.444.1497S', 'ARTICLE', 'PUB_PDF', ' ', ' ', '0');
INSERT INTO public.datalinks VALUES ('1903BD....C......0A', 'DATA', 'CDS', ' ', ' ', '1');
INSERT INTO public.datalinks VALUES ('1903BD....C......0A', 'DATA', 'Vizier', ' ', ' ', '1');
