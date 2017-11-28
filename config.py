

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres:postgres@localhost:5432/data_pipeline'
# metrics database used during ingest, not by celery code
METRICS_DATABASE = INGEST_DATABASE

DATA_PATH = '/inputDataDir/'
# filenames for column files
ADS_OPENACCESS = 'config/links/openaccess/ads.dat'
AUTHOR_OPENACCESS = 'config/links/openaccess/author.dat'
AUTHOR = 'config/links/facet_authors/all.links'
CANONICAL = 'config/bibcodes.list.can'
CITATION = 'config/links/citation/all.links'
DOWNLOAD = 'config/links/reads/downloads.links'
EPRINT_OPENACCESS = 'config/links/openaccess/eprint.dat'
GRANTS = 'config/links/grants/all.links'
NED = 'config/links/ned/ned_objects.tab'
OCRABSTRACT = 'config/links/ocr/all.links'
PRIVATE = 'config/links/private/all.links'
PUB_OPENACCESS = 'config/links/openaccess/pub.dat'
OPENACCESS = 'config/links/openaccess/all.links'
READER = 'config/links/alsoread_bib/all.links'
READS = 'config/links/reads/all.links'
REFEREED = 'config/links/refereed/all.links'
REFERENCE = 'config/links/reference/all.links'
RELEVANCE = 'config/links/relevance/docmetrics.tab'
SIMBAD = 'config/links/simbad/simbad_objects.tab'
TOC = 'config/links/toc/all.links'
DATALINKS = ['config/links/facet_datasources/datasources.links,DATA',
            'config/links/electr/all.links,ESOURCE,PUB_HTML', # EJOURNAL
            'config/links/eprint_html/all.links,ESOURCE,EPRINT_HTML', # PREPRINT
            'config/links/pub_pdf/all.links,ESOURCE,PUB_PDF',
            'config/links/ads_pdf/all.links,ESOURCE,ADS_PDF',
            'config/links/eprint_pdf/all.links,ESOURCE,EPRINT_PDF',
            'config/links/author_html/all.links,ESOURCE,AUTHOR_HTML',
            'config/links/author_pdf/all.links,ESOURCE,AUTHOR_PDF',
            'config/links/ads_scan/all.links,ESOURCE,ADS_SCAN',
            'config/links/associated/all.links,ASSOCIATED',
            'config/links/video/all.links,PRESENTATION',
            'config/links/library/all.links,LIBRARYCATALOG',
            'config/links/spires/all.links,INSPIRE']
            # Note that we have NED and SIMBAND data files but they have been added to datasources file
            # and hence no need to read their individual files anymore, to add to linksdata table
            # 'config/links/ned/all.links,NED',
            # 'config/links/simbad/all.links,SIMBAD',

# number of rows of column file to process
# set to a small number during testing to ingest just a little data quickly
# -1 means process all rows
MAX_ROWS = -1

TEST_DATA_PATH = 'tests/data/'

# ================= celery/rabbitmq rules============== #
# ##################################################### #

ACKS_LATE=True
PREFETCH_MULTIPLIER=1
CELERYD_TASK_SOFT_TIME_LIMIT = 60

CELERY_DEFAULT_EXCHANGE = 'ads-data'
CELERY_DEFAULT_EXCHANGE_TYPE = "topic"

CELERY_BROKER = 'pyamqp://guest:guest@localhost:5682/data_pipeline'
OUTPUT_CELERY_BROKER = 'pyamqp://guest:guest@localhost:5682/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'

PROPERTY_QUERY = "select string_agg(distinct link_type, ',') as property from {db}.datalinks where bibcode = '{bibcode}'"

ESOURCE_QUERY = "select string_agg(link_sub_type, ',') as eSource from {db}.datalinks where link_type = 'ESOURCE' and bibcode = '{bibcode}'"

DATA_QUERY = "select sum(item_count), string_agg(link_sub_type || ':' || item_count::text, ',') as data from {db}.datalinks where link_type = 'DATA' and bibcode = '{bibcode}'"

DATALINKS_QUERY = "select link_type, link_sub_type, url, title, item_count from {db}.datalinks where bibcode = '{bibcode}'"
