

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres:postgres@localhost:5432/data_pipeline'
# metrics database used during ingest, not by celery code
METRICS_DATABASE = INGEST_DATABASE

DATA_PATH = './logs/input/current/'
# filenames for column files
AUTHOR = 'links/facet_authors/all.links'
CANONICAL = 'bibcodes.list.can'
CITATION = 'links/citation/all.links'
DOWNLOAD = 'links/reads/downloads.links'
GRANTS = 'links/grants/all.links'
NED = 'links/ned/ned_objects.tab'
NONARTICLE = 'links/nonarticle/all.links'
OCRABSTRACT = 'links/ocr/all.links'
PRIVATE = 'links/private/all.links'
PUB_OPENACCESS = 'links/openaccess/pub.dat'
READER = 'links/alsoread_bib/all.links'
READS = 'links/reads/all.links'
REFEREED = 'links/refereed/all.links'
REFERENCE = 'links/reference/all.links'
RELEVANCE = 'links/relevance/docmetrics.tab'
SIMBAD = 'links/simbad/simbad_objects.tab'
DATALINKS = ['links/facet_datasources/datasources.links,DATA',
            'links/electr/all.links,ESOURCE,PUB_HTML', # EJOURNAL
            'links/eprint_html/all.links,ESOURCE,EPRINT_HTML', # PREPRINT
            'links/pub_pdf/all.links,ESOURCE,PUB_PDF',
            'links/ads_pdf/all.links,ESOURCE,ADS_PDF',
            'links/eprint_pdf/all.links,ESOURCE,EPRINT_PDF',
            'links/author_html/all.links,ESOURCE,AUTHOR_HTML',
            'links/author_pdf/all.links,ESOURCE,AUTHOR_PDF',
            'links/ads_scan/all.links,ESOURCE,ADS_SCAN',
            'links/associated/all.links,ASSOCIATED',
            'links/video/all.links,PRESENTATION',
            'links/library/all.links,LIBRARYCATALOG',
            'links/spires/all.links,INSPIRE',
            'links/toc/all.links,TOC']
            # Note that we have NED and SIMBAND data files but they have been added to datasources file
            # and hence no need to read their individual files anymore, to add to linksdata table
            # 'ned/all.links,NED',
            # 'simbad/all.links,SIMBAD',

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
