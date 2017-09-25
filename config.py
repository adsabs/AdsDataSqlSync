

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres:postgres@localhost:5432/postgres'
# metrics database used during ingest, not by celery code
METRICS_DATABASE = INGEST_DATABASE

DATA_PATH = '/inputDataDir/'
# filenames for column files
AUTHOR = 'config/links/facet_authors/all.links'
CANONICAL = 'config/bibcodes.list.can'
CITATION = 'config/links/citation/all.links'
DOWNLOAD = 'config/links/reads/downloads.links'
GRANTS = 'config/links/grants/all.links'
NED = 'config/links/ned/ned_objects.tab'
READER = 'config/links/alsoread_bib/all.links'
READS = 'config/links/reads/all.links'
REFEREED = 'config/links/refereed/all.links'
REFERENCE = 'config/links/reference/all.links'
RELEVANCE = 'config/links/relevance/docmetrics.tab'
SIMBAD = 'config/links/simbad/simbad_objects.tab'
DATALINKS = ['config/links/eprint_html/all.links,ARTICLE,EPRINT_HTML', # PREPRINT
            'config/links/electr/all.links,ARTICLE,PUB_HTML', # EJOURNAL
            'config/links/associated/all.links,ASSOCIATED',
            'config/links/video/all.links,PRESENTATION',
            'config/links/library/all.links,LIBRARYCATALOG',
            'config/links/spires/all.links,INSPIRE',
            'config/links/facet_datasources/datasources.links,DATA',
            'config/links/pub_pdf/all.links,ARTICLE,PUB_PDF',
            'config/links/ads_pdf/all.links,ARTICLE,ADS_PDF',
            'config/links/eprint_pdf/all.links,ARTICLE,EPRINT_PDF',
            'config/links/author_html/all.links,ARTICLE,AUTHOR_HTML',
            'config/links/author_pdf/all.links,ARTICLE,AUTHOR_PDF']
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






