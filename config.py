

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

DATA_PATH = '/inputDataDir/'
TEST_DATA_PATH = 'tests/data/'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres@localhost:5432/postgres'
# metrics database used during ingest, not by celery code
METRICS_DATABASE = INGEST_DATABASE


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






# number of rows of column file to process
# set to a small number during testing to ingest just a little data quickly
# -1 means process all rows
MAX_ROWS = -1

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






