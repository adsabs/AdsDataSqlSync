

#LOG_FILENAME = 'AdsDataSqlSync/logs/flatFileIngest.log'

# possible values: WARN, INFO, DEBUG                                                                    
LOGGING_LEVEL = 'DEBUG'

#DATA_PATH = '/SpacemanSteve/tmp/columnFiles3/'
#DATA_PATH = '/proj.adsqb/ads_abstracts/columnFiles2/'
#DATA_PATH = '/proj.adsqb/ads_abstracts/20160831/'
DATA_PATH = '/inputDataDir/current/'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres@localhost:5432/postgres'
# metrics database to use
METRICS_DATABASE = 'postgresql://postgres@localhost:5432/postgres'

# metrics database used for validation/comparison
METRICS_DATABASE2 = None

# filenames for column files
DOWNLOAD = 'reads/downloads.links'
READS = 'reads/all.links'
RELEVANCE = 'relevance/docmetrics.tab'
AUTHOR = 'facet_authors/all.links'
#AUTHOR = 'authors/all.links'
REFERENCE = 'reference/all.links'
SIMBAD = 'simbad/simbad_objects.tab'
GRANTS = 'grants/all.links'
CITATION = 'citation/all.links'
READER = 'alsoread_bib/all.links'
REFEREED = 'refereed/all.links'
CANONICAL = 'bibcodes.list.can'

# number of rows of column file to process
# set to a small number during testing to ingest just a little data quickly
# -1 means process all rows
MAX_ROWS = -1

#RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/'
RABBITMQ_URL = 'amqp://@adsx:5672/BumblebeeETL'
RABBITMQ_EXCHANGE = 'MergerPipelineExchange'
RABBITMQ_ROUTE = 'SolrUpdateRoute'

# based on ADSimmportpipleine.config.py
#CELERY_BROKER = 'pyamqp://guest@localhost:6672/new_host'
CELERY_BROKER = 'pyamqp://foo.bar/metrics'

# ================= celery/rabbitmq rules============== #
# ##################################################### #

ACKS_LATE=True
PREFETCH_MULTIPLIER=1
CELERYD_TASK_SOFT_TIME_LIMIT = 60
xCELERY_BROKER_URL='redis://localhost:6379/0'
xCELERY_RESULT_BACKEND='redis://localhost:6379/0'

CELERY_DEFAULT_EXCHANGE = 'import-pipeline'
CELERY_DEFAULT_EXCHANGE_TYPE = "topic"

