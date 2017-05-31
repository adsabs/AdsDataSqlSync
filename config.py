

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

DATA_PATH = '/inputDataDir/current/'

# where to read column files into
INGEST_DATABASE = 'postgresql://postgres@localhost:5432/postgres'
# metrics database used during ingest, not by celery code
METRICS_DATABASE = 'postgresql://postgres@localhost:5432/postgres'
#METRICS_DATABASE2 = 'postgresql://metrics:metrics@testing.ci1iae2ep00k.us-east-1.rds.amazonaws.com:5432/metrics'
#METRICS_DATABASE2 = 'postgresql://metrics:metrics@adsabs-psql.ci1iae2ep00k.us-east-1.rds.amazonaws.com:5432/metrics'

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

RABBITMQ_URL = 'amqp://user:pass@host:5682/etl'
RABBITMQ_EXCHANGE = 'MergerPipelineExchange'
RABBITMQ_ROUTE = 'SolrUpdateQueue'

# based on ADSimmportpipleine.config.py
CELERY_BROKER = 'pyamqp://user:pass@host:5682/metrics'

# ================= celery/rabbitmq rules============== #
# ##################################################### #

ACKS_LATE=True
PREFETCH_MULTIPLIER=1
CELERYD_TASK_SOFT_TIME_LIMIT = 60

CELERY_DEFAULT_EXCHANGE = 'import-pipeline'
CELERY_DEFAULT_EXCHANGE_TYPE = "topic"

# database connection and schema info for celery
# celery based code can read and write with any metrics store
# useful for moving data from tmp to staging or staging to test or prod

METRICS_CONNECTIONS = {}
METRICS_CONNECTIONS['localhost'] = 'postgresql://user:pass@host:5432/postgres'
METRICS_CONNECTIONS['test'] = 'postgresql://user:pass@host:5432/metrics'
METRICS_CONNECTIONS['prod'] = 'postgresql://user:pass@host:5432/metrics'

UPDATE_METRICS_VHOST = {}
UPDATE_METRICS_VHOST['metrics'] = {'schema': 'metricsstaging', 'db': METRICS_CONNECTIONS['localhost']}
UPDATE_METRICS_VHOST['metrics-test'] = {'schema': '', 'db': METRICS_CONNECTIONS['test']}
UPDATE_METRICS_VHOST['metrics-prod'] = {'schema': '', 'db': METRICS_CONNECTIONS['prod']}
