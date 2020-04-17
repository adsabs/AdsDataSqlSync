from __future__ import absolute_import, unicode_literals
from sqlalchemy.orm import load_only
import adsdata.app as app_module
from adsputils import get_date, exceptions
from kombu import Queue
import os
import adsmsg
from adsdata import models
from adsdata import datalinks

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.AdsDataCelery('ads-data', proj_home=proj_home)
logger = app.logger


app.conf.CELERY_QUEUES = (
    Queue('transform-results', app.exchange, routing_key='transform-results'),
    Queue('output-results', app.exchange, routing_key='output-results'),
    Queue('output-metrics', app.exchange, routing_key='output-metrics'),
)


# ============================= TASKS ============================================= #


# fields needed from nonbib to compute master record
nonbib_to_master_select_fields = ('bibcode', 'boost', 'citation_count',
                                  'grants', 'ned_objects', 'nonarticle', 'norm_cites', 'ocrabstract',
                                  'private', 'pub_openaccess', 'read_count', 'readers', 'reference',
                                  'refereed', 'simbad_objects')

# select fields that are not sent to master, they are used to compute solr property field
nonbib_to_master_property_fields = ('nonarticle', 'ocrabstract', 'private', 'pub_openaccess',
                                    'refereed', '_sa_instance_state')

@app.task(queue='transform-results')
def task_transform_results(schema, source=models.NonBibDeltaTable, offset=0, limit=100):
    """
    Transform records from the database to protobuf to be sent to master pipeline.

    Source can be:
    - models.NonBibDeltaTable (delta updates) or models.NonBibTable (all) but
    the task will only transform and send the limited number of records between
    offset and offset+limit.
    - a list of bibcodes (offset and limit parameters are ignored)
    """

    if source is not models.NonBibDeltaTable and source is not models.NonBibTable and not isinstance(source, (list, tuple)):
        raise Exception("Invalid source, it should be models.NonBibTable, models.NonBibDeltaTable, or a list of bibcodes")
    elif isinstance(source, (list, tuple)):
        bibcodes = source
        model = None
    else:
        bibcodes = None
        model = source

    with app.session_scope() as session:
        session.execute('set search_path to {}'.format(schema))
        records = []
        if bibcodes:
            q = session.query(models.NonBibTable).options(load_only('bibcode')).filter(models.NonBibTable.bibcode.in_(bibcodes))
        else:
            q = session.query(model).options(load_only('bibcode')).offset(offset).limit(limit)
        for record in q.yield_per(100):
            q = session.query(models.NonBibTable).filter(models.NonBibTable.bibcode==record.bibcode).options(load_only(*nonbib_to_master_select_fields))
            current_row = q.first()
            if current_row:
                current_data = current_row.__dict__
                author_count = len(current_data.get('authors', ()))
                current_data['citation_count_norm'] = current_data.get('citation_count', 0) / float(max(author_count, 1))
                #
                datalinks.add_data_links(session, current_data)
                _cleanup_for_master(current_data)
                record = adsmsg.NonBibRecord(**current_data)
                records.append(record._data)

        if len(records) > 0:
            records_list = adsmsg.NonBibRecordList()
            records_list.nonbib_records.extend(records)
            logger.debug("Calling 'task_output_results' for offset '%i' and limit '%i'", offset, limit)
            task_output_results.delay(records_list)


def _cleanup_for_master(r):
    """delete values from dict not needed by protobuf to master pipeline"""
    for f in nonbib_to_master_property_fields:
        r.pop(f, None)


@app.task(queue='output-results')
def task_output_results(msg):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param msg: a protobuf containing the non-bibliographic metadata

            {'bibcode': '....',
             'reads': [....],
             'simbad': '.....',
             .....
            }
    :return: no return
    """
    logger.debug('Will forward this nonbib record: %s', msg)
    app.forward_message(msg)


@app.task(queue='output-metrics')
def task_output_metrics(msg):
    """
    This worker will forward metrics to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param msg: a protobuf containing the metrics record

            {'bibcode': '....',
             'downloads': [....],
             'citations': [.....],
             .....
            }
    :return: no return
    """
    logger.debug('Will forward this metrics record: %s', msg)
    app.forward_message(msg)

if __name__ == '__main__':
    app.start()
