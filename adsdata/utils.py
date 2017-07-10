"""mostly code used by updater to update a sql table"""


def queue_changed_rows(delta_table, db_table, task, logger):
    """save queue changed metrics 

    delta_table: database table holding chagned and new bibcoces
    db_table: typically an instance of Metrics
    task: worker function that reads from queue"""
    bibcodes = []
    s = select(delta_table)
    deltas = delta_table.conn.execute(s)
    for current in delta:
        bibcodes.append(current)
        if len(bibcodes) > 100:
            db_records = db_table.get_by_bibcodes(bibcodes)
            update_buffer = []
            for db_record in db_records:
                update_buffer.append(create_clean(db_record))
            task.delay(update_buffer)
            bibcodes = []
    
    if len(bibcodes) > 0:
        db_records = db_table.get_by_bibcodes(bibcodes)
        update_buffer = []
        for db_record in db_records:
            update_buffer.append(create_clean(db_record))
        task.delay(update_buffer)
        
                     
            
def queue_rows(bibcodes_filename, db_table, task, logger):
    """for each bibcode in file, read db row and queue row to task
    
    bibcodes_filename: one bibcode per line
    db_table: follows duck typing of SqlSync and MetricsRecord
    task: a function decorated with @app.task 
    """
    bibcodes = []
    final_flag = False
    f = open(bibcodes_filename)
    if f is None:
        logger.error('could not open {}'.format(bibcodes_filename))
        return
    while True:
        line = f.readline()
        if len(line) == 0:
            final_flag = True
        else:
            bibcode = line.strip()
            bibcodes.append(bibcode)

        if len(bibcodes) >= 100 or final_flag:
            logger.info('time to queue data for {} bibcodes'.format(len(bibcodes)))
            if len(bibcodes) > 0:
                db_records = db_table.get_by_bibcodes(bibcodes)
                update_buffer = []
                for db_record in db_records:
                    update_buffer.append(create_clean(db_record))
                task.delay(update_buffer)
            logger.info('added {} records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            bibcodes = []
        if final_flag:
            return

        
def process_rows(records, db_table, logger):
    """write passed database rows to table
    
    records: a list of dicts, each representing a database row
    db_table: follws duck typing of SqlSyc and MetricsRecord
    """
    database_buffer = []
    for current in records:
        bibcode = current['bibcode']
        # read existing record from rds                                                                                                       
        r = db_table.read(bibcode)
        if r:
            # here if there is an existing record in the database, we need to do sql update                                               
            current['id'] = r['id']
            current['tmp_bibcode'] = bibcode
            database_buffer.append(current)
        else:
            # here if this is a new record for rds                                                                                            
            # we do not cache inserts, they are a small percentage of the traffic                                                             
            logger.info('performing rds insert for {}'.format(bibcode))
            db_table.connection.execute(db_table.table.insert(), [current])

    # for improved, performance we perform a single sql update operation                                                                      
    if len(database_buffer) > 0:
        db_table.connection.execute(db_table.updater_sql, database_buffer)
    logger.info('updated {} records to schema {}'.format(len(database_buffer), db_table.schema))
    
def create_clean(db_record):
    """called with records about to be sent to queue

       this function returns a new object, it does not alter the passed db_record"""
    d = dict(db_record)
    d.pop('id', None)   # id is primary key field, not included in queued messages
    return d 
    
    
    
    
    
    
