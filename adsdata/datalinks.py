from adsdata import reader

PROPERTY_QUERY = "select string_agg(distinct link_type, ',') as property from {db}.datalinks where bibcode = '{bibcode}'"
ESOURCE_QUERY = "select string_agg(link_sub_type, ',') as eSource from {db}.datalinks where link_type = 'ESOURCE' and bibcode = '{bibcode}'"
DATA_QUERY = "select sum(item_count), string_agg(link_sub_type || ':' || item_count::text, ',') as data from {db}.datalinks where link_type = 'DATA' and bibcode = '{bibcode}'"
DATALINKS_QUERY = "select link_type, link_sub_type, url, title, item_count from {db}.datalinks where bibcode = '{bibcode}'"

def load_column_files_datalinks_table(config, table_name, file_type, raw_conn, cur):
    # config['DATALINKS'] is a list of lines that could have one the following two formats
    # path,link_type,link_sub_type (i.e., config/links/eprint_html/all.links,ARTICLE,EPRINT_HTML) or
    # path,link_type (i.e., config/links/video/all.links,PRESENTATION)
    for oneLinkType in config['DATALINKS']:
        if (oneLinkType.count(',') == 1):
            [filename, linktype] = oneLinkType.split(',')
            linksubtype = 'NA'
        elif (oneLinkType.count(',') == 2):
            [filename, linktype, linksubtype] = oneLinkType.split(',')
        else:
            return

        if linktype == 'ASSOCIATED':
            r = reader.DataLinksWithTitleFileReader(file_type, config['DATA_PATH'] + filename, linktype)
        elif linktype == 'DATA':
            r = reader.DataLinksWithTargetFileReader(file_type, config['DATA_PATH'] + filename, linktype)
        else:
            r = reader.DataLinksFileReader(file_type, config['DATA_PATH'] + filename, linktype, linksubtype)

        if r:
            cur.copy_from(r, table_name)
            raw_conn.commit()

def add_data_links(session, data):
    """populate property, esource, data, total_link_counts, and data_links_rows fields"""

    q = PROPERTY_QUERY.format(db='nonbib', bibcode=data['bibcode'])
    result = session.execute(q)
    data['property'] = _fetch_data_link_elements(result.fetchone())

    q = ESOURCE_QUERY.format(db='nonbib', bibcode=data['bibcode'])
    result = session.execute(q)
    data['esource'] = _fetch_data_link_elements(result.fetchone())

    data = _add_data_link_extra_properties(data)

    q = DATA_QUERY.format(db='nonbib', bibcode=data['bibcode'])
    result = session.execute(q)
    data['data'], data['total_link_counts'] = _fetch_data_link_elements_counts(result.fetchone())

    q = DATALINKS_QUERY.format(db='nonbib', bibcode=data['bibcode'])
    result = session.execute(q)
    data['data_links_rows'] = _fetch_data_link_record(result.fetchall())

def _fetch_data_link_elements(query_result):
    elements = []
    if (query_result[0] != None):
        for e in query_result[0].split(','):
            if (e != None):
                elements.append(e)
    return elements

def _fetch_data_link_elements_counts(query_result):
    elements = []
    cumulative_count = 0
    if (query_result[1] != None):
        for e in query_result[1].split(','):
            if (e != None):
                elements.append(e)
        cumulative_count = query_result[0]
    return [elements, cumulative_count]


def _fetch_data_link_record(query_result):
    # since I want to use this function from the test side,
    # I was not able to use the elegant function row2dict function
    # convert query results to a list of dicts
    columns = ['link_type', 'link_sub_type', 'url', 'title', 'item_count']
    results = []
    for row in query_result:
        d = {}
        for i, field in enumerate(row):
            d[columns[i]] = field
        results.append(d)
    return results

def _add_data_link_extra_properties(data):
    # first, augment property field with article/nonartile, refereed/not refereed
    if data['nonarticle']:
        data['property'].append(u'NONARTICLE')
    else:
        data['property'].append(u'ARTICLE')
    if data['refereed']:
        data['property'].append(u'REFEREED')
    else:
        data['property'].append(u'NOT REFEREED')
    # now augment the property field with many other boolean fields
    extra_properties = ('pub_openaccess', 'private', 'ocrabstract')
    for p in extra_properties:
        if data[p]:
            data['property'].append(p.upper())
    # these property fields are set from availability of url
    extra_properties_link_type = {'ADS_PDF':'ADS_OPENACCESS', 'ADS_SCAN':'ADS_OPENACCESS',
                                  'AUTHOR_PDF':'AUTHOR_OPENACCESS', 'AUTHOR_HTML':'AUTHOR_OPENACCESS',
                                  'EPRINT_PDF':'EPRINT_OPENACCESS', 'EPRINT_HTML':'EPRINT_OPENACCESS'}
    for key,value in extra_properties_link_type.iteritems():
        if key in data['esource'] and value not in data['property']:
            data['property'].append(value)
    # see if there is any of *_openaccess flags set, if so set the generic openaccess flag
    if ('ADS_OPENACCESS' in data['property']) or ('AUTHOR_OPENACCESS' in data['property']) or \
       ('EPRINT_OPENACCESS' in data['property']) or ('PUB_OPENACCESS' in data['property']):
       data['property'].append('OPENACCESS')
    return data

