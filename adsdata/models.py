
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy import Table, bindparam, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql

import sqlalchemy
from sqlalchemy.schema import CreateSchema, DropSchema

from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from collections import defaultdict
from datetime import datetime

from adsputils import load_config, setup_logging

Base = declarative_base()


class NonBibTable(Base):
    __tablename__ = 'rowviewm'
    bibcode = Column(String, primary_key=True)
    id = Column(Integer)
    authors = Column(ARRAY(String))
    refereed = Column(Boolean)
    simbad_objects = Column(ARRAY(String))
    ned_objects = Column(ARRAY(String))
    grants = Column(ARRAY(String))
    citations = Column(ARRAY(String))
    boost = Column(Float)
    citation_count = Column(Integer)
    read_count = Column(Integer)
    norm_cites = Column(Integer)
    readers = Column(ARRAY(String))
    downloads = Column(ARRAY(String))
    reads = Column(ARRAY(String))
    reference = Column(ARRAY(String))

class NonBibDeltaTable(Base):
    __tablename__ = 'changedrowsmm'
    bibcode = Column(String, primary_key=True)
    id = Column(Integer)

class CanonicalTable(Base):
    __tablename__ = 'canonical'
    bibcode = Column(String, primary_key=True)
    id = Column(Integer)


class AuthorTable(Base):
    __tablename__ = 'author'
    bibcode = Column(String, primary_key=True)
    authors = Column(ARRAY(String)) 


class RefereedTable(Base):
    __tablename__ = 'refereed'
    bibcode = Column(String, primary_key=True)
    refereed = Column(Boolean)


class SimbadTable(Base):
    __tablename__ = 'simbad'
    bibcode = Column(String, primary_key=True)
    simbad_objects = Column(ARRAY(String))
                     

class NedTable(Base):
    __tablename__ = 'ned'
    bibcode = Column(String, primary_key=True)
    ned_objects = Column(ARRAY(String))
                     

class GrantsTable(Base):
    __tablename__ = 'grants'
    bibcode = Column(String, primary_key=True)
    grants = Column(ARRAY(String))
                     

class CitationTable(Base):
    __tablename__ = 'citation'
    bibcode = Column(String, primary_key=True)
    citations = Column(ARRAY(String))
                     

class RelevanceTable(Base):
    __tablename__ = 'relevance' 
    bibcode = Column(String, primary_key=True)
    boost = Column(Float)
    citation_count = Column(Integer)
    read_count = Column(Integer)
    norm_cites = Column(Integer)


class ReaderTable(Base):
    __tablename__ = 'reader'
    bibcode = Column(String, primary_key=True)
    readers = Column(ARRAY(String))
                     

class ReadsTable(Base):
    __tablename__ = 'reads'
    bibcode = Column(String, primary_key=True)
    reads = Column(ARRAY(Integer))
                     
                     
class DownloadTable(Base):
    __tablename__ = 'download'
    bibcode = Column(String, primary_key=True)
    downloads = Column(ARRAY(Integer))
                     

class ReferenceTable(Base):
    __tablename__ = 'reference'
    bibcode = Column(String, primary_key=True)
    reference = Column(ARRAY(String))
    
class ChangedTable(Base):
    __tablename__ = 'changedrows'
    bibcode = Column(String, primary_key=True)

class DataLinksTable(Base):
    __tablename__ = 'datalinks'
    bibcode = Column(String, primary_key=True)
    link_type = Column(String, primary_key=True)
    link_sub_type = Column(String, primary_key=True)
    url = Column(ARRAY(String))
    title = Column(ARRAY(String))

column_tables = (CanonicalTable, AuthorTable, RefereedTable, SimbadTable, NedTable,
                 GrantsTable, CitationTable, RelevanceTable, ReaderTable, DownloadTable, ReadsTable, ReferenceTable,
                 ChangedTable, DataLinksTable)


class MetricsTable(Base):
    # set schema name via table.schema
    __tablename__ = 'metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    bibcode = Column(String, nullable=False, index=True, unique=True)
    refereed = Column(Boolean)
    rn_citations = Column(postgresql.REAL)
    rn_citation_data = Column(postgresql.JSON)
    rn_citations_hist = Column(postgresql.JSON)
    downloads = Column(postgresql.ARRAY(Integer))
    reads = Column(postgresql.ARRAY(Integer))
    an_citations = Column(postgresql.REAL)
    refereed_citation_num = Column(Integer)
    citation_num = Column(Integer)
    reference_num = Column(Integer)
    citations = Column(postgresql.ARRAY(String))
    refereed_citations = Column(postgresql.ARRAY(String))
    author_num = Column(Integer)
    an_refereed_citations = Column(postgresql.REAL)
    modtime = Column(DateTime)
