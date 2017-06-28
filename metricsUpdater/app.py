"""                                                                                                                     
The main application object (it has to be loaded by any worker/script)                                                  
in order to get a working configuration.                                                    
"""
from __future__ import absolute_import, unicode_literals
from adsputils import ADSCelery


class MetricsUpdaterCelery(ADSCelery):
    
    # worker code in tasks.py
    @staticmethod
    def cleanup(db_records):
        """used before a database record or list of records is converted into protobuf
        
        this function returns a new object, it does not alter the passed db_record"""
        if isinstance(db_records, list):
            l = ()
            for db_record in db_records:
                l.append(MetricsUpdaterCelery.cleanup_aux(db_record))
            return l
        else:
            return MetricsUpdaterCelery.cleanup_aux(db_records)

    
    @staticmethod
    def cleanup_aux(db_record):
        d = dict(db_record)
        d.pop('id', None)   # id is primary key field, not included in queued messages
        return d 
    
