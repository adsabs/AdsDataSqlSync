"""                                                                                                                     
The main application object (it has to be loaded by any worker/script)                                                  
in order to get a working configuration.                                                    
"""
from __future__ import absolute_import, unicode_literals
from adsputils import ADSCelery


class NonbibUpdaterCelery(ADSCelery):
    
    pass
    

