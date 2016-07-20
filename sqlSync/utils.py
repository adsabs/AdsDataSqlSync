"""Contains useful functions and utilities, mostly from ADSOrcid
"""

import os
import logging
import imp
import sys
from cloghandler import ConcurrentRotatingFileHandler


def load_config():
    """                                                                                                 
    Loads configuration from config.py and also from local_config.py                                    
                                                                                                        
    :return dictionary                                                                                  
    """
    conf = {}
    PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), './'))
    if PROJECT_HOME not in sys.path:
        sys.path.append(PROJECT_HOME)
    conf['PROJ_HOME'] = PROJECT_HOME

    conf.update(load_module(os.path.join(PROJECT_HOME, 'config.py')))
    conf.update(load_module(os.path.join(PROJECT_HOME, 'local_config.py')))

    return conf

def load_module(filename):
    """                                                                                                 
    Loads module, first from config.py then from local_config.py                                        
                                                                                                        
    :return dictionary                                                                                  
    """

    filename = os.path.join(filename)
    d = imp.new_module('config')
    d.__file__ = filename
    try:
        with open(filename) as config_file:
            exec(compile(config_file.read(), filename, 'exec'), d.__dict__)
    except IOError as e:
        pass
    res = {}
    from_object(d, res)
    return res


def setup_logging(file_, name_, level='WARN'):
    """                                                                                                 
    Sets up generic logging to file with rotating files on disk                                         
                                                                                                        
    :param file_: the __file__ doc of python module that called the logging                             
    :param name_: the name of the file that called the logging                                          
    :param level: the level of the logging DEBUG, INFO, WARN                                            
    :return: logging instance                                                                           
    """

    level = getattr(logging, level)

    logfmt = '%(levelname)s\t%(process)d [%(asctime)s]:\t%(message)s'
    datefmt = '%m/%d/%Y %H:%M:%S'
    formatter = logging.Formatter(fmt=logfmt, datefmt=datefmt)
    logging_instance = logging.getLogger(name_)
    fn_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'logs')
    if not os.path.exists(fn_path):
        os.makedirs(fn_path)
    fn = os.path.join(fn_path, '{0}.log'.format(name_))
    rfh = ConcurrentRotatingFileHandler(filename=fn,
                                                                        maxBytes=2097152,
                                                                        backupCount=5,
                                        mode='a',
                                        encoding='UTF-8')  # 2MB file                                   
    rfh.setFormatter(formatter)
    logging_instance.handlers = []
    logging_instance.addHandler(rfh)
    logging_instance.setLevel(level)

    return logging_instance


def from_object(from_obj, to_obj):
    """Updates the values from the given object.  An object can be of one                               
    of the following two types:                                                                         
                                                                                                        
    Objects are usually either modules or classes.                                                      
    Just the uppercase variables in that object are stored in the config.                               
                                                                                                        
    :param obj: an import name or object                                                                
    """
    for key in dir(from_obj):
        if key.isupper():
            to_obj[key] = getattr(from_obj, key)

