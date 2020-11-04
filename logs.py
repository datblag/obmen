#from config import logname,logname_debug,logname_error
from logging.handlers import TimedRotatingFileHandler
import logging

def run_logers(prm_logname,prm_logname_debug,prm_logname_error):
    #logname = prm_logname
    #logname_debug = prm_logname_debug
    #logname_error = prm_logname_error
    f = logging.Formatter(fmt=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S")
    handlers = [
        logging.handlers.RotatingFileHandler(prm_logname, encoding='utf8',
            maxBytes=100000000, backupCount=100),
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(prm_logname_debug, encoding='utf8',
            maxBytes=100000000, backupCount=100),
        logging.handlers.RotatingFileHandler(prm_logname_error, encoding='utf8',
            maxBytes=100000000, backupCount=100)
    ]
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    handlers[0].setLevel(logging.INFO)
    handlers[1].setLevel(logging.INFO)
    handlers[2].setLevel(logging.DEBUG)
    handlers[3].setLevel(logging.ERROR)

    for h in handlers:
        h.setFormatter(f)
        #h.setLevel(logging.INFO)
        root_logger.addHandler(h)
    ##############################
    #### END LOGGING SETTINGS ####
    ##############################


def run(prm_logname,prm_logname_debug,prm_logname_error):
    run_logers(prm_logname,prm_logname_debug,prm_logname_error)










