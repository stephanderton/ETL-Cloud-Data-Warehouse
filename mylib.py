import logging
import time


FILENAME_ROOT = 'etl'


def set_log_file_name():
    """
    Return the logfile pathname for the **current** date.

    """
    return "./logs/{}-{}.log".format(FILENAME_ROOT, time.strftime('%Y%m%d'))


def get_log_file_name():
    return LOGFILE
    

def setup_logger(logfile):
    """
    Create instances of logger and file handler.
    Create formatter and add to file handler.
    Add file handler to logger instance and set level to info.

    Arguments:
        logfile - file pathname for logfile

    Return:
        fl - Logger object

    """
    fl = logging.getLogger()
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter(fmt = FORMAT, datefmt = FORMAT_DATE)
    fh.setFormatter(formatter)
    fl.addHandler(fh)
    fl.setLevel(logging.INFO)

    return fl


def reset_logger():
    """
    Reset the logger object.
    This needs to be done when the date changes at midnight!

    Return:
        fl - Logger object

    """
    logfile = set_log_file_name()
    return setup_logger(logfile)


def log_timestamp():
    """
    Write the current timestamp to the logfile

    """
    logger.info(time.strftime('%Y-%m-%d  %I:%M:%S %p'))


# ------------------------------------------------------------------------------

LOGFILE     = set_log_file_name()
FORMAT      = '%(asctime)s :  %(message)s'
FORMAT_DATE = '%I:%M:%S %p'

logger = setup_logger(LOGFILE)
