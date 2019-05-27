import logging
import time


def log_file_name():
    """
    Return the logfile path for the **current** date.

    """
    return "./logs/test-{}.log".format(time.strftime('%Y%m%d'))


def setup_logger(logfile):
    """
    Create instances of logger and file handler.
    Create formatter and add to file handler.
    Add file handler to logger instance and set level to info.

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

    """
    logfile = log_file_name()
    return setup_logger(logfile)

# ------------------------------------------------------------------------------

LOGFILE     = log_file_name()
FORMAT      = '%(asctime)s :  %(message)s'
FORMAT_DATE = '%I:%M:%S %p'

logger = setup_logger(LOGFILE)
