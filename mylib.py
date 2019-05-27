import logging
import time

LOGFILE     = "./logs/test-{}.log".format(time.strftime('%Y%m%d'))
FORMAT      = '%(asctime)s :  %(message)s'
FORMAT_DATE = '%I:%M:%S %p'

# create logger
logger = logging.getLogger()

# create file handler
fh = logging.FileHandler(LOGFILE)

# create formatter
formatter = logging.Formatter(fmt = FORMAT, datefmt = FORMAT_DATE)

# add formatter to fh
fh.setFormatter(formatter)

# add fh to logger
logger.addHandler(fh)

# set level to info
logger.setLevel(logging.INFO)
