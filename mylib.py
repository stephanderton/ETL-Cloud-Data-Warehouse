import logging
import time

LOGFILE = "./logs/test-{}.log".format(time.strftime('%Y%m%d'))

# logging.basicConfig(format = '%(asctime)s :  %(message)s', datefmt = '%I:%M:%S %p',
#                     filename = LOGFILE, level = logging.INFO)

# create logger
logger = logging.getLogger(LOGFILE)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(fmt = '%(asctime)s :  %(message)s', datefmt = '%I:%M:%S %p')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
