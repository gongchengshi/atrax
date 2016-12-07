import sys
from atrax.common.logger import LocalLogger, LogType


def lumberjack():
    bright_side_of_death()


def bright_side_of_death():
    return tuple()[0]


def test_exception_log():
    logger = LocalLogger('dummy_logger', 0)
    logger.log(LogType.Info, "Hello world")
    url = 'https://www.fortbildung.siemens.com/ebis2/jobapplication/_resources/css/ui-lightness/jquery-ui-1.10.4.custom.min.css'

    try:
        lumberjack()
    except IndexError, ex:
        logger.log(LogType.Debug, "Big problem", url=url, exception=ex, exc_info=sys.exc_info())


test_exception_log()
