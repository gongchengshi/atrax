class LogType:
    InternalError = 6  # Events that are caused the system - potentially avoidable
    ExternalError = 5  # Events the system can't avoid
    InternalWarning = 4
    ExternalWarning = 3
    Info = 2
    Debug = 1


import time
import logging

for level_name, level_value in LogType.__dict__.iteritems():
    logging.addLevelName(level_value, level_name)

logging.getLogger().setLevel(LogType.Debug)


class LocalLogger:
    def __init__(self, source, name=None):
        self.source = source.replace('/', '_').replace(' ', '_')
        self.name = name.replace(' ', '_') if name else ''
        self._logger = logging.getLogger(source)
        self._logger.setLevel(LogType.Debug)

        if not self._logger.handlers:
            datetime_format = '%Y-%m-%dT%H:%M:%S'  # expected by CloudWatch Logs
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt=datetime_format)
            formatter.converter = time.gmtime

            fh = logging.FileHandler('/var/log/atrax-' + source + '.log', encoding='utf-8')
            fh.setLevel(LogType.Debug)
            fh.setFormatter(formatter)
            self._logger.addHandler(fh)

            if __debug__:
                ch = logging.StreamHandler()
                ch.setLevel(LogType.Debug)
                ch.setFormatter(formatter)
                self._logger.addHandler(ch)

    def log(self, severity, message, url=None, exception=None, exc_info=None):
        message = "%s %s" % (self.name, message)
        if url:
            message += " (URL: %s)" % url
        if exc_info is None and exception is not None:
            message += " (Exception: %s: %s)" % (type(exception).__name__, str(exception))

        self._logger.log(severity, message, exc_info=exc_info)


import sys
import pprint
import traceback

from aws.sdb import truncate_attribute_from_beginning, truncate_attribute_from_end


class SimpleLogger:
    def __init__(self, log_table, source):
        self.source = source
        self.log_table = log_table
        self._local_logger = None

    def log(self, severity, message, url=None, exception=None, exc_info=None):
        """
        :param severity:
        :param message:
        :param url:
        :param exception:
        :param exc_info:
        """
        timestamp = time.time()
        attributes = {
            'host': self.source,
            'type': severity,
            'message': truncate_attribute_from_end(message or '')}

        if url:
            attributes['url'] = truncate_attribute_from_end(url)

        if exception is not None:
            attributes['exception'] = truncate_attribute_from_end(type(exception).__name__ + ': ' + str(exception))

        if exc_info:
            # Instead of using traceback.format_exc() which appends the exception to the end, this gets just the stack.
            stack_trace = ''.join(traceback.format_tb(exc_info[2]))
            attributes['stacktrace'] = truncate_attribute_from_beginning(stack_trace)

        try:
            # utcnow has microsecond resolution so collisions are possible but not likely. It is good enough for a log.
            self.log_table.put_attributes(timestamp, attributes)
        except Exception, ex:
            if self._local_logger is None:
                self._local_logger = LocalLogger('logger')
            self._local_logger.log(LogType.InternalError, "SimpleLogger failed to emit log. Source: %s %s" %
                                   (self.source, pprint.pformat(attributes)), exception=ex, exc_info=sys.exc_info())
