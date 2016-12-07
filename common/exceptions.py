class InvalidCrawlJobNameException(Exception):
    def __init__(self, crawl_job_name):
        Exception.__init__(self, "The crawl job name is invalid: '%s'" % crawl_job_name)


class DependencyInitializationError(Exception):
    def __init__(self, dependency_name):
        Exception.__init__(self, "%s is not running" % dependency_name)


class IllegalDataType(Exception):
    def __init__(self, data_type):
        Exception.__init__(self, "Illegal data type: %s" % data_type)
