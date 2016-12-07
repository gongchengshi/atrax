from abc import abstractmethod


class UrlExtractorBase:
    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def extract_urls(self, url_info, contents):
        pass


class ExtractUrlException:
    def __init__(self, tag, e, stack_trace):
        self.tag = tag
        self.inner_exception = e
        self.stack_trace = stack_trace
