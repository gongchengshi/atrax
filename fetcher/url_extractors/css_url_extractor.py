import traceback
import tinycss
import tinycss.decoding
from tinycss.css21 import RuleSet
from atrax.common.logger import LogType
from atrax.common.url_info import UrlInfo
from atrax.fetcher.url_extractors.url_extractor_base import UrlExtractorBase
from python_common.web.url_canonization import create_absolute_url
from python_common.web.url_utils import InvalidUrl


class CssUrlExtractor(UrlExtractorBase):
    def __init__(self, logger):
        UrlExtractorBase.__init__(self, logger)
        self.css_parser = tinycss.make_parser()

    def extract_urls(self, url_info, contents):
        urls = set()
        css_unicode, encoding = tinycss.decoding.decode(contents)
        stylesheet = self.css_parser.parse_stylesheet(css_unicode, encoding=encoding)
        for rule in stylesheet.rules:
            if not isinstance(rule, RuleSet):
                continue
            for declaration in rule.declarations:
                for token in declaration.value:
                    try:
                        if token.type != 'URI' or not token.value:
                            continue
                        new_url_info = UrlInfo(create_absolute_url(token.value, base=url_info.url))
                        if new_url_info == url_info:
                            continue
                        urls.add(new_url_info)  # removes duplicates
                    except Exception, ex:
                        if type(ex) is InvalidUrl:
                            stack_trace = None
                        else:
                            stack_trace = sys.exc_info()
                        self.logger.log(LogType.InternalWarning, "Failed to extract URL from: %s" % token.value,
                                        url_info.url, ex, stack_trace)

        return urls
