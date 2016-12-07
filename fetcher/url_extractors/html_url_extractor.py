import sys

from bs4 import BeautifulSoup

from atrax.common.logger import LogType
from atrax.common.url_info import UrlInfo
from atrax.fetcher.url_extractors.url_extractor_base import UrlExtractorBase
from python_common.web.html_utils import starttag
from python_common.web.url_canonization import create_absolute_url
from python_common.web.url_utils import InvalidUrl


class HtmlUrlExtractor(UrlExtractorBase):
    # todo: find SWFs using this pattern: ['"](.*\.swf)['"]
    def extract_urls(self, url_info, contents):
        try:
            content_type, charset = url_info.content_type
            unicode_contents = contents.decode(charset) if charset else contents.decode('utf-8')
        except UnicodeDecodeError:
            unicode_contents = contents.decode("iso-8859-1")  # try this.
        soup = BeautifulSoup(unicode_contents)

        # set the base URL for the page used for creating absolute URLs
        base = soup.base.get('href') if soup.base else url_info.url

        urls = set()

        for element in soup.find_all('object', {'data': True}):
            try:
                data = element.get('data')
                if data.startswith('http'):
                    new_url_info = UrlInfo(create_absolute_url(data, base=base))
                    if new_url_info:
                        urls.add(new_url_info)
            except Exception, ex:
                self.log_extract_failure(ex, url_info, element)

        self.find_all_urls_with_attributes(['href', 'src'], soup, urls, base, url_info)

        return urls

    def find_all_urls_with_attributes(self, attributes, soup, urls, base, url_info):
        for attribute in attributes:
            for element in soup.find_all(attrs={attribute: True}):
                try:
                    value = element.get(attribute)
                    if value == '':
                        continue
                    lcase_value = value.lower()
                    if 'javascript:' in lcase_value or 'mailto:' in lcase_value:
                        continue

                    new_url_info = UrlInfo(create_absolute_url(value, base=base))
                    if new_url_info == url_info:
                        continue
                    urls.add(new_url_info)
                except Exception, ex:
                    self.log_extract_failure(ex, url_info, element)

    def log_extract_failure(self, ex, url_info, element):
        if type(ex) is InvalidUrl:
            exc_info = None
        else:
            exc_info = sys.exc_info()
        self.logger.log(LogType.InternalWarning, "Failed to extract URL from: %s" % starttag(str(element).strip('<>')),
                        url_info.url, ex, exc_info)
