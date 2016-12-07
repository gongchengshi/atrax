from StringIO import StringIO
import sys

from lxml import etree
from lxml.etree import XMLSyntaxError

from atrax.common.logger import LogType
from atrax.common.url_info import UrlInfo
from atrax.fetcher.url_extractors.url_extractor_base import UrlExtractorBase
from python_common.web.url_utils import InvalidUrl


class SitemapUrlExtractor(UrlExtractorBase):
    def __init__(self, logger):
        UrlExtractorBase.__init__(self, logger)

    def extract_urls(self, url_info, content):
        try:
            locations = []
            if url_info.url.endswith('txt'):
                locations = content.splitlines()
            elif url_info.url.endswith('xml'):
                tree = etree.parse(StringIO(content))
                if tree.docinfo.root_name == 'sitemapindex':
                    locations = tree.xpath('//*[name()="loc"]/text()')
                elif tree.docinfo.root_name == 'urlset':
                    locations = tree.xpath('//*[substring(name(), string-length(name())-2)="loc"]/text()')

            urls = set()
            for loc in [text.strip() for text in locations]:
                if loc.startswith('http'):
                    try:
                        urls.add(UrlInfo(loc))
                    except Exception, ex:
                        if type(ex) is InvalidUrl:
                            exc_info = None
                        else:
                            exc_info = sys.exc_info()
                        self.logger.log(LogType.InternalWarning, "Failed to extract URL from: %s" % loc,
                                        url_info.url, ex, exc_info)
        except XMLSyntaxError, ex:
            urls = []
        return urls
