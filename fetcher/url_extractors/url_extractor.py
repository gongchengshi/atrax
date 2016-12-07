from atrax.fetcher.url_extractors.css_url_extractor import CssUrlExtractor
from atrax.fetcher.url_extractors.html_url_extractor import HtmlUrlExtractor
from atrax.fetcher.url_extractors.sitemap_url_extractor import SitemapUrlExtractor
from atrax.fetcher.url_extractors.url_extractor_base import UrlExtractorBase


class UrlExtractor(UrlExtractorBase):
    def __init__(self, logger):
        UrlExtractorBase.__init__(self, logger)

        html_url_extractor = HtmlUrlExtractor(logger)
        self.extractors = {
            "text/html": html_url_extractor,
            "application/xhtml+xml": html_url_extractor,
            "text/css": CssUrlExtractor(logger),
            "sitemap": SitemapUrlExtractor(logger)
        }

    @property
    def parsable_content_types(self):
        return self.extractors.keys()

    def extract_urls(self, url_info, contents):
        """
        Create UrlInfo objects for each unique URL found in the content
        """
        content_type, charset = url_info.content_type
        try:
            new_url_infos = self.extractors[content_type].extract_urls(url_info, contents)
        except:
            new_url_infos = []

        for new_url_info in new_url_infos:
            new_url_info.referrer_id = url_info.id

        return new_url_infos
