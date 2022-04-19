# noinspection PyUnresolvedReferences
# This needs to be here in order to monkey patch httplib.HTTPConnection and get it to do DNS caching.
import python_common.web.dns_cache
import urllib
from abc import abstractmethod, ABCMeta

import requests
requests.packages.urllib3.disable_warnings()

from python_common.web.http_headers import *


CommonHeaders = {
    X_FORWARDED_FOR_HEADER: '',
    CLIENT_IP_HEADER: '',
    VIA_HEADER: '0'
}


class DownloaderBase(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        return

    @staticmethod
    def construct_headers(referrer=None, if_modified_since=None, if_none_match=None):
        headers = {}
        if referrer:
            headers[REFERRER_HEADER] = referrer
        if if_modified_since:
            headers[IF_MODIFIED_SINCE_HEADER] = if_modified_since
        if if_none_match:
            headers[IF_NONE_MATCH_HEADER] = if_none_match
        return headers


class CrawleraDownloader(DownloaderBase):
    def __init__(self, user_agent):
        self.session = requests.session()
        self.session.auth = ('', '')
        self.session.headers.update({USER_AGENT_HEADER: user_agent})
        self.session.headers.update(CommonHeaders)
        self.session.allow_redirects = False
        self.session.verify = False
        self.session.timeout = 10

    #_charactersToRemove = len("http://proxy.crawlera.com:8010/fetch?url=")
    _charactersToRemove = len("http://api.crawlera.com/fetch?url=")

    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        response = self.session.get("http://api.crawlera.com/fetch",
                                    params={'url': url},
                                    headers=DownloaderBase.construct_headers(referrer, if_modified_since, if_none_match),
                                    allow_redirects=False)
        response.url = urllib.unquote(response.url[self._charactersToRemove:])

        if response.status_code == 503 and len(response.text) < 50:
            response.reason = '%s (%s)' % (response.reason, response.text)
        return response


class ProxyDownloader(DownloaderBase):
    def __init__(self, user_agent, proxy):
        self.session = requests.session()
        self.session.proxies = {'http': proxy, 'https': proxy}
        self.session.headers.update({USER_AGENT_HEADER: user_agent})
        self.session.headers.update(CommonHeaders)
        self.session.allow_redirects = False
        self.session.verify = False
        self.session.timeout = 10

    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        return self.session.get(url,
                                headers=DownloaderBase.construct_headers(referrer, if_modified_since, if_none_match),
                                allow_redirects=False)


class GenericDownloader(DownloaderBase):
    def __init__(self, user_agent):
        self.session = requests.session()
        self.session.headers.update({USER_AGENT_HEADER: user_agent})
        self.session.headers.update(CommonHeaders)
        self.session.allow_redirects = False
        self.session.verify = False
        self.session.timeout = 10

    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        return self.session.get(url,
                                headers=DownloaderBase.construct_headers(referrer, if_modified_since, if_none_match),
                                allow_redirects=False)
