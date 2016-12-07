import socket
from httplib import HTTPResponse
from urlparse import urlparse

from urllib3.response import HTTPResponse as urllib3Response
import requests
from requests.structures import CaseInsensitiveDict
from requests.utils import get_encoding_from_headers
from atrax.fetcher.downloaders.downloaders import DownloaderBase, CommonHeaders


class ProxyDownException(Exception):
    def __init__(self, inner_exception):
        self.inner_exception = inner_exception


class PassThroughProxyDownloader(DownloaderBase):
    def __init__(self, user_agent, proxy):
        self.user_agent = user_agent

        web_proxy_split = proxy.split(':')
        self.proxy_ip = web_proxy_split[0]
        self.proxy_port = int(web_proxy_split[1])

    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        request = [
            "GET %s HTTP/1.1" % url,
            "User-Agent: %s" % self.user_agent,
            "Accept-Encoding: gzip, deflate, compress",
            "Accept: */*"
            "Host: %s" % urlparse(url).hostname,
            "Connection: Close"  # this may not be necessary
        ]

        for header, value in CommonHeaders.iteritems():
            request.append("%s: %s" % (header, value))

        for header, value in DownloaderBase.construct_headers(referrer, if_modified_since, if_none_match).iteritems():
            request.append("%s: %s" % (header, value))

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.proxy_ip, self.proxy_port))
        except socket.error, ex:
            raise ProxyDownException(ex)

        s.send("\r\n".join(request) + '\r\n\r\n')

        r = HTTPResponse(s, strict=False, method='GET', buffering=True)
        r.begin()

        resp = urllib3Response.from_httplib(r, decode_content=False)

        s.close()
        r.close()

        response = requests.Response()

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, 'status', None)
        response.headers = CaseInsensitiveDict(getattr(resp, 'headers', {}))
        response.encoding = get_encoding_from_headers(response.headers)
        response._content = resp.data
        response.raw = resp
        response.reason = response.raw.reason
        response.url = url

        # don't care about cookies right now
        #extract_cookies_to_jar(response.cookies, req, resp)

        # don't worry about the requests' Request object right now until it is needed
        #response.request = req
        response.request = None

        return response
