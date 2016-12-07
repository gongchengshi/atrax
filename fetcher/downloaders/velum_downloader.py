import time
from multiprocessing import TimeoutError
import requests
from requests import Timeout as RequestsTimeout

from python_common.timeout_wrapper import Timeout
from python_common.web.http_headers import USER_AGENT_HEADER
from velum.common.utils import calculate_throughput
from velum.common.constants import GetNewProxyReasons
from atrax.common.constants import USER_AGENT_PATTERN
from atrax.fetcher.downloaders.downloaders import DownloaderBase, CommonHeaders


RESPONSE_TIMEOUT = 600  # 10 minutes
CONNECT_READ_TIMEOUT = 10


class ProxyDownException(Exception):
    def __init__(self, inner_exception):
        self.inner_exception = inner_exception


class VelumDownloader(DownloaderBase):
    def __init__(self, sld, is_robots_compliant, proxy_manager_client, fetcher_id):
        self.proxy_manager_client = proxy_manager_client
        self.is_robots_compliant = is_robots_compliant
        self.fetcher_id = fetcher_id
        self.domain = sld
        self.session = None
        self.proxy_id = None
        self.proxy_endpoint_id = None
        self.timeout = Timeout(RESPONSE_TIMEOUT,
                               TimeoutError("Proxy failed to complete request within %s seconds." % RESPONSE_TIMEOUT))

    def _configure_session(self, proxy_info):
        self.proxy_id = proxy_info.proxy_id
        self.proxy_endpoint_id = proxy_info.proxy_endpoint_id
        self.session = requests.session()
        self.session.proxies = {'http': proxy_info.proxy, 'https': proxy_info.proxy}
        self.session.headers.update({USER_AGENT_HEADER: USER_AGENT_PATTERN % (proxy_info.name, self.fetcher_id)})
        self.session.headers.update(CommonHeaders)
        self.session.allow_redirects = False
        self.session.verify = False
        self.session.timeout = CONNECT_READ_TIMEOUT

    def download(self, url, referrer=None, if_modified_since=None, if_none_match=None):
        if not self.session:
            self._configure_session(self.proxy_manager_client.get_current(self.domain, self.is_robots_compliant))

        timed_out_except = None

        t_start = time.time()

        try:
            with self.timeout:
                r = self.session.get(url,
                                     headers=DownloaderBase.construct_headers(referrer, if_modified_since, if_none_match),
                                     allow_redirects=False)
                throughput = calculate_throughput(r, t_start, time.time())

                if r.status_code in [403, 402, 420, 429]:  # The proxy has been blocked. Request a new one.
                    self._configure_session(self.proxy_manager_client.get_new(self.domain,
                                                                              self.is_robots_compliant,
                                                                              GetNewProxyReasons.BLOCKED))
        except RequestsTimeout, ex:
            timed_out_except = ex
            throughput = 0
            r = None
        except TimeoutError, ex:
            timed_out_except = ex
            throughput = 0
            r = None

        self.proxy_manager_client.update_stats(self.proxy_endpoint_id, throughput)

        if timed_out_except:
            self._configure_session(self.proxy_manager_client.get_new(self.domain, self.is_robots_compliant))
            raise timed_out_except

        return r
