import time

import tldextract

from atrax.common import schemeless_url_to_s3_key, domain_from_schemeless_url
from atrax.common.constants import CACHE_STAGNATION_FACTOR
from python_common.web.url_canonization import canonize_url
from python_common.web.url_utils import remove_url_scheme
from python_common.web.http_utils import split_content_type_header, parse_http_timestamp
from python_common.web.http_headers import *


class UrlInfo:
    def __init__(self, raw_url, canonized_url=None):
        self.raw_url = raw_url if type(raw_url) is unicode else raw_url.decode('utf-8')
        self.url = None
        self.id = None
        # For efficiency sake, the crawler does not support case-sensitivity in the URL path
        self.set_url(canonized_url if canonized_url else canonize_url(self.raw_url, lowercase_path=True))
        self.discovered = None  # float - When the URL was first identified
        # if fetched > requested then one of the requests failed before it was able to be fetched.
        # if requested > fetched then the resource hasn't changed since it was fetched at the time it was requested.
        self.requested = None  # float - The last time the resource was requested from the server
        self.fetched = None  # float - When the resource was successfully downloaded
        self.fetcher = None  # The name of the fetcher that downloaded the resource
        self.referrer_id = None  # The id of the URL that this URL was found at.
        self.robot_exclusion_reasons = set()  # Reasons the User Agent was excluded from crawling the resource.
        self.googlebot_exclusion_reasons = set()  # Reasons Google was excluded from crawling the resource.
        self.http_status = None  # Integer status of the last request for this resource.
        self.redirects_to = None  # The ID of the URL that this URL redirects to
        self.fingerprint = None  # The fingerprint of fetched content
        self.original = None  # The ID of the URL that has content identical to this resource
        self.process_attempts = 0  # The number of attempts the fetcher has made to download this resource.
        self.response_headers = {}  # The headers returned in the response
        self.num_redirects = 0  # The number of redirects that happened in order to get to this URL
        self.is_seed = False
        self._s3_key = None
        self._expires = None
        self._last_modified = None
        self._domain = None
        self._sld = None
        self._content_type = None
        self._charset = None
        self._dust_info = None

    def set_url(self, url):
        self.url = url
        self.id = remove_url_scheme(self.url)

    def change_scheme(self, new_scheme):
        self.url = new_scheme + '://' + self.id

    def set_dust_info(self, dust_info):
        self._dust_info = dust_info

    def get_dust_info(self):
        return self._dust_info

    dust_info = property(get_dust_info, set_dust_info)

    @property
    def s3_key(self):
        if not self._s3_key:
            self._s3_key = schemeless_url_to_s3_key(self.id)
        return self._s3_key

    @property
    def domain(self):
        if not self._domain:
            self._domain = domain_from_schemeless_url(self.id)
        return self._domain

    @property
    def sld(self):
        if not self._sld:
            r = tldextract.extract(self.domain)
            self._sld = r.domain + '.' + r.suffix
        return self._sld

    @property
    def content_type(self):
        if not self._content_type:
            self._content_type, self._charset = split_content_type_header(
                self.response_headers.get(CONTENT_TYPE_HEADER, ''))

        return self._content_type, self._charset

    @property
    def expires(self):
        if not self._expires:
            expires_header = self.response_headers.get(EXPIRES_HEADER, None)
            if expires_header:
                self._expires = parse_http_timestamp(expires_header)
        return self._expires

    @property
    def last_modified(self):
        if not self._last_modified:
            last_modified_header = self.response_headers.get(LAST_MODIFIED_HEADER, None)
            if last_modified_header:
                self._last_modified = parse_http_timestamp(last_modified_header)
        return self._last_modified

    @property
    def referrer(self):
        return self.response_headers.get(REFERRER_HEADER, ('http://' + self.referrer_id) if self.referrer_id else None)

    @property
    def etag(self):
        return self.response_headers.get(ETAG_HEADER, None)

    @property
    def is_expired(self):
        if not self.expires:
            return True
        return self.expires <= time.time()

    @property
    def use_cached(self):
        """
        :return: True if the resource has probably not changed since it was last fetched, otherwise returns False.
        """
        if not self.fetched:
            return False

        if not self.is_expired:
            return True

        if self.last_modified:
            # Stagnant resources are more likely to have not changed since the last time they were fetched.
            now = time.time()
            # The resource is considered stagnant if it hasn't changed in over ten times the
            # length of time since it was lasted fetched.
            is_stagnant = (now - self.last_modified) > (CACHE_STAGNATION_FACTOR * (now - self.fetched))
            return is_stagnant

        # There is no reason to believe that the cached copy is up-to-date.
        return False

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def __cmp__(self, other):
        return self.id.__cmp__(other.id)

    def __ne__(self, other):
        return self.id != other.id

    def __str__(self):
        return self.raw_url
