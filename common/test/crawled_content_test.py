import unittest
import urllib

import requests

from atrax.common.crawl_job import CrawlJobGlossary
from atrax.common.sdb_url_info import unpack_sdb_url_info
from atrax.common.url_info import UrlInfo
from aws import USWest2 as AwsConnections
from atrax.common.crawled_content import CrawledContent
from aws.s3 import get_or_create_bucket


class CrawledContentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._bucket = get_or_create_bucket(AwsConnections.s3(), 'crawled-content-test-bucket')
        sdb = AwsConnections.sdb()
        cls._crawled_urls = sdb.lookup(CrawlJobGlossary('sel11122014').crawled_urls_table_name)
        cls._target = CrawledContent(cls._bucket)
        cls._content = "yada yada yada"

    def test_put(self):
        url_info = UrlInfo('http://www.example.com/bogus.html')
        self._target.put(url_info, self._content)
        
        m = self._crawled_urls.get_item('www2.selinc.com/robots.txt')
        url_info = unpack_sdb_url_info(m)
        self._target.put(url_info, self._content)

        m = self._crawled_urls.get_item('lojavirtual.selinc.com.br/')
        url_info = unpack_sdb_url_info(m)
        self._target.put(url_info, self._content)

        m = self._crawled_urls.get_item('www.selinc.com/assets/0/114/234/236/f9895377-e729-4242-bf6e-2cf76fdcdf58.pdf#page%3D2')
        url_info = unpack_sdb_url_info(m)
        self._target.put(url_info, self._content)

    def test_response_headers(self):
        base_url = 'http://crawled-content-test-bucket.s3-website-us-east-1.amazonaws.com/'
        r = requests.get(base_url + 'lojavirtual.selinc.com.br')
        print r.headers
        r = requests.get(base_url + 'www2.selinc.com/robots.txt')
        print r.headers
        url = base_url + urllib.quote('www.selinc.com/assets/0/114/234/236/f9895377-e729-4242-bf6e-2cf76fdcdf58.pdf#page%3D2')
        r = requests.get(url)
        print r.headers

    def test_get(self):
        content = self._target.get('lojavirtual.selinc.com.br')
        self.assertEqual(content, self._content)
        content = self._target.get('www2.selinc.com/robots.txt')
        self.assertEqual(content, self._content)
        content = self._target.get('www.selinc.com/assets/0/114/234/236/f9895377-e729-4242-bf6e-2cf76fdcdf58.pdf#page%3D2')
        self.assertEqual(content, self._content)
