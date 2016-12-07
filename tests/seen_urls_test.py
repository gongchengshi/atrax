import unittest
from atrax.common.url_info import UrlInfo
from atrax.common.seen_urls import SeenUrls


class SeenUrlsTest(unittest.TestCase):
    urls = [
        'http://www.example.com/',
        'http://www.example.com',
        'http://www.example.com/one/two',
        'http://www.foo.com',
        'http://www.foo.com/',
        'http://www.foo.com/one/two'
    ]

    def test_add_contains(self):
        target = SeenUrls('base_key')

        target.add(UrlInfo('http://www.example.com'))
        self.assertIn(UrlInfo('http://www.example.com/'), target)
        self.assertIn(UrlInfo('http://www.example.com'), target)
        target.add(UrlInfo('http://www.example.com/'))

        self.assertNotIn(UrlInfo('http://www.example.com/one'), target)

        target.add(UrlInfo('http://www.example.com/one/two'))
        target.add(UrlInfo('http://www.foo.com'))
        target.add(UrlInfo('http://www.foo.com/'))
        target.add(UrlInfo('http://www.foo.com/one/two'))

        self.assertIn(UrlInfo('http://www.example.com/one/two'), target)
        self.assertIn(UrlInfo('http://www.foo.com/one/two'), target)
        self.assertIn(UrlInfo('http://www.foo.com'), target)
        self.assertIn(UrlInfo('http://www.foo.com/'), target)
