from atrax.fetcher.downloaders.downloaders import ProxyDownloader


UserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36"


def test_proxy_downloader1():
    ua = UserAgent
    # d = ProxyDownloader(ua, 'http://10.16.26.224:8888')
    # d = ProxyDownloader(ua, 'http://127.0.0.1:8888')
    d = ProxyDownloader(ua, '127.0.0.1:8888')

    r = d.download('http://www.wikipedia.org/')

    print r


import requests


def test_proxy_https_request():
    proxiesDict = {
        'http': "http://wall.ad.selinc.com:8080",
        'https': "http://wall.ad.selinc.com:8080"
    }

    #r = requests.get("https://www.paypal.com/", proxies=proxiesDict, verify=False, allow_redirects=False)
    #r = requests.get("https://www.paypal.com/home", proxies=proxiesDict)
    #r = requests.get("http://en.wikipedia.org/wiki/", proxies=proxiesDict)
    #r = requests.get("https://www.usbank.com/", proxies=proxiesDict, verify=False, allow_redirects=False)
    #r = requests.get("https://www.att.com/olam/", proxies=proxiesDict, verify=False)
    r = requests.get("http://www.keybank.com/", proxies=proxiesDict, verify=False, allow_redirects=False)

    print r


import urllib3
import unittest


class TestProxyConnect(unittest.TestCase):
    def setUp(self):
        self.proxy_url = 'http://wall.ad.selinc.com:8080/'
        self.https_url = 'https://www.paypal.com/home'

    def test_connect(self):
        proxy = urllib3.proxy_from_url(self.proxy_url)
        res = proxy.urlopen('GET', self.https_url)
        self.assertEqual(res.status, 200)


def test_proxy_downloader2():
    d = ProxyDownloader(UserAgent, 'wall.ad.selinc.com:8080')
    r = d.download('http://www.example.com/', None)
    return r

from atrax.fetcher.downloaders.downloaders import GenericDownloader


class TestGenericDownloader(unittest.TestCase):
    def test_connect(self):
        pass


def test_generic_downloader():
    d = GenericDownloader(UserAgent)
    r = d.download('http://www.example.com/', None)
    return r

from atrax.fetcher.downloaders.pass_through_proxy_downloader import PassThroughProxyDownloader


def test_pass_through_proxy_downloader():
    d = PassThroughProxyDownloader(UserAgent, 'wall.ad.selinc.com', 8080)
    r = d.download('http://www.example.com/', None)
    return r


def test_generic_downloader2():
    d = GenericDownloader(UserAgent)
    r = d.download('https://www.cia.gov/index.html', None)
    return r


def test_pass_through_proxy_downloader2():
    d = PassThroughProxyDownloader(UserAgent, 'localhost', 8084)
    r = d.download('http://www.example.com/', None)
    #r = d.download('https://www.eff.org/', None)
    #r = d.download('https://www.cia.gov/index.html', None)
    return r

if __name__ == '__main__':
    #test_pass_through_proxy_downloader()
    #test_pass_through_proxy_downloader2()
    #test_generic_downloader2()
    # test_generic_downloader()
    test_proxy_downloader1()
    #test_proxy_downloader2()
    #test_proxy_https_request()
    #unittest.main()
