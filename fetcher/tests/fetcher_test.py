from atrax.common.url_info import UrlInfo
from atrax.fetcher.fetcher import Fetcher


def test_seen_urls():
    fetcher = Fetcher('sel09292014')

    url_info = UrlInfo('selinc.lojablindada.com/customer/account/login/')

    # url_info = UrlInfo('http://www.yadayadayada.com/2')

    # fetcher.global_seen_urls.add(url_info.id, url_info.sld)
    # fetcher.mark_url_seen(url_info)

    # url_info = UrlInfo('http://www.yadayadayada.com/2')

    print fetcher.url_is_seen(url_info)

# test_seen_urls()


def test_download_resource():
    fetcher = Fetcher('sel11032014')
    parser = fetcher.download_robots_txt('www.usbank.com')
    fetcher.extract_sitemaps(parser.sitemaps, 'www.usbank.com')
    i = 0

# test_download_resource()


def test_get_original():
    fetcher = Fetcher('siemens20150201')

    url_info = UrlInfo('http://' + 'www.siemens.com.ph/_framework/slt/widget/tabbedpanels/sprytabbedpanels.css')
    # url_info.fingerprint = '7fd8d4e6e43f454d9b60c9d7e246d3ac8bc6468b'
    url_info.fingerprint = '227e04c77d1476708086800b7a4da8cc6f997fea'

    orig = fetcher.get_original(url_info)

    print orig.name

test_get_original()
