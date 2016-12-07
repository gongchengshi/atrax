from atrax.common.crawl_scope import CrawlerScope
from atrax.common.url_info import UrlInfo
from python_common.simple_list_file import SimpleListFile

target = CrawlerScope(SimpleListFile('/usr/local/crawl_jobs/siemens23012015/siemens23012015.scope'))

actual = target.get(UrlInfo(u'http://www.douban.com/recommend/?title=Traditional%20Chinese%20medicine&url=http://w1.siemens.com.cn/sustainable-city-en/sustainable-city.html%23tcm-healthcare'))

i = 9
