#from ExtractUrls import *
#
#urlInfo = UrlInfo()
#urlInfo.url = "http://www.example.com/www.selinc.com.mx/css/default.css"
#
#input = open('TestInput/ExtractUrlsFromCss.css', 'r')
#content = input.read()
#
#urls = ExtractUrlsFromCss(urlInfo, content, None)

import urllib2
from ExtractUrls import *

url = 'http://news.usa.siemens.biz/print/node/940'
resp = urllib2.urlopen(url)
content = resp.read()
urlInfo = UrlInfo('http://news.usa.siemens.biz/print/node/940')
urlExtractor = UrlExtractor(None)
urlInfos = urlExtractor.extract_urls_from_html(urlInfo, content)

i = 9