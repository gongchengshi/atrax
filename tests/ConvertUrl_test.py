from atrax.UrlInfo import UrlInfo
from atrax.CrawlerInstance import CrawlerInstance

crawler = CrawlerInstance('1001', 'siemens17042013')

urls = [
    "http://www.example.com/a.pdf?p=1234",
    "http://www.example.com/a.pdf?x=1234Abcd&p=1234",
    "http://www.example.com/a.pdf?x=1234Abcd&p=1234&y=1234Abcd",
    "http://www.example.com/a.pdf?p=1234Abcd&y=1234",
    "http://www.example.com/a.pdf?page=1234",
    "http://www.example.com/a.pdf?x=1234Abcd&page=1234",
    "http://www.example.com/a.pdf?x=1234Abcd&page=1234&y=1234Abcd",
    "http://www.example.com/a.pdf?page=1234&y=1234Abcd",
    "http://health.siemens.com/ct_applications/somatomsessions/?p=5650&print=1&print=1&print=1&print=1",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?prnt=True",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?Language=de&prnt=True&UserID=110221",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?prnt=True&UserID=110221&Language=de",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?UserID=110221&Language=de&prnt=True"
]

for url in urls:
    urlInfo = UrlInfo(url)
    crawler.ConvertUrl(urlInfo)
    print urlInfo.url
