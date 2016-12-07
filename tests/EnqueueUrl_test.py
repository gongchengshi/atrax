from crawler import CrawlerInstance, UrlInfo

crawler = CrawlerInstance(1002, 'siemens17042013', 2)

crawler.EnqueueUrl(UrlInfo('https://www.automation.siemens.com/FR/forum/guests/PostShow.aspx?Language=en&PageIndex=1&PostID=92681'))
