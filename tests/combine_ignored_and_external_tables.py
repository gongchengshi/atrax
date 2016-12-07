from CrawlJob import CrawlJob
import aws
from atrax.constants import ReferrerAttrName, ReasonAttrName, CrawlPriority

crawlJob = CrawlJob('siemens17042013')

sdb = aws.sdb()
skippedUrlsTable = sdb.get_domain(crawlJob.SkippedUrlsTableName)

ignoredUrlsTable = sdb.lookup('ignored-urls.' + crawlJob.Name)

skippedItems = {}
if ignoredUrlsTable:
    i = 0
    items = ignoredUrlsTable.select("select * from `ignored-urls.%s`" % crawlJob.Name)
    for item in items:
        skippedItems[item.name] = {ReferrerAttrName: item['referrer'], ReasonAttrName: CrawlPriority.Ignore}
        i += 1
        if i == 25:
            skippedUrlsTable.batch_put_attributes(skippedItems)
            i = 0
            skippedItems = {}

if len(skippedItems) > 0:
    skippedUrlsTable.batch_put_attributes(skippedItems)


externalUrlsTable = sdb.lookup('external-urls.' + crawlJob.Name)
skippedItems = {}
if externalUrlsTable:
    i = 0
    items = externalUrlsTable.select("select url,  referrer from `external-urls.%s`" % crawlJob.Name)
    for item in items:
        skippedItems[item['url']] = {ReferrerAttrName: item['referrer'], ReasonAttrName: CrawlPriority.External}
        i += 1
        if i == 25:
            skippedUrlsTable.batch_put_attributes(skippedItems)
            i = 0
            skippedItems = {}

if len(skippedItems) > 0:
    skippedUrlsTable.batch_put_attributes(skippedItems)