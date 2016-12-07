from StringIO import StringIO
import sys
import os
import bz2
from boto.s3.key import Key
import pysolr
from pprint import pprint
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import aws
from atrax.CrawlJob import CrawlJob
from atrax.UrlInfo import *

crawlJob = CrawlJob('siemens17042013')

crawledUrls = aws.sdb().get_domain(crawlJob.CrawledUrlsTableName)
crawledContent = aws.s3().lookup(crawlJob.CrawledContentBucketName)

keyNames = {
    'cache.automation.siemens.com/dnl/TUzODQ5AAAA_25441475_Tools/MOP_new.txt': 'MOP_new.txt',
    'www.usa.siemens.com/entry/en/greencityindex.htm': 'greencityindex.htm',
    'www.siemens.com/annual/12/en/download/pdf/Siemens_AR2012_At-a-glance.pdf': 'Siemens_AR2012_At-a-glance.pdf',
    'w1.hearing.siemens.com/cn/10-about-us/news/_resources/2012.xls': '2012.xls',
    'www.siemens.com/innovation/pool/pof/pof-register_2001-2-2008_en.xls': 'pof-register_2001-2-2008_en.xls',
    'www.siemens.com/sustainability/pool/compliance/siemens-integrity-initiative_eoi_template.doc': 'siemens-integrity-initiative_eoi_template.doc',
    'mes-simaticit.siemens.com/tss/SupportWorkflowAnd@Service.ppt': 'SupportWorkflowAnd@Service.ppt',
    'www.industry.siemens.com.cn/home/cn/zh/customer-service/vs/sitrain/Pages/Siemens-e-book.mht': 'Siemens-e-book.mht'
}

baseSolrUrl = 'http://lpul-solr.ad.selinc.com:8080/solr/%s/' % crawlJob.Name
solr = pysolr.Solr(baseSolrUrl)

for keyName, fileName in keyNames.iteritems():

    whereClause = "itemName() = '%s'" % keyName
    fields = "`url`, `fetched`, `content-type`, `last-modified`, `content-length`, `responseHeaders`, `googleBotExclusionReasons`"
    query = "select %s from `%s` where %s" % (fields, crawledUrls.name, whereClause)

    try:
        items = crawledUrls.select(query)

        item = items.next()

        fetched = item['fetched']

        responseHeadersJson = item.pop('responseHeaders', None)
        responseHeadersJson = None if responseHeadersJson == 'None' or responseHeadersJson == '' \
            else responseHeadersJson
        responseHeaders = json.loads(responseHeadersJson) if responseHeadersJson else None

        if responseHeaders:
            contentLength = responseHeaders.get('content-length', None)
            contentType = responseHeaders.get('content-type', None)
            lastModified = responseHeaders.get('last-modified', None)
        else:
            contentLength = item.get('content-length', None)
            contentType = item.get('content-type', None)
            lastModified = item.get('last-modified', None)

        mimeType, charSet = SplitContentTypeHeader(contentType)

        k = Key(crawledContent, item.name)
        if not k.exists():
            pass

        contents = k.get_contents_as_string()
        if contents.startswith('BZh'):
            contents = bz2.decompress(contents)

        params = {'literal.id': item.name,
                  'literal.url': item['url'],
                  'literal.fetched': fetched,
                  # 'commit': 'true',
        }

        # if int(contentLength) > (2048 * 1000):
        #     files = (('file', contents),)

        r = requests.post(baseSolrUrl + 'update/extract?', params=params, data=contents)
        r.raise_for_status()

        i = 9

        # file = StringIO(contents)
        # file.name = item.name

        # outFile = open(fileName, 'w')
        # outFile.write(file.read())

        # data = solr.extract(file, extractOnly=True)
        # data = solr.extract(file, extractOnly=False)

        # pprint(data, outFile)
        # outFile.close()

    except Exception, ex:
        raise

solr.commit()

