import requests

r = requests.get('http://proxy.crawlera.com:8010/fetch?url=https://www.selinc.com', auth=('jmclain', 'Ev9mohge'))

print r.text
print r.url
print r.status_code

exit()

import urllib2
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='urllib2')

pwmgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
pwmgr.add_password('Crawlera', 'http://proxy.crawlera.com:8010', 'jmclain', 'Ev9mohge')

opener = urllib2.build_opener(urllib2.HTTPBasicAuthHandler(pwmgr))

resp = opener.open('http://proxy.crawlera.com:8010/fetch?url=https://www.selinc.com')

content = resp.read()
print content
print resp.url
print resp.code
