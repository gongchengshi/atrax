import urllib
import urllib2
from urlparse import urljoin

def CreateUrlOpener():
    class CrawleraRedirectHandler(urllib2.HTTPRedirectHandler):
        prepend = 'http://proxy.crawlera.com:8010/fetch?url='

        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            if not newurl.startswith(self.prepend):
                origSource = req.get_full_url()[len(self.prepend):]
                newurl = urllib.quote(urljoin(urllib.unquote(origSource), hdrs['location']))

            return urllib2.Request(newurl)

    class CrawleraHandler(urllib2.BaseHandler):
        prepend = 'http://proxy.crawlera.com:8010/fetch?url='

        def http_request(self, request):
            url = request.get_full_url()
            if url.startswith(self.prepend):
                return request

            req = urllib2.Request(self.prepend + urllib.quote(url))
            if hasattr(request, 'timeout'):
                req.timeout = request.timeout
            return req

        def http_response(self, request, response):
            if response.url.startswith(self.prepend):
                response.url = urllib.unquote(response.url[len(self.prepend):])
            return response

        https_response = http_response
        https_request = http_request

    pwmgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    pwmgr.add_password('Crawlera', 'http://proxy.crawlera.com:8010', 'jmclain', 'Ev9mohge')

    urlOpener = urllib2.build_opener(CrawleraHandler(),
                                     urllib2.HTTPBasicAuthHandler(pwmgr), CrawleraRedirectHandler())
    return urlOpener

