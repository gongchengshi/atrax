from atrax.common.seen_urls import SeenUrls

seen_urls = SeenUrls('test')
seen_urls.add('www.selinc.com/', 'selinc.com')

print ('www.selinc.com/', 'selinc.com') in seen_urls

seen_urls.save()


