from aws import USWest2 as AwsConnections

domain_name = 'crawled-urls.siemens17042013'
domain = AwsConnections.sdb().lookup(domain_name)
query = "select * from `{0}` where `redirectsTo` is null".format(domain_name)
items = domain.select(query)

count = 0

for item in items:
    # print item.name + '\n'
    count += 1
    next_token = items.next_token
    if next_token is not None:
        print next_token
        break

print '\n' + str(count)
