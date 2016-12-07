import aws
from ec2.helpers import CreateSdbDomain

sdb = aws.sdb()

testTable = CreateSdbDomain(sdb, 'test')

input = {}

input['0'] = {'list': ['a', 'b', 'c']}
input['1'] = {'list': ['d']}
input['2'] = {'list': ['e', 'f']}

testTable.batch_put_attributes(input, False)

rows = testTable.select("select * from `test`")

for row in rows:
    print row
