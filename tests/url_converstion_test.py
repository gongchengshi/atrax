import re

subjects = [
    "http://www.example.com/a.pdf?p=1234Abcd",
    "http://www.example.com/a.pdf?x=1234Abcd&p=1234Abcd",
    "http://www.example.com/a.pdf?x=1234Abcd&p=1234Abcd&y=1234Abcd",
    "http://www.example.com/a.pdf?p=1234Abcd&y=1234Abcd",
    "http://www.example.com/a.pdf?page=1234Abcd",
    "http://www.example.com/a.pdf?x=1234Abcd&page=1234Abcd",
    "http://www.example.com/a.pdf?x=1234Abcd&page=1234Abcd&y=1234Abcd",
    "http://www.example.com/a.pdf?page=1234Abcd&y=1234Abcd"]

r = re.compile("(?P<pdf>\.pdf.*)p(age)?=.*?(&|$)")
for subject in subjects:
    result = r.sub(r"\g<pdf>", subject)
    print result
print "----------------------------"

subject = "http://health.siemens.com/ct_applications/somatomsessions/?p=5650&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1&print=1"

r = re.compile("print=1?(&|$)")
result = r.sub('', subject)
print result
print "----------------------------"

subjects = [
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?prnt=True",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?Language=de&prnt=True&UserID=110221",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?prnt=True&UserID=110221&Language=de",
    "http://www.automation.siemens.com/forum/guests/UserProfile.aspx?UserID=110221&Language=de&prnt=True"]

r = re.compile("prnt=True?(&|$)")

for subject in subjects:
    result = r.sub('', subject)
    print result
print "----------------------------"
