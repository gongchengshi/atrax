# coding=utf-8
from base64 import b64encode, b64decode
import pickle
import time

from atrax.common.url_info import UrlInfo


def decode_test():
    url = u'http://www.selinc.com.mx/pdf/papers/TP-2005-001 Aplicación de Relevadores Multifuncionales.pdf'

    encoded = url.encode('utf-8')

    if not isinstance(encoded, unicode):
        decoded = encoded.decode('utf-8')


def pack_message(url_info):
    return b64encode(pickle.dumps(url_info))

def unpack_message(m):
    return pickle.loads(b64decode(m))

from boto.sqs.message import RawMessage as SqsMessage
from aws import USWest2 as AwsConnections

queue = AwsConnections.sqs().lookup('test_queue')
queue.set_message_class(SqsMessage)


def dequeue():

    while True:
        received_msg = queue.read()
        if not received_msg:
            break
        received_body = received_msg.get_body()
        received_url_info = unpack_message(received_body)

        print received_url_info.raw_url
        received_msg.delete()


def sqs_test():
    url = u'http://www.selinc.com.mx/pdf/papers/TP-2005-001 Aplicación de Relevadores Multifuncionales.pdf'
    # url = u'http://www.selinc.com.mx/pdf/papers/'
    sent_url_info = UrlInfo(url)
    sent_body = pack_message(sent_url_info)
    sent_msg = SqsMessage(body=sent_body)
    queue.write(sent_msg)

    time.sleep(2)

    dequeue()


def sqs_batch_test():
    urls = [
        UrlInfo(u'http://www.selinc.com.mx/pdf/papers/TP-2005-001 Aplicación de Relevadores Multifuncionales.pdf'),
        UrlInfo(u'http://www.selinc.com.mx/pdf/papers/')
    ]
    batch = []

    for i in xrange(0, len(urls)):
        sent_body = pack_message(urls[i])
        # reminder: the body is sent not a message object. Bad: sent_msg = SqsMessage(body=sent_body)
        batch.append((i, sent_body, 0))

    queue.write_batch(batch)

    time.sleep(2)

    dequeue()


def sqs_batch_test2():
    msg_bodies = ['"Hello"', '"Hello"']
    batch = []

    for i in xrange(0, len(msg_bodies)):
        sent_body = b64encode(msg_bodies[i])
        # reminder: the body is sent not a message object. Bad: sent_msg = SqsMessage(body=sent_body)
        batch.append((i, sent_body, 0))

    queue.write_batch(batch)

    time.sleep(2)

    dequeue()

sqs_batch_test()
# sqs_test()
# sqs_batch_test2()
