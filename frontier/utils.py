import re

legal_sqs_char_pattern = re.compile(r'[^a-zA-Z0-9-_]')


def build_queue_name(crawl_job, key):
    return '%s-%s' % (crawl_job, legal_sqs_char_pattern.sub('_', key))

import socket


class QueueKeyDict:
    def __init__(self):
        self._keys = {}

    def __getitem__(self, url_info):
        try:
            key = self._keys[url_info.domain]
        except KeyError:
            try:
                key = socket.gethostbyname(url_info.domain)
            except socket.gaierror:
                key = url_info.domain
            self._keys[url_info.domain] = key

        return key
