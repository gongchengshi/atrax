import json
from atrax.common.exceptions import IllegalDataType
from aws.sdb import truncate_attribute_from_end
from datetime import datetime
from atrax.common.constants import ORIGINAL_ATTR_NAME
from atrax.common.url_info import UrlInfo


def sdb_attribute_too_long(value):
    return value and (type(value) is unicode or type(value) is str) and len(value) > 1024


def trim_sdb_attribute(value):
    return truncate_attribute_from_end(value) if sdb_attribute_too_long(value) else value


def trim_sdb_attributes(d):
    # make sure that no attribute value exceeds 1024 by truncating it.
    for key, value in d.iteritems():
        if sdb_attribute_too_long(value):
            d[key] = truncate_attribute_from_end(value)


def pack_sdb_object(u, fields):
    d = {}

    for field, data_type in fields.iteritems():
        value = u.__dict__.get(field, None)
        if value is None:
            continue
        if data_type is unicode or data_type is float:
            d[field] = value
        elif data_type is list:
            d[field] = json.dumps(value)
        elif data_type is set:
            d[field] = json.dumps(list(value))
        elif data_type is dict:
            d[field] = d.update(value)
        elif data_type is datetime:
            d[field] = value.isoformat()
        else:
            raise IllegalDataType(data_type)

    trim_sdb_attributes(d)
    return d


import dateutil.parser


def unpack_sdb_object(m, fields):
    u = UrlInfo(m['raw_url'], m['url'])

    for field, data_type in fields.iteritems():
        if field in m:
            if data_type is unicode:
                u.__dict__[field] = m[field]
            elif data_type is float:
                u.__dict__[field] = float(m[field])
            elif data_type is list:
                u.__dict__[field] = json.loads(m[field])
            elif data_type is set:
                u.__dict__[field] = set(json.loads(m[field]))
            elif data_type is dict:
                d = {}

                for name, value in m.iteritems():
                    if name in fields.keys():
                        continue
                    d[name] = value
                u.__dict__[field] = d
            elif data_type is datetime:
                u.__dict__[field] = dateutil.parser.parse(m[field])
            else:
                raise IllegalDataType(data_type)

    return u


url_fields = {'response_headers': dict,
              'raw_url': unicode, 'url': unicode, 'fetcher': unicode,
              'discovered': float, 'requested': float, 'fetched': float,
              'referrer_id': unicode, 'http_status': unicode,
              'fingerprint': unicode, ORIGINAL_ATTR_NAME: unicode,
              'robot_exclusion_reasons': set, 'googlebot_exclusion_reasons': set}


def pack_sdb_url_info(u):
    return pack_sdb_object(u, url_fields)


def unpack_sdb_url_info(m):
    return unpack_sdb_object(m, url_fields)


redirected_url_fields = {'response_headers': dict,
                         'raw_url': unicode, 'url': unicode, 'fetcher': unicode,
                         'discovered': float, 'requested': float, 'fetched': float,
                         'referrer_id': unicode, 'http_status': unicode, 'redirects_to': unicode,
                         'robot_exclusion_reasons': set, 'googlebot_exclusion_reasons': set}


def pack_sdb_redirected_url_info(u):
    return pack_sdb_object(u, redirected_url_fields)


def unpack_sdb_redirected_url_info(m):
    return unpack_sdb_object(m, redirected_url_fields)
