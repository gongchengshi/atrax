from boto.s3.key import Key
from atrax.common import schemeless_url_to_s3_key
from atrax.common.constants import ORIGINAL_ATTR_NAME, ORIGINAL_URL_ATTR_NAME
from python_common.exceptions import IllegalArgumentError
from python_common.web.http_headers import *


class CrawledContent:
    HEADER_FIELDS = [CONTENT_TYPE_HEADER,
                     CONTENT_DISPOSITION_HEADER,
                     CONTENT_LANGUAGE_HEADER,
                     # LAST_MODIFIED_HEADER,
                     # ETAG_HEADER,
                     EXPIRES_HEADER]

    def __init__(self, bucket, compression='gzip'):
        self.bucket = bucket
        self.compression = compression
        if compression:
            if compression == 'gzip':
                from StringIO import StringIO
                import gzip

                def gzip_compress(c):
                    out_file = StringIO()
                    gzip.GzipFile(fileobj=out_file, mode="w").write(c)
                    return out_file.getvalue()
                self.compress = gzip_compress
                self.decompress = lambda c: gzip.GzipFile(fileobj=StringIO(c), mode='r').read()
            elif compression == 'deflate':
                # May not be completely compatible with deflate.
                # See http://newsgroups.derkeiler.com/Archive/Comp/comp.compression/2007-07/msg00011.html
                # Notably, .NET can't decompress a zlib stream using System.IO.Compression.DeflateStream
                import zlib
                self.compress = zlib.compress
                self.decompress = zlib.decompress
            else:
                raise IllegalArgumentError("`compression` must be 'gzip' or 'deflate' or None")
        else:
            self.compress = self.decompress = None

    def put(self, url_info, contents):
        k = Key(self.bucket, url_info.s3_key)

        k.set_metadata(ORIGINAL_URL_ATTR_NAME, url_info.url)
        k.set_metadata(ORIGINAL_ATTR_NAME, schemeless_url_to_s3_key(url_info.original))

        # Setting these doesn't seem to have any effect on the key attributes.
        # See https://github.com/boto/boto/issues/2798
        # k.cache_control = 'private'  # Use whatever S3 determines is the default. Probably 'private'
        # k.content_type = url_info.response_headers.get(CONTENT_TYPE_HEADER, None)
        # k.content_disposition = url_info.response_headers.get(CONTENT_DISPOSITION_HEADER, None)
        # k.content_language = url_info.response_headers.get(CONTENT_LANGUAGE_HEADER, None)
        # k.expiry_date = url_info.response_headers.get(EXPIRES_HEADER, None)
        # k.etag = url_info.response_headers.get(ETAG_HEADER, None)
        # k.last_modified = url_info.response_headers.get(LAST_MODIFIED_HEADER, None)

        # Do to the issue in boto these headers must be set using key.set_metadata()
        for header_name in CrawledContent.HEADER_FIELDS:
            value = url_info.response_headers.get(header_name, None)
            if value is not None:
                # Boto URL encodes special characters in metadata values if they are unicode.
                # See https://github.com/boto/boto/issues/2536 and https://github.com/boto/boto/issues/1469
                if type(value) is unicode:
                    value = value.encode('utf-8')
                k.set_metadata(header_name, value)

        if self.compress:
            k.set_metadata(CONTENT_ENCODING_HEADER, self.compression)
            # k.content_encoding = self.compression
            out_contents = self.compress(contents)
        else:
            out_contents = contents

        k.set_contents_from_string(out_contents, replace=True, reduced_redundancy=True)

    def get(self, key_name):
        k = Key(self.bucket, key_name)
        if not k.exists():
            return None
        content = k.get_contents_as_string()

        if self.decompress:
            return self.decompress(content)
        return content
