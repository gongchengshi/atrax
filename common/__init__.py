def schemeless_url_to_s3_key(schemeless_url):
    # trailing slashes cannot be used in S3 keys that contain content.
    return schemeless_url.rstrip('?/')

import re
end_of_domain_name_pattern = re.compile('[?/:]')


def domain_from_schemeless_url(schemeless_url):
    return end_of_domain_name_pattern.split(schemeless_url, 1)[0]
