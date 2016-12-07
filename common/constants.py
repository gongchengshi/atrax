MAX_REDIRECTS = 20

# Simple DB Attribute Names
REFERRER_ID_ATTR_NAME = 'referrer_id'
REASON_ATTR_NAME = 'reason'
FINGERPRINT_ATTR_NAME = 'fingerprint'
ORIGINAL_ATTR_NAME = 'original'
ORIGINAL_ATTR_VALUE_SELF = 'self'

# S3 Key Metadata Fields
ORIGINAL_URL_ATTR_NAME = 'original_url'

# User Agent Names
GOOGLEBOT_USER_AGENT_NAME = 'GoogleBot'
USER_AGENT_PATTERN = "(Mozilla/5.0 (compatible; %s/1.0; ID %s)"

CACHE_STAGNATION_FACTOR = 10

DEFAULT_REDIS_PORT = 6379
LOCALHOST_IP = '127.0.0.1'

CRAWL_JOB_STATE_DOMAIN_NAME = 'crawl_job_state'


class Databases:
    REDIS = "Redis"
    DYNAMO_DB = "DynamoDB"
    SIMPLE_DB = "SimpleDB"


class RobotExclusionReasons:
    ROBOTS_TXT = "robots.txt"
    REL_NO_FOLLOW = "rel=nofollow"
    CONTENT_NO_INDEX = "content=noindex"
    CONTENT_NO_FOLLOW = "content=nofollow"
    CONTENT_NO_SNIPPET = "content=nosnippet"
    CONTENT_NO_ODP = "content=noodp"
    CONTENT_NO_ARCHIVE = "content=noarchive"
    CONTENT_UNAVAILABLE_AFTER = "content=unavailable_after"
    # CONTENT_NO_IMAGE_INDEX = "content=noimageindex" # not tracked by our crawlers
    X_ROBOTS_TAG_NO_INDEX = "X-Robots-Tag:noindex"
    X_ROBOTS_TAG_NO_FOLLOW = "X-Robots-Tag:nofollow"
    X_ROBOTS_TAG_NO_SNIPPET = "X-Robots-Tag:nosnippet"
    X_ROBOTS_TAG_NO_ODP = "X-Robots-Tag:noodp"
    X_ROBOTS_TAG_NO_ARCHIVE = "X-Robots-Tag:noarchive"
    X_ROBOTS_TAG_UNAVAILABLE_AFTER = "X-Robots-Tag:unavailable_after"
    # X_ROBOTS_TAG_NO_IMAGE_INDEX = "X-Robots-Tag:noimageindex" # not tracked by our crawlers


CONTENT_BUCKET_POLICY_FORMAT = """{{
                                        "Version": "2012-10-17",
                                        "Statement": [
                                            {{
                                                "Sid": "PublicReadGetObject",
                                                "Effect": "Allow",
                                                "Principal": "*",
                                                "Action": [ "s3:GetObject" ],
                                                "Resource": "arn:aws:s3:::{0}/*"
                                            }},
                                            {{
                                                "Sid": "PublicListObjects",
                                                "Effect": "Allow",
                                                "Principal": "*",
                                                "Action": [ "s3:ListBucket" ],
                                                "Resource": "arn:aws:s3:::{0}"
                                            }}
                                        ]
                                    }}
                                    """
