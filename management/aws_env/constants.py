# EC2 Constants
EC2_KEY_PAIR_NAME = 'selkey'
FETCHER_SECURITY_GROUP_NAME = 'AtraxFetcherSecurityGroup'
FRONTIER_SECURITY_GROUP_NAME = 'AtraxFrontierSecurityGroup'
REDIS_SECURITY_GROUP_NAME = 'RedisSecurityGroup'
VELUM_SECURITY_GROUP_NAME = 'VelumSecurityGroup'

COMMANDER_BASE_IMAGE_ID = 'ami-e7b8c0d7'  # Ubuntu Server 14.04 LTS (HVM), SSD Volume Type
# ubuntu/images/ubuntu-trusty-14.04-amd64-server-20140724 SSD Instance Store Backed
# PARAVIRTUAL_FETCHER_BASE_IMAGE_ID = 'ami-eb81f9db'
PARAVIRTUAL_FETCHER_BASE_IMAGE_ID = 'ami-37501207'  # Ubuntu Server 14.04 LTS (PV), SSD Volume Type
HVM_FETCHER_BASE_IMAGE_ID = 'ami-e7b8c0d7'  # Ubuntu Server 14.04 LTS (HVM), SSD Volume Type
FRONTIER_BASE_IMAGE_ID = 'ami-e7b8c0d7'  # Ubuntu Server 14.04 LTS (HVM), SSD Volume Type
DEFAULT_BASE_IMAGE_ID = 'ami-e7b8c0d7'  # Ubuntu Server 14.04 LTS (HVM), SSD Volume Type

FRONTIER_INSTANCE_TYPE = 'm3.medium'
FETCHER_INSTANCE_TYPES = ['m3.medium', 'm1.medium']  # In order of preference

AVAILABILITY_ZONES = ['us-west-2a', 'us-west-2b', 'us-west-2c']

NAME_TAG_NAME = 'Name'
PACKAGES_TAG_NAME = 'Packages'
CRAWL_JOB_TAG_NAME = 'CrawlJob'
CREATED_TAG_NAME = 'Created'

PACKAGE_DIR_NAME = 'packages'
CRAWL_JOB_DIR_NAME = 'crawl_jobs'
CONFIG_BUCKET_NAME = 'atrax-configuration-management'
SPOT_INSTANCE_ARN = 'arn:aws:iam::610509431426:instance-profile/atrax-spot-instance'
STANDARD_INSTANCE_ARN = 'arn:aws:iam::610509431426:instance-profile/atrax-standard-instance'

LOCAL_CRAWL_JOB_DIR = '/usr/local/crawl_jobs'


class ModuleNames:
    REDIS = 'redis'
    FRONTIER = 'frontier'
    FETCHER = 'fetcher'
    VELUM = 'velum'
