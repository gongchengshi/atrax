import os

from boto.s3.key import Key
from boto.exception import S3ResponseError

from atrax.common.job_config_file import ConfigFile
from aws import USWest2 as AwsConnections
from python_common.simple_list_file import SimpleListFile
from python_common.web.http_utils import sec_since_epoch_to_http_date
from atrax.management.aws_env.constants import CONFIG_BUCKET_NAME, CRAWL_JOB_DIR_NAME, LOCAL_CRAWL_JOB_DIR


class ConfigFetcher:
    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        s3 = AwsConnections.s3()
        self.bucket = s3.lookup(CONFIG_BUCKET_NAME)

    def download_from_s3(self, remote_path, local_path):
        local_dir = os.path.dirname(local_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        k = Key(self.bucket, remote_path)
        try:
            headers = None
            if os.path.exists(local_path):
                last_modified = sec_since_epoch_to_http_date(os.path.getmtime(local_path))
                headers = {'If-Modified-Since': last_modified}

            # Must use this instead of k.get_contents_as_filename to avoid having the file deleted.
            # Todo: Log an issue with Boto: Don't delete the file if there is an exception if the file already existed.
            # See Boto issue:
            contents = k.get_contents_as_string(headers=headers)
            with open(local_path, 'wb') as fp:
                fp.write(contents)
        except S3ResponseError as ex:
            if ex.status != 304:
                raise

    def get_crawl_job_file(self, extension):
        remote_path = '{0}/{1}/{1}.{2}'.format(CRAWL_JOB_DIR_NAME, self.crawl_job_name, extension)
        local_path = '{0}/{1}/{1}.{2}'.format(LOCAL_CRAWL_JOB_DIR, self.crawl_job_name, extension)
        self.download_from_s3(remote_path, local_path)
        return local_path

    def get_config_file(self):
        return ConfigFile(self.get_crawl_job_file('job'))

    def get_scope_file(self):
        return SimpleListFile(self.get_crawl_job_file('scope'))

    def get_seeds_file(self):
        self.get_crawl_job_file('seeds')
