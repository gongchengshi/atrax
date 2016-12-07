from aws import USWest2 as AwsConnections
import aws.s3
from aws.ec2 import wait_for_state
from aws.sqs import persist_to_s3, restore_from_s3

from atrax.common.crawl_job import CrawlJobGlossary, CrawlJob
from atrax.common.url_info import UrlInfo
from atrax.common.constants import *
from atrax.common.crawl_scope import CrawlerScope, UrlClass
from atrax.frontier.remote_frontier_client import get_frontier_client
from atrax.frontier.frontier_queue import FrontierQueue
from atrax.management.constants import InstanceState
from atrax.management.exceptions import PreconditionNotMet
from atrax.management.config_fetcher import ConfigFetcher
from atrax.management.aws_env.constants import *
from atrax.management.aws_env.user_data_factory import generate_upstart_script, create_multipart, \
    generate_cloud_config, generate_stopgap_debian_setup
from atrax.management.aws_env.instance_accessor import AwsInstanceAccessor
from atrax.management.aws_env.ami import get_latest_frontier_ami


class FrontierController:
    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        self.ec2_instance = AwsInstanceAccessor(self.crawl_job_name).get_frontier_instance()

    def start(self, availability_zone=None):
        if self.ec2_instance is None or self.ec2_instance.state in [InstanceState.TERMINATED,
                                                                    InstanceState.SHUTTING_DOWN]:
            self.ec2_instance = self.create_instance(availability_zone)
        else:
            if self.ec2_instance.state == InstanceState.STOPPED:
                self.ec2_instance.start()
            elif self.ec2_instance.state == InstanceState.STOPPING:
                wait_for_state(self.ec2_instance, [InstanceState.STOPPED], sleep_seconds=10)
                self.ec2_instance.start()
            # Other wise it is in the pending state and is already created and starting up

    def pause(self):
        if self.ec2_instance:
            self.ec2_instance.stop()
            wait_for_state(self.ec2_instance, [InstanceState.STOPPED], sleep_seconds=10)

    def stop(self):
        self.pause()
        self.persist()

    def destroy(self):
        if self.ec2_instance:
            self.ec2_instance.terminate()
        self.delete_queues()
        self.delete_persisted()

    def persist(self):
        if self.ec2_instance is not None and self.ec2_instance.state in [InstanceState.RUNNING, InstanceState.PENDING]:
            raise PreconditionNotMet('Frontier is still in the %s state' % self.ec2_instance.state)

        crawl_job = CrawlJob(self.crawl_job_name)

        # Persist all queues with names that start with the crawl_job.name
        persist_to_s3(AwsConnections.sqs(), crawl_job.name, crawl_job.persisted_frontier_bucket)

    def restore(self):
        crawl_job = CrawlJob(self.crawl_job_name)
        restore_from_s3(AwsConnections.sqs(), crawl_job.persisted_frontier_bucket,
                        queue_creator=FrontierQueue.create_sqs_queue)

    def delete_persisted(self):
        aws.s3.delete_non_empty_bucket(CrawlJobGlossary(self.crawl_job_name).persisted_frontier_bucket_name)

    def delete_queues(self):
        queues = AwsConnections.sqs().get_all_queues(self.crawl_job_name)
        for queue in queues:
            queue.delete()

    def create_instance(self, availability_zone=None):
        modules = [ModuleNames.FRONTIER, ModuleNames.REDIS]
        security_groups = [FRONTIER_SECURITY_GROUP_NAME,
                           REDIS_SECURITY_GROUP_NAME,
                           FETCHER_SECURITY_GROUP_NAME]
        parts = [
            ('cloud_config.yaml', 'cloud-config', generate_cloud_config()),
            ('stopgap_debian_setup.sh', 'x-shellscript', generate_stopgap_debian_setup())
        ]

        for module in modules:
            script = generate_upstart_script(self.crawl_job_name, module)
            parts.append((module + '.conf', 'upstart-job', script))

        user_data = create_multipart(parts)

        ec2 = AwsConnections.ec2()
        reservation = ec2.run_instances(image_id=get_latest_frontier_ami(ec2).id,
                                        key_name=EC2_KEY_PAIR_NAME, security_groups=security_groups,
                                        instance_type=FRONTIER_INSTANCE_TYPE, placement=availability_zone,
                                        monitoring_enabled=True, instance_initiated_shutdown_behavior='stop',
                                        instance_profile_arn=STANDARD_INSTANCE_ARN, user_data=user_data,
                                        disable_api_termination=True)  # This instance can only be terminated manually

        frontier_instance = reservation.instances[0]
        wait_for_state(frontier_instance, (InstanceState.PENDING, InstanceState.RUNNING))
        ec2.create_tags([frontier_instance.id],
                        {CRAWL_JOB_TAG_NAME: self.crawl_job_name,
                         PACKAGES_TAG_NAME: ' '.join(modules)})
        return frontier_instance

    def enqueue_skipped(self):
        # Todo: Not tested
        crawl_job = CrawlJob(self.crawl_job_name)
        scope = CrawlerScope(ConfigFetcher(self.crawl_job_name).get_scope_file())
        frontier = get_frontier_client(self.ec2_instance, None)
        for item in crawl_job.skipped_urls.select("select * from %s" % crawl_job.skipped_urls.name):
            url_info = UrlInfo(item.name)
            if scope.get(url_info) == UrlClass.InScope:
                url_info.referrer_id = item[REFERRER_ID_ATTR_NAME]
                frontier.enqueue(url_info)
                item.delete()  # Todo: do this in batches?
