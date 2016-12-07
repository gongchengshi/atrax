#!/usr/bin/python2
import argparse
import os
import sys

from python_common.interruptable_thread import InterruptableThread
from aws.sdb import get_or_create_domain
from aws import USWest2 as AwsConnections
from atrax.common.fetcher_id import create_frontier_id
from atrax.common.crawl_job import CrawlJobGlossary
from atrax.common.logger import SimpleLogger, LogType, LocalLogger
from atrax.common.crawl_job_notifications import CrawlJobNotifications
from atrax.frontier.aws_frontier import AwsFrontier
from atrax.frontier.local_frontier import LocalFrontier
from atrax.frontier.exceptions import SqsMessageRetentionException, FrontierEmpty
from atrax.frontier.remote_frontier import RemoteFrontier
from atrax.frontier.frontier_seeder import FrontierSeeder
from atrax.management.config_fetcher import ConfigFetcher
from atrax.management.constants import ComputeEnv
from atrax.management.aws_env.crawl_job_controller import CrawlJobController
from atrax.metrics_service import MetricsService


def main():
    local_logger = LocalLogger('frontier')
    local_logger.log(LogType.Info, 'Starting')

    if not __debug__:
        os.nice(-1)

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store_true')
    parser.add_argument('job', type=str)
    args = parser.parse_args()

    config_fetcher = ConfigFetcher(args.job)
    config_file = config_fetcher.get_config_file()

    logger = SimpleLogger(
        get_or_create_domain(AwsConnections.sdb(), CrawlJobGlossary(args.job).logs_table_name),
        create_frontier_id(config_file.global_config.environment))
    try:
        if config_file.global_config.environment == ComputeEnv.AWS:
            frontier = AwsFrontier(args.job, logger)
        else:
            frontier = LocalFrontier(args.job, logger)

        seeder = FrontierSeeder(config_file.global_config, frontier)
        seeder_thread = InterruptableThread(lambda t: seeder.run())
        seeder_thread.start()

        metrics_service = MetricsService(args.job, 10)
        metrics_service.start()

        frontier_service = RemoteFrontier(frontier)
        frontier_service.start()
        logger.log(LogType.Info, 'Started')
        frontier_service.join()
        if frontier_service.threw_exception:
            logger.log(LogType.InternalError, 'Unexpectedly stopped',
                       None, frontier_service.exception, frontier_service.exc_info)
    except SqsMessageRetentionException, ex:
        logger.log(LogType.InternalWarning, "Full-stopping crawl job", None, ex, sys.exc_info())
        CrawlJobController(args.job).stop()
    except FrontierEmpty, ex:
        logger.log(LogType.Info, 'Finished')
    except Exception, ex:
        logger.log(LogType.InternalError, 'Unexpectedly stopped', None, ex, sys.exc_info())
    finally:
        notifier = CrawlJobNotifications(args.job)
        logger.log(LogType.Info, 'Stopped')
        notifier.frontier_stopped()
        local_logger.log(LogType.Info, 'Exited')

if __name__ == "__main__":
    main()
