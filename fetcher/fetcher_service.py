#!/usr/bin/python2
import argparse
import sys

from atrax.common.logger import LocalLogger, LogType
from atrax.fetcher.fetcher import Fetcher
from atrax.fetcher.fetcher_process import FetcherProcess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--daemonize', action='store_true')
    parser.add_argument('-e', '--env', required=False, default='local', choices=['aws', 'local'])
    parser.add_argument('--id', type=int, default=0, nargs='?')
    parser.add_argument('-c', '--proc_count', type=int, default=0, nargs='?')
    parser.add_argument('job', type=str)
    args = parser.parse_args()

    local_logger = LocalLogger('fetcher')
    try:
        if args.daemonize:
            if args.proc_count > 1:
                fetcher_processes = [FetcherProcess(args.job, i) for i in xrange(0, args.proc_count)]
                for process in fetcher_processes:
                    process.start()

                for process in fetcher_processes:
                    process.join()
            else:
                local_logger.log(LogType.Debug, "Starting fetcher thread")
                fetcher = Fetcher(args.job)
                fetcher.start()
                fetcher.join()
                if fetcher.threw_exception:
                    fetcher.logger.log(LogType.InternalError, 'Unexpectedly stopped',
                                       None, fetcher.exception, fetcher.exc_info)

            # if args.env == 'aws':
            #     from aws import USWest2 as AwsConnections
            #     from aws.ec2 import get_instance_id
            #     AwsConnections.ec2().terminate_instances([get_instance_id()])
        else:
            fetcher = Fetcher(args.job, args.id)
            fetcher.start()
            fetcher.join()
            if fetcher.threw_exception:
                fetcher.logger.log(LogType.InternalError, 'Unexpectedly stopped',
                                   None, fetcher.exception, fetcher.exc_info)
    except Exception, ex:
        local_logger.log(LogType.InternalError, "", exception=ex, exc_info=sys.exc_info())
    finally:
        local_logger.log(LogType.Info, "Fetcher service exited")

if __name__ == "__main__":
    main()
