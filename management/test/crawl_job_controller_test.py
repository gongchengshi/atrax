import os

import atrax.common.constants

atrax.common.constants.ControllerCrawlJobDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input')
from management.aws.crawl_job_controller import CrawlJobController

crawl_job_name = 'dummycrawljob'

target = CrawlJobController(crawl_job_name)

# target.initialize()
# target.start()
# target.pause()
# target.stop()
# target.start()
target.destroy()
