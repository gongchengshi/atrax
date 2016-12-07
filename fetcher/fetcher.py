from _ssl import SSLError
import hashlib
import multiprocessing
import socket
import sys
import time
from urlparse import urljoin
from httplib import IncompleteRead
import warnings

from boto.exception import BotoServerError, SDBResponseError
import dns.exception
import requests

from aws.sdb import select_one
from python_common.collections.trie_set import TrieSet
from python_common.web.http_utils import sec_since_epoch_to_http_date
from python_common.web.url_utils import is_spider_trap, extract_scheme
from python_common.web.url_canonization import create_absolute_url
from python_common.web.robots_txt.reppy_wrapper import ReppyWrapper as RobotsTxtParser
from python_common.interruptable_thread import InterruptableThread
from atrax.common.crawl_job_notifications import CrawlJobNotifications
from atrax.common.fetcher_id import create_fetcher_id
from atrax.common.exceptions import DependencyInitializationError
from atrax.common.constants import *
from atrax.common.url_transforms import UrlTransformer
from atrax.common.crawl_scope import CrawlerScope, UrlClass
from atrax.common.url_info import UrlInfo
from atrax.common.crawl_job import CrawlJob
from atrax.common.sdb_url_info import unpack_sdb_url_info, pack_sdb_redirected_url_info, pack_sdb_url_info, \
    trim_sdb_attribute
from atrax.common.logger import LogType, SimpleLogger, LocalLogger
from atrax.management.constants import ComputeEnv
from atrax.management.config_fetcher import ConfigFetcher
from atrax.frontier.remote_frontier_client import get_frontier_client
from atrax.frontier.exceptions import FrontierNotResponding, FrontierMessageCorrupt
from atrax.fetcher.url_extractors.url_extractor import UrlExtractor
from atrax.fetcher.downloaders.downloader_wrappers import *
from atrax.fetcher.dust_detection import DustInfoFactory
from atrax.fetcher.redundant_url_detection import RedundantUrlDetector
from atrax.fetcher.content_stripping import strip_content
from atrax.fetcher.exceptions import AlreadyFetchedException
from atrax.fetcher.fetcher_metrics import FetcherMetrics


warnings.filterwarnings("ignore", category=UserWarning, module='urllib2')


class Fetcher(InterruptableThread):
    def __init__(self, job_name, local_id=0):
        InterruptableThread.__init__(self)
        self.id = 'unknown'
        try:
            socket.setdefaulttimeout(30)  # wait up to 30 seconds for a response on all sockets

            config_fetcher = ConfigFetcher(job_name)
            config_file = config_fetcher.get_config_file()
            self.global_config = config_file.global_config
            self.id = create_fetcher_id(self.global_config.environment, local_id)
            self.crawl_job = CrawlJob(job_name, self.global_config)
            self.logger = SimpleLogger(self.crawl_job.logs_table, self.id)
            self.local_logger = LocalLogger('fetcher', local_id)

            try:
                self.metrics = FetcherMetrics(self.crawl_job.name)
                self.notifier = CrawlJobNotifications(self.crawl_job.name)

                if self.global_config.reference_job:
                    if self.global_config.reference_job_version < self.global_config.version:
                        from atrax.prior_versions import crawl_job_versions

                        self.reference_job = crawl_job_versions[self.global_config.reference_job_version](
                            self.global_config.reference_job)
                    else:
                        self.reference_job = CrawlJob(self.global_config.reference_job)
                else:
                    self.reference_job = None

                self.url_extractor = UrlExtractor(self.logger)

                self.user_agent = config_file.user_agents['Standard'] % self.id
                non_compliant_ua = config_file.user_agents.get('NonCompliant', None)
                self.non_compliant_user_agent = (non_compliant_ua % self.id) if non_compliant_ua else None

                self.select_original_query = \
                    "select itemName() from `{0}` where `{1}`='%s' and `{2}`='{3}' limit 1".format(
                        self.crawl_job.crawled_urls.name, FINGERPRINT_ATTR_NAME,
                        ORIGINAL_ATTR_NAME, ORIGINAL_ATTR_VALUE_SELF)

                scope_file = config_fetcher.get_scope_file()
                self.scope = CrawlerScope(scope_file)
                self.url_transformer = UrlTransformer(config_file)
                self.robots_txt_cache = {}

                self.downloader = self.initialize_downloader()

                # Initialize frontier client
                frontier_instance = self.crawl_job.instance_accessor.get_frontier_instance()
                self.frontier = get_frontier_client(frontier_instance, self.new_queue_assigned)

                self.local_seen_urls = TrieSet()  # a local list of URLs that this fetcher has seen since it started.

                # Don't crawl pages that are at least 90% similar to the previous 5 crawled pages in the URL's lineage.
                self.dust_info_factory = DustInfoFactory(.9, .1, 5)

                redis_instance = self.crawl_job.instance_accessor.get_redis_instance()
                self.redundant_url_detector = RedundantUrlDetector(self.crawl_job.glossary.redundant_urls_key,
                                                                   host=redis_instance.private_ip_address,
                                                                   port=DEFAULT_REDIS_PORT)
            except Exception, ex:
                self.logger.log(LogType.InternalError, "Unexpectedly stopped", None, ex, sys.exc_info())
                raise
        except Exception, ex:
            sys.stderr.write("Failed to initialize fetcher %s.: \n%s\nStack Trace:%s\n" % (
                self.id, ex, sys.exc_info()))
            raise

    def new_queue_assigned(self, queue_name):
        self.logger.log(LogType.Info, "Queue assigned: " + queue_name)

    def initialize_downloader(self):
        if self.global_config.proxy_address:
            if self.global_config.proxy_address == 'velum':
                from atrax.fetcher.downloaders.velum_downloader_wrapper import VelumDownloaderWrapper
                from velum.common.constants import DEFAULT_VELUM_PORT

                velum_instance = self.crawl_job.instance_accessor.get_velum_instance()
                if velum_instance.state != 'running':
                    raise DependencyInitializationError('Velum')
                downloader = VelumDownloaderWrapper(velum_instance.private_ip_address + ':' + DEFAULT_VELUM_PORT,
                                                    self.global_config.robot_exclusion_standard_compliant,
                                                    self.id)
            elif self.global_config.proxy_address == 'crawlera':
                downloader = CrawleraDownloaderWrapper(self.user_agent, self.non_compliant_user_agent)
            else:
                downloader = ProxyDownloaderWrapper(self.global_config.proxy_address,
                                                    self.user_agent, self.non_compliant_user_agent)
        else:
            downloader = GenericDownloaderWrapper(self.user_agent, self.non_compliant_user_agent)

        return downloader

    def run(self):
        try:
            self.logger.log(LogType.Info, "Started")
            self._run()
            self.logger.log(LogType.Info, "Stopped")
        except FrontierNotResponding, ex:
            self.logger.log(LogType.InternalError, "Frontier not responding", None, ex)
            self.logger.log(LogType.Info, "Stopped")
        except Exception, ex:
            self.logger.log(LogType.InternalError, "Unexpectedly stopped", None, ex, sys.exc_info())
        finally:
            self.notifier.fetcher_stopped(self.id)

    def _run(self):
        next_url_info = None
        next_msg_id = None

        while not self.is_stop_requested():
            url_info = None
            msg_id = None
            contents = None  # this needs to be here
            ex = None  # this needs to be here

            try:
                if next_url_info:
                    url_info = next_url_info
                    msg_id = next_msg_id
                    next_url_info = None
                    next_msg_id = None
                else:
                    msg_id, url_info = self.frontier.dequeue(self.id)

                if url_info is None:
                    # the frontier is empty, time to exit.
                    self.logger.log(LogType.Info, "No more URLs to fetch")
                    break

                # Putting this here instead of in process_url() otherwise a lot of URLs could be added to the frontier
                # before they are detected as redundant. The problem this tries to solve has to do with downloading
                # redundant URLs so this really is the best place for this.
                if self.redundant_url_detector.is_redundant(url_info):
                    self.save_skipped_url(url_info, UrlClass.Redundant)
                    self.frontier.delete(msg_id)
                    continue

                if (url_info.is_seed or self.global_config.filter_frontier) and not self.process_frontier_url(url_info):
                    self.metrics.put_filtered_from_frontier()
                    self.frontier.delete(msg_id)
                    continue

                contents, from_cache = self.download_resource(url_info, self.reference_job)
                if contents is not None:
                    if from_cache:
                        self.metrics.put_fetched_from_cache()
                        if self.global_config.log_urls_to_file:
                            self.local_logger.log(LogType.Debug, "Fetched from cache", url=url_info.url)
                    stripped_contents = strip_content(url_info.content_type[0], contents) or contents
                    if not url_info.fingerprint:  # this could already be set if the content came from cache.
                        url_info.fingerprint = hashlib.sha1(stripped_contents).hexdigest()

                    original = self.get_original(url_info)

                    if original is not None:
                        if original.name == url_info.id:
                            raise AlreadyFetchedException()
                        self.redundant_url_detector.insert_redundant(url_info)
                        url_info.original = original.name
                        if self.global_config.store_content:
                            self.crawl_job.crawled_content.put(url_info, '')
                    else:
                        self.redundant_url_detector.insert_non_redundant(url_info)
                        url_info.original = ORIGINAL_ATTR_VALUE_SELF
                        if url_info.content_type[0] in DustInfoFactory.dust_content_types:
                            dust_info = self.dust_info_factory.create(url_info.dust_info, stripped_contents)
                        else:
                            dust_info = None
                        # For completeness and to use any improvements in the extraction methods,
                        # extract and enqueue even if the content came from cache.
                        urls_to_fetch = []
                        for extracted_url_info in self.url_extractor.extract_urls(url_info, contents):
                            self.url_transformer.transform(extracted_url_info)
                            if self.process_url(extracted_url_info):
                                if dust_info is not None and extracted_url_info.domain == url_info.domain:
                                    extracted_url_info.dust_info = dust_info
                                urls_to_fetch.append(extracted_url_info)

                        if dust_info is not None and dust_info.is_dust():
                            for dusty_url in urls_to_fetch:
                                self.save_skipped_url(dusty_url, UrlClass.DUST)
                        else:
                            next_msg_id, next_url_info = self.frontier.enqueue_dequeue(self.id, urls_to_fetch)

                        self.mark_urls_seen(urls_to_fetch)
                        if self.global_config.store_content:
                            self.crawl_job.crawled_content.put(url_info, contents)
                    self.save_crawled_url(url_info)
            except requests.HTTPError, ex:
                # 410 is very explicit and the URL should not be requested again.
                requeue = (ex.response is not None) and ex.response.status_code not in [410]
                # requeue = (e.response is not None) and \
                # (e.response.status_code in (407, 408, 503, 409, 500, 501, 502, 503, 504))
                self.handle_outer_exception(ex, url_info, None, requeue=requeue)
            except (IncompleteRead, requests.ConnectionError, requests.Timeout, multiprocessing.TimeoutError,
                    BotoServerError, SSLError), ex:
                self.handle_outer_exception(ex, url_info, None, requeue=True)
            except requests.RequestException, ex:
                self.handle_outer_exception(ex, url_info, None, requeue=False)
            except (KeyboardInterrupt, SystemExit):
                break
            except AlreadyFetchedException, ex:
                self.metrics.put_already_fetched()
                self.logger.log(LogType.InternalWarning, ex.message, url_info.id)
            except FrontierMessageCorrupt, ex:
                log_message = ("From queue: %s\n" % (ex.queue_name or 'unknown')) + ex.message
                self.logger.log(LogType.InternalError, log_message)
                self.local_logger.log(LogType.InternalError, log_message)
            except FrontierNotResponding, ex:
                raise
            except Exception, ex:
                if self.global_config.environment == ComputeEnv.AWS:
                    self.handle_outer_exception(ex, url_info, sys.exc_info(), requeue=True)
                else:
                    raise
            except:
                sys.stderr.write('Unhandled Exception in Run().\nUrl: %s\nStack trace:\n%s' %
                                 (url_info.id, sys.exc_info()))
                raise

            # Don't delete the message from the frontier until after the URL is completely handled.
            # If the process stops between getting the message from the frontier and completely handling it the
            # message will be automatically be re-inserted into the frontier by the frontier after a timeout
            if msg_id is not None:
                self.frontier.delete(msg_id)

    def get_original(self, url_info):
        return select_one(self.crawl_job.crawled_urls, self.select_original_query % url_info.fingerprint)

    def add_to_frontier(self, url_infos):
        self.frontier.enqueue(url_infos)
        self.mark_urls_seen(url_infos)

    def save_crawled_url(self, url_info):
        self.crawl_job.crawled_urls.put_attributes(trim_sdb_attribute(url_info.id), pack_sdb_url_info(url_info))

    def save_failed_url(self, url_info):
        self.crawl_job.failed_urls.put_attributes(trim_sdb_attribute(url_info.id), pack_sdb_url_info(url_info))

    def save_skipped_url(self, url_info, url_class):
        self.crawl_job.skipped_urls.put_attributes(trim_sdb_attribute(url_info.raw_url),
                                                   {REFERRER_ID_ATTR_NAME: trim_sdb_attribute(url_info.referrer_id),
                                                    REASON_ATTR_NAME: url_class})

    def save_redirected_url(self, url_info):
        self.crawl_job.redirected_urls.put_attributes(
            trim_sdb_attribute(url_info.id), pack_sdb_redirected_url_info(url_info))

    def download(self, url_info, fetched=None, etag=None):
        if self.global_config.log_urls_to_file:
            self.local_logger.log(LogType.Debug, "Fetching", url=url_info.url)

        url_info.requested = time.time()
        response = self.downloader.download(url_info, fetched, etag)
        self.metrics.put_http_status(response.status_code)
        return response

    def download_resource(self, url_info, reference_job=None):
        if reference_job and url_info.use_cached:
            content = reference_job.crawled_content.get(url_info.s3_key)
            if content is not None:
                return content, True

        fetched = sec_since_epoch_to_http_date(url_info.fetched) if url_info.fetched else None
        response = self.download(url_info, fetched=fetched, etag=url_info.etag)

        if response.status_code == 304:
            content = reference_job.crawled_content.get(url_info.s3_key)
            if content is not None:
                return content, True
            response = self.download(url_info)

        url_info.http_status = response.status_code
        url_info.response_headers = response.headers
        url_info.fetcher = self.id
        url_info.fetched = time.time()

        response.raise_for_status()

        if response.status_code in [300, 301, 302, 303, 307, 308]:
            if url_info.num_redirects >= MAX_REDIRECTS:
                raise requests.TooManyRedirects('Exceeded %s redirects.' % MAX_REDIRECTS)

            new_raw_url = urljoin(url_info.raw_url, response.headers['location'])
            if url_info.raw_url == new_raw_url:  # URL redirects back to itself
                return None, None

            new_url_info = UrlInfo(new_raw_url)
            self.url_transformer.transform(new_url_info)

            if url_info.id == new_url_info.id:
                # Only the scheme changed (usually http -> https) so just fetch that URL right away instead
                url_info.change_scheme(extract_scheme(new_raw_url))
                return self.download_resource(url_info, reference_job)

            new_url_info.referrer_id = url_info.id
            new_url_info.num_redirects = url_info.num_redirects + 1
            new_url_info.discovered = time.time()

            url_info.redirects_to = new_url_info.id
            self.save_redirected_url(url_info)  # Log the redirect

            # Calling self.process_url() on the new URL can cause unbounded recursion so return None here. Redirects
            # from robots.txt files are usually useless anyway. It is either a domain level redirect or an error page.
            if url_info.url.endswith('/robots.txt'):
                return None, None

            if self.process_url(new_url_info):
                self.mark_url_seen(new_url_info)  # local only because new_url_info isn't added to the frontier.
                self.add_to_frontier([new_url_info])

            # The body of non-300 type responses is usually empty.
            if response.status_code != 300:
                return None, None

        return response.content, False

    def robot_is_excluded(self, url_info):
        return (self.global_config.google_exclusion_standard_compliant and url_info.googlebot_exclusion_reasons) or \
               (self.global_config.robot_exclusion_standard_compliant and url_info.robot_exclusion_reasons)

    def process_url(self, url_info):
        try:
            if self.url_is_seen(url_info):
                return False

            if not url_info.discovered:
                url_info.discovered = time.time()

            c = self.scope.get(url_info)
            if c < 0:
                self.save_skipped_url(url_info, c)
                self.mark_url_seen(url_info)
                return False

            self.process_domain(url_info.domain)
            self.set_exclusion_reasons(url_info)

            if self.robot_is_excluded(url_info):
                self.save_skipped_url(url_info, UrlClass.RobotExclusionStandard)
                self.mark_url_seen(url_info)
                return False

            if is_spider_trap(url_info.raw_url):
                self.logger.log(LogType.Info, "Possible spider trap: %s" % url_info.raw_url, url_info.referrer_id)
                self.mark_url_seen(url_info)
                return False
        except Exception, ex:
            # This shouldn't ever happen but the exception shouldn't stop other URL's from being processed
            self.logger.log(LogType.InternalError, "Unable to process extracted URL: " + url_info.raw_url,
                            url_info.referrer_id, ex, sys.exc_info())
            return False
        return True

    def process_frontier_url(self, url_info):
        try:
            if not url_info.discovered:
                url_info.discovered = time.time()

            self.url_transformer.transform(url_info)

            self.process_domain(url_info.domain)
            self.set_exclusion_reasons(url_info)

            c = self.scope.get(url_info)
            if c < 0:
                self.save_skipped_url(url_info, c)
                self.mark_url_seen(url_info)
                return False

            if self.robot_is_excluded(url_info):
                self.save_skipped_url(url_info, UrlClass.RobotExclusionStandard)
                self.mark_url_seen(url_info)
                return False
        except Exception, ex:
            # This shouldn't ever happen but the exception shouldn't stop other URL's from being processed
            self.logger.log(LogType.InternalError, "Unable to process extracted URL: " + url_info.raw_url,
                            url_info.referrer_id, ex, sys.exc_info())
            return False
        return True

    def mark_urls_seen(self, url_infos):
        for url_info in url_infos:
            self.mark_url_seen(url_info)

    def mark_url_seen(self, url_info, globally=False):
        # The URL is added to the global seen URLs set by the frontier when a URL is enqueued.
        self.local_seen_urls.add(url_info.id)

        if globally:
            self.crawl_job.seen_urls.add(url_info.id, url_info.domain)

    def url_is_seen(self, url_info):
        if url_info.id in self.local_seen_urls:
            return True

        if (url_info.id, url_info.domain) in self.crawl_job.seen_urls:
            self.local_seen_urls.add(url_info.id)
            return True

        return False

    def process_domain(self, domain):
        try:
            if self.robots_txt_cache[domain].expired:
                self.robots_txt_cache[domain] = self.download_robots_txt(domain)
        except KeyError:
            self.robots_txt_cache[domain] = self.download_robots_txt(domain)

    def download_robots_txt(self, domain):
        robots_txt = RobotsTxtParser()  # is_allowed() always returns True for an empty parser.
        url_id = domain + '/robots.txt'
        m = self.crawl_job.crawled_urls.get_item(url_id)
        url_info = unpack_sdb_url_info(m) if m else UrlInfo("http://" + url_id)

        try:
            if not url_info.discovered:
                url_info.discovered = time.time()
            robots_dot_txt, from_cache = self.download_resource(url_info, self.crawl_job)
            if robots_dot_txt:
                robots_txt = RobotsTxtParser(
                    robots_dot_txt if self.global_config.strict_robots_txt_parsing else robots_dot_txt.lower(),
                    url_info.expires)
                if not from_cache:
                    url_info.original = ORIGINAL_ATTR_VALUE_SELF
                    self.crawl_job.crawled_content.put(url_info, robots_dot_txt)
                    self.save_crawled_url(url_info)
                self.extract_sitemaps(robots_txt.sitemaps, domain)
        except (requests.HTTPError, IncompleteRead, requests.ConnectionError, requests.Timeout,
                requests.RequestException, dns.exception.DNSException), ex:
            # The default RobotsTxtParser object will cause the fetcher to check again in RobotsTxtParser.MIN_TTL
            self.logger.log(LogType.ExternalWarning, 'Unable to download robots.txt', url_info.url, ex)

        return robots_txt

    def extract_sitemaps(self, sitemaps, domain):
        # Add the sitemap URLs to the frontier
        sitemap_url_infos = set()
        # Get sitemap.html, sitemap.htm, sitemap.php, sitemap.aspx, sitemap.asp, sitemap.jsp, sitemap.txt, etc?
        # Just to make sure that this one gets handled.
        sitemaps.append(create_absolute_url('sitemap.xml', 'http://' + domain))

        for sitemap_url in sitemaps:
            url_info = UrlInfo(sitemap_url)
            # this only works because the _content_type backing field is pickled.
            url_info._content_type = 'sitemap'
            url_info.discovered = time.time()
            if not self.url_is_seen(url_info):
                sitemap_url_infos.add(url_info)

        if sitemap_url_infos:
            self.add_to_frontier(sitemap_url_infos)

    def set_exclusion_reasons(self, url_info):
        try:
            robots_txt = self.robots_txt_cache[url_info.domain]
        except KeyError:
            # process_domain() must be called before calling set_exclusion_reasons() to avoid this exception
            self.process_domain(url_info.domain)
            robots_txt = self.robots_txt_cache[url_info.domain]

        url = url_info.raw_url if self.global_config.strict_robots_txt_parsing else url_info.url

        if not robots_txt.allowed(self.user_agent, url):
            url_info.robot_exclusion_reasons.add(RobotExclusionReasons.ROBOTS_TXT)

        if not robots_txt.allowed(GOOGLEBOT_USER_AGENT_NAME, url):
            url_info.googlebot_exclusion_reasons.add(RobotExclusionReasons.ROBOTS_TXT)

    def handle_outer_exception(self, exception, url_info, stack_trace, requeue=True):
        if url_info is None:
            self.logger.log(LogType.InternalError, "Unknown error", None, exception, stack_trace)
            return

        url_info.process_attempts += 1

        if (not requeue) or url_info.process_attempts >= self.global_config.max_process_url_attempts:
            if __debug__:
                self.logger.log(LogType.ExternalError,
                                'Failed to process URL after %s attempts' % url_info.process_attempts,
                                url_info.id, exception, stack_trace)
            try:
                self.save_failed_url(url_info)
            except SDBResponseError, ex:
                # This could be caused by invalid characters in the request. See:
                # https://forums.aws.amazon.com/thread.jspa?messageID=82370&#82370
                if ex.error_code == "SignatureDoesNotMatch":
                    self.local_logger.log(LogType.InternalWarning,
                                          "SDB SignatureDoesNotMatch Error", url_info.raw_url, exception, stack_trace)
                else:
                    raise
        else:
            # self.logger.log(LogType.ExternalWarning, 'Requeuing URL.', url_info.id, ex, stack_trace)
            self.frontier.enqueue([url_info])
