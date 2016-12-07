from abc import ABCMeta, abstractmethod
from atrax.fetcher.downloaders.downloaders import CrawleraDownloader, ProxyDownloader, GenericDownloader


class DownloaderWrapperBase(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def download(self, url_info, if_modified_since=None, if_none_match=None):
        return


class CrawleraDownloaderWrapper(DownloaderWrapperBase):
    def __init__(self, standard_user_agent, non_compliant_user_agent=None):
        self.standard_downloader = CrawleraDownloader(standard_user_agent)
        self.non_compliant_downloader = CrawleraDownloader(
            non_compliant_user_agent) if non_compliant_user_agent else None

    def download(self, url_info, if_modified_since=None, if_none_match=None):
        downloader = self.non_compliant_downloader if (
            self.non_compliant_downloader and url_info.robot_exclusion_reasons) else self.standard_downloader

        return downloader.download(url_info.url, url_info.referrer, if_modified_since, if_none_match)


class ProxyDownloaderWrapper(DownloaderWrapperBase):
    def __init__(self, proxy, standard_user_agent, non_compliant_user_agent=None):
        self.standard_downloader = ProxyDownloader(standard_user_agent, proxy)
        self.non_compliant_downloader = ProxyDownloader(non_compliant_user_agent,
                                                        proxy) if non_compliant_user_agent else None

    def download(self, url_info, if_modified_since=None, if_none_match=None):
        downloader = self.non_compliant_downloader if (
            self.non_compliant_downloader and url_info.robot_exclusion_reasons) else self.standard_downloader
        return downloader.download(url_info.url, url_info.referrer, if_modified_since, if_none_match)


class GenericDownloaderWrapper(DownloaderWrapperBase):
    def __init__(self, standard_user_agent, non_compliant_user_agent=None):
        self.standard_downloader = GenericDownloader(standard_user_agent)
        self.non_compliant_downloader = GenericDownloader(
            non_compliant_user_agent) if non_compliant_user_agent else None

    def download(self, url_info, if_modified_since=None, if_none_match=None):
        downloader = self.non_compliant_downloader if (
            self.non_compliant_downloader and url_info.robot_exclusion_reasons) else self.standard_downloader
        return downloader.download(url_info.url, url_info.referrer, if_modified_since, if_none_match)


from atrax.fetcher.downloaders.pass_through_proxy_downloader import PassThroughProxyDownloader


class PassThroughProxyDownloaderWrapper(DownloaderWrapperBase):
    def __init__(self, proxy, standard_user_agent, non_compliant_user_agent=None):
        self.standard_downloader = PassThroughProxyDownloader(standard_user_agent, proxy)
        self.non_compliant_downloader = PassThroughProxyDownloader(non_compliant_user_agent,
                                                                   proxy) if non_compliant_user_agent else None

    def download(self, url_info, if_modified_since=None, if_none_match=None):
        downloader = self.non_compliant_downloader if (
            self.non_compliant_downloader and url_info.robot_exclusion_reasons) else self.standard_downloader
        return downloader.download(url_info.url, url_info.referrer, if_modified_since, if_none_match)
