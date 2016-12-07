from velum.proxy_manager.proxy_manager_client import ProxyManagerClient
from atrax.fetcher.downloaders.downloader_wrappers import DownloaderWrapperBase
from atrax.fetcher.downloaders.velum_downloader import VelumDownloader


class VelumDownloaderWrapper(DownloaderWrapperBase):
    def __init__(self, address, robot_exclusion_standard_compliant, fetcher_id):
        self.proxy_manager_client = ProxyManagerClient(address)
        self.robot_exclusion_standard_compliant = robot_exclusion_standard_compliant
        self.fetcher_id = fetcher_id
        self.downloaders = {}

    def download(self, url_info, if_modified_since=None, if_none_match=None):
        try:
            downloader = self.downloaders[url_info.sld]
        except KeyError:
            if self.robot_exclusion_standard_compliant:
                downloader = VelumDownloader(url_info.sld, True, self.proxy_manager_client, self.fetcher_id)
            else:
                downloader = (VelumDownloader(url_info.sld, True, self.proxy_manager_client, self.fetcher_id),
                              VelumDownloader(url_info.sld, False, self.proxy_manager_client, self.fetcher_id))
            self.downloaders[url_info.sld] = downloader

        if not self.robot_exclusion_standard_compliant:
            downloader = downloader[1] if url_info.robot_exclusion_reasons else downloader[0]

        return downloader.download(url_info.url, url_info.referrer, if_modified_since, if_none_match)
