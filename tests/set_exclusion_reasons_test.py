# coding=utf-8

from CrawlerInstance import CrawlerInstance
from UrlInfo import UrlInfo

crawlerInstance = CrawlerInstance(1005, 'siemens17042013')
u = UrlInfo(u'https://blogs.siemens.com/competitive-industries/stories/tags/schüler/')
crawlerInstance.set_exclusion_reasons(u)
