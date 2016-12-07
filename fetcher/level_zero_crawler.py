import argparse

from atrax.fetcher.fetcher import Fetcher
from atrax.frontier.file_frontier import FileFrontier


parser = argparse.ArgumentParser()
parser.add_argument('job', type=str)
parser.add_argument('urls_path', type=str)

args = parser.parse_args()

crawler = Fetcher(args.job)
crawler.Frontier = FileFrontier(args.urls_path)
crawler.run()

print "Finished"
