import multiprocessing
import atrax.fetcher.fetcher


class FetcherProcess(multiprocessing.Process):
    def __init__(self, job_name, local_id):
        super(FetcherProcess, self).__init__(name='Fetcher-' + str(local_id))
        self.job_name = job_name
        self.local_id = local_id

    def run(self):
        fetcher = atrax.fetcher.fetcher.Fetcher(self.job_name, self.local_id)
        fetcher.start()
