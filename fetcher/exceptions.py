class AlreadyFetchedException(Exception):
    def __init__(self):
        Exception.__init__(self, "URL has already been fetched")
