class SqsMessageRetentionException(Exception):
    def __init__(self, queue_name):
        Exception.__init__(self, "Messages in the queue '%s' will soon to exceed their retention period" % queue_name)


class FrontierNotResponding(Exception):
    def __init__(self, ex):
        Exception.__init__(self, 'No response was received from the frontier within the specified timeout: ' + str(ex))


class FrontierEmpty(Exception):
        def __init__(self):
            Exception.__init__(self, 'There are no more URLs in the frontier')


class FrontierMessageCorrupt(Exception):
    def __init__(self, contents, queue_name=None):
            Exception.__init__(self, 'Unexpected message content:\n' + contents)
            self.queue_name = queue_name
