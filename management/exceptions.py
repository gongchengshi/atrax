class NoConfigurationFound(Exception):
    def __init__(self):
        Exception.__init__(self, 'No valid configuration could be found')


class PreconditionNotMet(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
