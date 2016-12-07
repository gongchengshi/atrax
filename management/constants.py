class ComputeEnv:
    LOCAL = 0
    AWS = 1

    @staticmethod
    def name_to_int(name):
        name = name.lower()
        if name == 'local':
            return ComputeEnv.LOCAL
        elif name == 'aws':
            return ComputeEnv.AWS
        else:
            raise RuntimeError('No environment named ' + name)


class InstanceState:
    RUNNING = 'running'
    PENDING = 'pending'
    TERMINATED = 'terminated'
    SHUTTING_DOWN = 'shutting-down'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
