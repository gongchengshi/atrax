from atrax.management.utils import get_instance_id


def create_fetcher_id(environment, local_id):
    return "%s:%s" % (get_instance_id(environment), local_id)


def create_frontier_id(environment):
    return "%s:%s" % (get_instance_id(environment), 'frontier')


def parse_id(fetcher_id):
    return fetcher_id.split(':')
