from atrax.management.aws_env.constants import CREATED_TAG_NAME, NAME_TAG_NAME


def get_latest_ami(ec2, name, virtualization_type=None):
    filters = {'tag:' + NAME_TAG_NAME: name}
    if virtualization_type is not None:
        filters['virtualization_type'] = virtualization_type

    images = ec2.get_all_images(owners='self', filters=filters)
    if len(images) == 0:
        raise LookupError(name)

    return max(images, key=lambda i: i.tags[CREATED_TAG_NAME])


def get_latest_frontier_ami(ec2):
    return get_latest_ami(ec2, 'frontier')


def get_latest_fetcher_ami(ec2, virtualization_type):
    return get_latest_ami(ec2, 'fetcher', virtualization_type=virtualization_type)
