from atrax.management.constants import ComputeEnv
from aws.ec2 import get_metadata


def get_instance_id(environment):
    """
    :return: The ID of the instance that called this method.
    """
    if environment == ComputeEnv.AWS:
        instance_id = get_metadata('instance-id')
    elif environment == ComputeEnv.LOCAL:
        instance_id = '0000'
    else:
        instance_id = '0000'

    return instance_id
