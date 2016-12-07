from atrax.management.aws_env.ami import *
from aws import USWest2 as AwsConnections

image = get_latest_frontier_ami(AwsConnections.ec2())
