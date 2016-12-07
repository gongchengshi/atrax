#!/bin/bash
# avoids superfluous logs in /var/log/awslogs.log
touch /var/log/atrax-logger.log /var/log/atrax-fetcher.log /var/log/atrax-frontier.log
touch /var/log/upstart/fetcher.log /var/log/upstart/frontier.log /var/log/upstart/redis.log
# CloudWatch Logs Agent
wget https://s3.amazonaws.com/aws-cloudwatch/downloads/latest/awslogs-agent-setup.py
chmod +x ./awslogs-agent-setup.py
./awslogs-agent-setup.py -n -r us-west-2 -c s3://atrax-configuration-management/cloudwatch_logging.config
