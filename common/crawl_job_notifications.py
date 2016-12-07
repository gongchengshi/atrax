from aws import USEast1 as AwsConnections
import aws.sns
import json
from atrax.management.config_fetcher import ConfigFetcher


class CrawlJobNotifications:
    def __init__(self, name):
        self.name = name
        self.sns_conn = AwsConnections.sns()
        self.stopping_topic_name = self.name + '_stopping'
        self.fetcher_stopped_topic_name = self.name + '_fetcher_stopped'
        self.frontier_stopped_topic_name = self.name + '_frontier_stopped'

        self.topics = {self.stopping_topic_name, self.fetcher_stopped_topic_name, self.frontier_stopped_topic_name}
        self.topic_arns = None

    def initialize_topics(self):
        if self.topic_arns is not None:
            return
        topics = set(aws.sns.get_topics(self.sns_conn, self.topics))
        for topic in self.topics - topics:
            arn = self.sns_conn.create_topic(topic)['CreateTopicResponse']['CreateTopicResult']['TopicArn']
            self.sns_conn.set_topic_attributes(arn, 'DisplayName', 'Atrax')

        self.topic_arns = {aws.sns.topic_name_from_topic_arn(topic_arn): topic_arn
                           for topic_arn in aws.sns.get_topic_arns(self.sns_conn, self.topics)}

    def _get_contact_endpoints(self):
        contact_endpoints = {}
        for contact_name, details in ConfigFetcher(self.name).get_config_file().contacts.iteritems():
            for protocol, endpoint in json.loads(details).iteritems():
                contact_endpoints[endpoint] = protocol
        return contact_endpoints

    def subscribe_to_notifications(self):
        self.initialize_topics()

        for contact_name, details in ConfigFetcher(self.name).get_config_file().contacts.iteritems():
            for protocol, endpoint in json.loads(details).iteritems():
                for topic_arn in self.topic_arns.values():
                    self.sns_conn.subscribe(topic_arn, protocol, endpoint)

    def sync_with_contact_list(self):
        self.initialize_topics()

        for topic in self.topics:
            subscriptions = aws.sns.get_subscriptions_to_topic(self.sns_conn, topic)
            current_endpoints = {s['Endpoint']: s['SubscriptionArn'] for s in subscriptions}

            contact_endpoints = self._get_contact_endpoints()
            for endpoint, subscription in current_endpoints.iteritems():
                if endpoint not in contact_endpoints.keys() and endpoint in current_endpoints.keys():
                    self.sns_conn.unsubscribe(subscription)

        self.subscribe_to_notifications()

    def unsubscribe_all(self):
        self.initialize_topics()
        for topic_arn in self.topic_arns.values():
            subscriptions = aws.sns.get_confirmed_subscriptions_to_topic_arn(self.sns_conn, topic_arn)
            for subscription in subscriptions:
                self.sns_conn.unsubscribe(subscription['SubscriptionArn'])

    def delete_all_topics(self):
        arns = aws.sns.get_topic_arns(self.sns_conn, self.topics)
        for topic_arn in aws.sns.get_topic_arns(self.sns_conn, self.topics):
            self.sns_conn.delete_topic(topic_arn)

    # notifications
    def stopping_crawl_job(self):
        self.initialize_topics()
        message = aws.sns.format_message("%s is stopping.", self.name)
        self.sns_conn.publish(self.topic_arns[self.stopping_topic_name], message)

    def frontier_stopped(self):
        self.initialize_topics()
        message = aws.sns.format_message("%s frontier stopped.", self.name)
        self.sns_conn.publish(self.topic_arns[self.frontier_stopped_topic_name], message)

    def fetcher_stopped(self, fetcher_id):
        self.initialize_topics()
        message = aws.sns.format_message("%s fetcher %s stopped.", self.name, fetcher_id)
        self.sns_conn.publish(self.topic_arns[self.fetcher_stopped_topic_name], message)
