from atrax.frontier.constants import SECONDS_PER_MINUTE_F
from atrax.management.constants import ComputeEnv
from python_common.simple_list_file import SimpleListFile


class GlobalConfig:
    def __init__(self, settings):
        self.job_name = settings.get('Name', None)
        self.version = int(settings.get('Version', 2))
        self.environment = ComputeEnv.name_to_int(settings.get('Environment', 'aws'))
        self.use_spot_instances = (settings.get('UseSpotInstances', 'True') == 'True')
        self.max_process_url_attempts = int(settings.get('MaxProcessUrlAttempts', 2))
        self.monthly_budget = float(settings.get('MonthlyBudget', 100.0))
        self.proxy_address = settings.get('ProxyAddress', None)
        self.robot_exclusion_standard_compliant = (settings.get('RobotExclusionStandardCompliant', 'False') == 'True')
        self.google_exclusion_standard_compliant = (settings.get('GoogleExclusionStandardCompliant', 'False') == 'True')
        self.strict_robots_txt_parsing = (settings.get('StrictRobotsTxtParsing', 'False') == 'True')
        self.fetchers_per_client = 2  # Todo: This is hard coded in atrax_deployment/deployment/fetcher/run.sh
        # self.fetchers_per_client = int(settings.get('FetchersPerInstance', 2))
        # Entered in config file as requests per minute but stored as requests per second
        self.max_fetch_rate = int(settings.get('MaxFetchRate', 120)) / SECONDS_PER_MINUTE_F
        self.reference_job = settings.get('ReferenceJob', None)
        self.reference_job_version = int(settings.get('ReferenceJobVersion', self.version))
        self.seed_from_reference_job = (settings.get('SeedFromReferenceJob', 'False') == 'True')
        self.lb_maintenance_cycle_period = int(settings.get('LBMaintenanceCyclePeriod', 10))  # In minutes
        self.filter_frontier = (settings.get('FilterFrontier', 'False') == 'True')
        self.store_content = (settings.get('StoreContent', 'True') == 'True')
        self.log_urls_to_file = (settings.get('LogURLsToFile', 'False') == 'True')


class ConfigFile(SimpleListFile):
    def __init__(self, path):
        SimpleListFile.__init__(self, path)
        self.global_config = GlobalConfig(self.colon_delimited_dict('Global'))
        self.user_agents = self.colon_delimited_dict('UserAgents')
        self.contacts = self.colon_delimited_dict('Contacts')
