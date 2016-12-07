class UrlClass:
    # Reasons for not crawling are <= 0
    Redundant = -7
    DUST = -6
    RobotExclusionStandard = -5
    ExcludedDomain = -4  # Matches an include domain but it also matches an exclude domain
    Redirect404 = -3
    OutOfScope = -2  # It doesn't match an include domain
    ExcludeUrl = -1  # Matches an include domain but it also matches an exclude URL
    Crawled = 0
    InScope = 1  # Matches an include domain or it matches an an include URL

    ToName = None
    ToValue = None

UrlClass.ToName = {
    UrlClass.Redundant: 'Redundant',
    UrlClass.DUST: 'DUST',
    UrlClass.RobotExclusionStandard: 'ROBOTEXCLUSIONSTANDARD',
    UrlClass.ExcludedDomain: 'EXCLUDEDDOMAIN',
    UrlClass.Redirect404: '404',
    UrlClass.OutOfScope: 'OUTOFSCOPE',
    UrlClass.ExcludeUrl: 'EXCLUDEURL',
    UrlClass.Crawled: 'CRAWLED',
    UrlClass.InScope: 'INSCOPE'
}

UrlClass.ToValue = {value: key for key, value in UrlClass.ToName.iteritems()}


import re


class CrawlerScope:
    def __init__(self, scope_config_file):
        self.include_domain_patterns = [re.compile('.*' + pattern + '.*', re.UNICODE)
                                        for pattern in scope_config_file.lists['IncludeDomains']]

        self.include_url_patterns = [re.compile(pattern, re.UNICODE)
                                     for pattern in scope_config_file.lists['IncludeUrls']]

        self.exclude_domain_patterns = [re.compile('.*' + pattern + '.*', re.UNICODE)
                                        for pattern in scope_config_file.lists['ExcludeDomains']]

        self.exclude_url_patterns = [re.compile(pattern, re.UNICODE)
                                     for pattern in scope_config_file.lists['ExcludeUrls']]

    def get(self, url_info):
        if not url_info.url.startswith('http'):
            return UrlClass.OutOfScope

        domain = url_info.domain
        url_id = url_info.id

        for include_domain_pattern in self.include_domain_patterns:
            if include_domain_pattern.match(domain):  # it's in scope (Yes!)
                for exclude_domain_pattern in self.exclude_domain_patterns:
                    if exclude_domain_pattern.match(domain):  # but it's an excluded domain (No!)
                        for include_url_pattern in self.include_url_patterns:
                            if include_url_pattern.match(url_id):  # but it's an included URL (Yes!)
                                return UrlClass.InScope
                        return UrlClass.ExcludedDomain
                for exclude_url_pattern in self.exclude_url_patterns:
                    if exclude_url_pattern.match(url_id):  # but it's an excluded URL (No!)
                        return UrlClass.ExcludeUrl
                return UrlClass.InScope

        # It isn't included with a domain pattern but it might be included with a URL pattern
        for include_url_pattern in self.include_url_patterns:
            if include_url_pattern.match(url_id):  # it's an included URL (Yes!)
                return UrlClass.InScope

        return UrlClass.OutOfScope  # Otherwise is not in scope (No!)
