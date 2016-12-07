import re


class UrlReplacementTypes:
    Normal = 0
    RemoveParam = 1


class UrlTransformer:
    def __init__(self, config_file):
        transformations = [(pattern.strip('{}'), replacement.strip('{}'))
                           for pattern, replacement in config_file.colon_delimited_tuples('UrlTransformations')]

        self.transformations = []
        for pattern, replacement in transformations:
            if replacement == 'RemoveParam':
                pattern += '?(&|$)' if pattern[-1] in '*+' else '(&|$)'
                t = (re.compile(pattern, re.UNICODE | re.IGNORECASE), '', UrlReplacementTypes.RemoveParam)
            else:
                t = (re.compile(pattern, re.UNICODE | re.IGNORECASE), replacement, UrlReplacementTypes.Normal)
            self.transformations.append(t)

    def transform(self, url_info):
        applied = False
        for pattern, replacement, replacementType in self.transformations:
            if replacementType == UrlReplacementTypes.RemoveParam and 'begin' in pattern.groupindex:
                replacement = "\g<begin>"
            result, n = pattern.subn(replacement, url_info.url)
            if n > 0:
                url_info.set_url(result.strip('?&') if replacementType == UrlReplacementTypes.RemoveParam else result)
                applied = True
        return applied
