from bs4 import BeautifulSoup
import re

html_content_types = {"text/html": "application/xhtml+xml"}
whitespace_pattern = re.compile(r"\W+")


def strip_content(content_type, content):
    if content_type in html_content_types:
        soup = BeautifulSoup(content)
        for script in soup(["script", "style"]):
            script.extract()

        return whitespace_pattern.sub('', soup.get_text())
    else:
        return content
