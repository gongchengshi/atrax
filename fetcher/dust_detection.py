import nilsimsa

from python_common.math import percent_diff


class DustInfo:
    def __init__(self, referrer_dust, content, similarity_threshold, size_threshold, history_size):
        assert(0 < similarity_threshold <= 1)
        assert(0 < size_threshold <= 1)
        self._history_size = history_size
        self.size = len(content)
        self.digest = nilsimsa.Nilsimsa(content).hexdigest()
        self.count = 0

        if referrer_dust is None:
            return

        if percent_diff(self.size, referrer_dust.size) < size_threshold and referrer_dust.digest is not None:
            score = nilsimsa.compare_hexdigests(referrer_dust.digest, self.digest)
            similarity = (256 - (128 - score)) / 256.0
            self.count = referrer_dust.count + 1 if similarity >= similarity_threshold else 0

    def is_dust(self):
        return self.count > self._history_size


class DustInfoFactory:
    dust_content_types = {"text/html": "application/xhtml+xml"}

    def __init__(self, similarity_threshold, size_threshold, history_size):
        self._similarity_threshold = similarity_threshold
        self._size_threshold = size_threshold
        self._history_size = history_size

    def create(self, referrer_dust, content):
        return DustInfo(referrer_dust, content, self._similarity_threshold, self._size_threshold, self._history_size)
