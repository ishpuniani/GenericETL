from abc import abstractmethod


class Extractor:

    def _before_extract(self): pass

    @abstractmethod
    def _extract(self):
        raise NotImplementedError

    def _after_extract(self): pass

    def extract(self):
        self._before_extract()
        self._extract()
        self._after_extract()


class GenericExtractor(Extractor):
    def __init__(self, config):
        # self.config = config
        pass

    def _extract(self):
        pass
