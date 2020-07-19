from abc import abstractmethod

import helpers.factory as factory


class Extractor:

    def _before_extract(self): pass

    @abstractmethod
    def _extract(self):
        raise NotImplementedError

    def _after_extract(self): pass

    def extract(self):
        try:
            self._before_extract()
            self._extract()
            self._after_extract()
        except Exception as e:
            print("Exception in extracting data")
            raise e


class GenericExtractor(Extractor):
    def __init__(self, config):
        self.type = config.type
        self.source = factory.ReaderFactory.create(config.source)
        self.destination = factory.WriterFactory.create(config.destination)

    def _extract(self):
        content = self.source.read()
        self.destination.write(content)
        print("Extracted")
