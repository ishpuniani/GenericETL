from abc import abstractmethod
from python_json_config import ConfigBuilder

import helpers.factory as factory


class Transformer:

    def _before_transform(self):
        pass

    @abstractmethod
    def _transform(self):
        raise NotImplementedError

    def _after_transform(self):
        pass

    def transform(self):
        try:
            self._before_transform()
            self._transform()
            self._after_transform()
        except Exception as e:
            print("Exception transforming data")
            raise e


class CsvTransformer(Transformer):
    def __init__(self, config):
        if config.mapping is None:
            raise ValueError("Transformer config missing mapping")
        self.type = config.type
        self.mapping_config = ConfigBuilder().parse_config(config.mapping)
        self.source = factory.ReaderFactory.create(config.source)
        self.destination = factory.WriterFactory.create(config.destination)

    def _transform(self):
        print("Transform")
