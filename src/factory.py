from abc import abstractmethod

from extract import GenericExtractor
from load import CsvLoader
from transform import CsvTransformer


class Factory:
    @classmethod
    def create(cls, config):
        try:
            obj = cls._create(config)
            return obj
        except ValueError as ve:
            print("Unable to create instance in factory")
            raise ve

    @classmethod
    @abstractmethod
    def _create(cls, config):
        raise NotImplementedError


class ExtractorFactory(Factory):
    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'generic':
            return GenericExtractor(config)
        else:
            raise ValueError("Invalid executor type: " + config.type)


class TransformerFactory(Factory):
    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'csv':
            return CsvTransformer(config)
        else:
            raise ValueError("Invalid transformer type: " + config.type)


class LoaderFactory(Factory):
    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'csv':
            return CsvLoader(config)
        else:
            raise ValueError("Invalid loader type: " + config.type)
