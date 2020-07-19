from abc import abstractmethod

from extract import GenericExtractor
from load import CsvLoader
from transform import CsvTransformer, PprTransformer
from .reader import HttpReader, S3Reader
from .writer import S3Writer


class Factory:
    @classmethod
    def create(cls, config):
        """
        Factory method to create objects on the basis of the key "type" in configuration
        :param config: config object
        :return: instance of appropriate class on the basis of "type"
        """
        try:
            if config is not None and config.type is not None:
                obj = cls._create(config)
                return obj
            else:
                raise ValueError("Config is empty or missing type")
        except ValueError as ve:
            print("Unable to create instance in factory")
            raise ve

    @classmethod
    @abstractmethod
    def _create(cls, config):
        raise NotImplementedError


class ExtractorFactory(Factory):
    """
    Factory for Extractors
    """

    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'generic':
            return GenericExtractor(config)
        else:
            raise ValueError("Invalid executor type: " + config.type)


class TransformerFactory(Factory):
    """
    Factory for Transformers
    """

    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'csv':
            return CsvTransformer(config)
        elif config.type.lower() == 'ppr':
            return PprTransformer(config)
        else:
            raise ValueError("Invalid transformer type: " + config.type)


class LoaderFactory(Factory):
    """
    Factory for Loaders
    """

    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'csv':
            return CsvLoader(config)
        else:
            raise ValueError("Invalid loader type: " + config.type)


class ReaderFactory(Factory):
    """
    Factory for Readers
    """

    @classmethod
    def _create(cls, config):
        if config.type.lower() == 'http':
            return HttpReader(config)
        elif config.type.lower() == 's3':
            return S3Reader(config)
        else:
            raise ValueError("Invalid Reader type: " + config.type)


class WriterFactory(Factory):
    """
    Factory for Writers
    """

    @classmethod
    def _create(cls, config):
        if config.type.lower() == 's3':
            return S3Writer(config=config)
        else:
            raise ValueError("Invalid Writer type: " + config.type)
