from abc import abstractmethod

import helpers.factory as factory


class Loader:

    def _before_load(self): pass

    @abstractmethod
    def _load(self):
        raise NotImplementedError

    def _after_load(self): pass

    def load(self):
        try:
            self._before_load()
            self._load()
            self._after_load()
        except Exception as e:
            print("Exception loading data")
            raise e


class CsvLoader(Loader):
    def __init__(self, config):
        self.backup = True if config.backup is None else config.backup
        self.update_source = factory.ReaderFactory.create(config.upsert.source)
        self.current_source = factory.ReaderFactory.create(config.current.source)
        self.destination = factory.WriterFactory.create(config.destination)

    def _load(self):
        print("Load")
