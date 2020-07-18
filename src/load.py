from abc import abstractmethod


class Loader:

    def _before_load(self): pass

    @abstractmethod
    def _load(self):
        raise NotImplementedError

    def _after_load(self): pass

    def load(self):
        self._before_load()
        self._load()
        self._after_load()


class CsvLoader(Loader):
    def __init__(self, config):
        pass

    def _load(self):
        pass
