from abc import abstractmethod


class Transformer:

    def _before_transform(self): pass

    @abstractmethod
    def _transform(self):
        raise NotImplementedError

    def _after_transform(self): pass

    def transform(self):
        self._before_transform()
        self._transform()
        self._after_transform()


class CsvTransformer(Transformer):
    def __init__(self, config):
        pass

    def _transform(self):
        pass
