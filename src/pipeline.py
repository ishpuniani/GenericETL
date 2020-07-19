class Pipeline:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        """
        Runs the pipeline using the objects initialized
        :return: None
        """
        self.extractor.extract()

        self.transformer.transform()

        self.loader.load()
