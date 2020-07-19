import errno
import os
from abc import abstractmethod

from helpers.reader import S3Reader


class Writer:
    def __init__(self, config):
        if config is not None:
            self._init_from_config(config)
        else:
            raise ValueError("Writer config not found")

    @abstractmethod
    def _init_from_config(self, config):
        raise NotImplementedError

    @abstractmethod
    def write(self, content):
        raise NotImplementedError


class S3Writer(Writer):
    s3_bucket = S3Reader.s3_bucket

    def _init_from_config(self, config):
        self.type = config.type
        self.path = config.path

    def write(self, content):
        """
        Simulating writing to s3 bucket
        :param content: the content to be written, line by line
        """
        filename = self.s3_bucket + self.path
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w") as f:
                f.writelines(content)
            print("Written to file: " + filename)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise exc
        except Exception as e:
            print("Exception writing to : " + filename)
            raise e
