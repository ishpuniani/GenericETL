import errno
import os
import time
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
    def write(self, content, backup=False):
        raise NotImplementedError


class S3Writer(Writer):
    s3_bucket = S3Reader.s3_bucket

    def _init_from_config(self, config):
        self.type = config.type
        self.path = config.path

    def write(self, content, backup=False):
        """
        Simulating writing to s3 bucket
        :param content: the content to be written, line by line
        :param backup: boolean parameter which asks s3 to take a backup before writing new file
        """
        filepath = self.s3_bucket + self.path
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            if backup:
                backup_path = os.path.dirname(filepath) + "/history/" + time.strftime("%Y%m%d-%H%M%S") + "-" + \
                              filepath.split("/")[-1]
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                os.rename(filepath, backup_path)
            with open(filepath, "w") as f:
                f.writelines(content)
            print("Written to file: " + filepath)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise exc
        except Exception as e:
            print("Exception writing to : " + filepath)
            raise e
