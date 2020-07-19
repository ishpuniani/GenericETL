import pandas as pd
import ssl

from abc import abstractmethod
from io import BytesIO, StringIO
from urllib.error import URLError
from urllib.request import urlopen
from zipfile import ZipFile

# to bypass the issue ::
# urllib.error.URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1076)>
ssl._create_default_https_context = ssl._create_unverified_context


class Reader:
    # ENCODING = "ISO-8859-1"
    ENCODING = "windows-1252"

    def __init__(self, config):
        if config is not None:
            self._init_from_config(config)
        else:
            raise ValueError("Reader config not found")

    @abstractmethod
    def _init_from_config(self, config):
        raise NotImplementedError

    @abstractmethod
    def read(self):
        raise NotImplementedError


class HttpReader(Reader):
    def _init_from_config(self, config):
        if config.path is None:
            raise ValueError("HttpReader missing value")
        self.type = config.type
        self.path = config.path
        self.unzip = False if config.unzip is None else config.unzip

    def read(self):
        """
        Reads and unzips file found over http
        :return: list of csv rows
        """
        url = self.path
        try:
            print("Downloading content from : " + url)
            resp = urlopen(url)
            zipfile = ZipFile(BytesIO(resp.read()))
            file = zipfile.namelist()[0]
            content = [str(row, self.ENCODING) for row in zipfile.open(file, 'r').readlines()]
            print("Downloaded content!")
            return content
        except URLError as ue:
            print("URLError reading from url: " + url)
            raise ue
        except Exception as e:
            print("Exception reading from url: " + url)
            raise e


class S3Reader(Reader):
    s3_bucket = 'resources/s3_buckets/'

    def _init_from_config(self, config):
        if config.path is None:
            raise ValueError("S3Reader missing value")
        self.type = config.type
        self.path = config.path

    def read(self):
        """
        Simulating reading content from s3
        Actually reading from s3_buckets folder in resources.
        :return: file content in pandas dataframe
        """
        location = self.s3_bucket + self.path
        try:
            print("Reading content from : " + location)
            content = pd.read_csv(location)
            return content
        except FileNotFoundError as fe:
            print("File not found at : " + location)
            raise fe
        except Exception as e:
            print("Exception reading from s3: " + location)
            raise e
