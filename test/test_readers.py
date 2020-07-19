import unittest
from unittest import mock
from python_json_config import ConfigBuilder

from src.helpers.factory import ReaderFactory


class TestReaders(unittest.TestCase):
    # This method will be used by the mock to replace requests.get
    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

        if args[0] == 'http://someurl.com/success.zip':
            msg = [
                'Date of Sale (dd/mm/yyyy),Address,Postal Code,County,Price (€),Not Full Market Price,VAT Exclusive,Description of Property,Property Size Description',
                '"01/01/2010","5 Braemor Drive, Churchtown, Co.Dublin","","Dublin","€343,000.00","No","No","Second-Hand Dwelling house /Apartment",""',
                '"03/01/2010","134 Ashewood Walk, Summerhill Lane, Portlaoise","","Laois","€185,000.00","No","Yes","New Dwelling house /Apartment","greater than or equal to 38 sq metres and less than 125 sq metres"',
                '"04/01/2010","1 Meadow Avenue, Dundrum, Dublin 14","","Dublin","€438,500.00","No","No","Second-Hand Dwelling house /Apartment",""',
                '"04/01/2010","1 The Haven, Mornington","","Meath","€400,000.00","No","No","Second-Hand Dwelling house /Apartment",""'
            ]
        elif args[0] == 'http://someurl.com/fail.zip':
            return None

        return MockResponse(None, 404)

    # Testing the get_request method
    @mock.patch('urlopen.return_value.read', side_effect=mocked_requests_get)
    def test_get_request_ok(self, mock_get):
        try:
            config_json = '{"type": "http","path": "http://someurl.com/success.zip","unzip": true}'
            conf = ConfigBuilder().parse_config(config_json)
            reader = ReaderFactory.create(conf)
            reader.read()
        except Exception as e:
            self.fail("Exception should not have occurred")

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_request_fail(self, mock_get):
        config_json = '{"type": "http","path": "http://someurl.com/fail","unzip": true}'
        conf = ConfigBuilder().parse_config(config_json)
        reader = ReaderFactory.create(conf)
        self.assertRaises(Exception, reader.read())