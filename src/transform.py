import json
import datetime as dt
import unicodedata
import pandas as pd
import numpy as np

from io import StringIO
from abc import abstractmethod

import helpers.factory as factory


class Transformer:

    def _before_transform(self):
        pass

    @abstractmethod
    def _transform(self):
        raise NotImplementedError

    def _after_transform(self):
        pass

    def transform(self):
        try:
            self._before_transform()
            self._transform()
            self._after_transform()
        except Exception as e:
            print("Exception transforming data")
            raise e


class CsvTransformer(Transformer):
    """
    Basic CSV Transformer that takes in the mapping file, modifies columns and does basic pre-processing by field type.
    """

    def __init__(self, config):
        if config.mapping is None:
            raise ValueError("Transformer config missing mapping")
        try:
            self.type = config.type
            self.mappings = json.load(open(config.mapping))
            self.source = factory.ReaderFactory.create(config.source)
            self.destination = factory.WriterFactory.create(config.destination)
        except FileNotFoundError as fe:
            print("Mapping file not found: " + config.mapping)
            raise fe

    def _before_transform(self):
        self.source_df = self.source.read()
        print("source count: " + str(self.source_df.shape[0]))
        # print(self.source_df.head())
        self.data = pd.DataFrame()
        print("Read data to be transformed")

    def _transform(self):
        for mapping in self.mappings:
            self.process_mapping(mapping)
        print("Transformed!")

    def _after_transform(self):
        print("Transformed! Writing transformed data")
        data = self.data
        # print(data.head())
        print("transformed data count: " + str(data.shape[0]))
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        self.destination.write(csv_buffer.getvalue())
        print("Written transformed data")

    def process_mapping(self, mapping):
        field = mapping['field']
        target_field = mapping['target_field']
        field_type = mapping['type']
        options = mapping['options'] if "options" in mapping else None

        column = self.process_column(self.source_df[field], field_type, options)
        self.data[target_field] = column

    def process_column(self, field, field_type, options):
        if field_type.lower() == 'string':
            return self.process_string(field)
        elif field_type.lower() == 'price':
            return self.process_price(field)
        elif field_type.lower() == 'date':
            return self.process_date(field)
        elif field_type.lower() == 'bool_int':
            return self.process_bool_int(field)
        else:
            raise ValueError("Invalid pre-processing type")

    def process_string(self, field, options=None):
        arr = field.str.lower().str.strip()
        arr = [self.strip_accents(val) for val in arr]
        return arr

    def strip_accents(self, s):
        return ''.join(c for c in unicodedata.normalize('NFD', s)
                       if unicodedata.category(c) != 'Mn')

    def process_price(self, field, options=None):
        arr = pd.to_numeric(field.str[1:].str.replace(',', ''), errors='coerce').astype(float)
        return arr

    def process_date(self, field, options=None):
        if options is not None and options["format"] is not None:
            date_format = options["format"]
        else:
            date_format = "%d/%m/%Y"
        arr = []
        for old_date in field:
            dt_obj = dt.datetime.strptime(old_date, '%d/%m/%Y')
            new_date = """{}/{}/{}""".format(dt_obj.day, dt_obj.month, dt_obj.year)
            arr.append(new_date)
        return arr

    def process_bool_int(self, field):
        arr = [1 if val.lower() == 'yes' else 0 for val in field]
        return arr


class PprTransformer(CsvTransformer):
    """
    Specific transformer to handle columns : month_start, new_home_ind, quarantine_ind, quarantine_code
    """

    # Create list of counties in Ireland
    county_list = ['Cork', 'Galway', 'Mayo', 'Donegal', 'Kerry', 'Tipperary', 'Clare', 'Tyrone', 'Antrim', 'Limerick',
                   'Roscommon', 'Down', 'Wexford', 'Meath', 'Londonderry', 'Kilkenny', 'Wicklow', 'Offaly', 'Cavan',
                   'Waterford', 'Westmeath', 'Sligo', 'Laois', 'Kildare', 'Fermanagh', 'Leitrim', 'Armagh', 'Monaghan',
                   'Longford', 'Dublin', 'Carlow', 'Louth']
    county_list = [element.lower() for element in county_list]

    def _transform(self):
        super()._transform()
        self.custom_transformations()

    def custom_transformations(self):
        print("Custom Transformations: ")

        # new home conditional column
        self.data["new_home_ind"] = [1 if i == "New Dwelling house /Apartment" else 0 for i in
                                     self.source_df["Description of Property"]]

        non_duplicate_records = self.data.drop_duplicates(subset=["address", "county", "sales_value"])
        duplicate_records = self.data[~self.data.isin(non_duplicate_records)]

        self.data["quarantine_ind"] = np.where(
            np.logical_and(self.data['not_full_market_price_ind'] == 1, self.data['new_home_ind'] == 1), 1,
            np.where(self.data['address'].isin(duplicate_records['address']), 1,
                     (np.where(~self.data['county'].isin(self.county_list), 1, 0))))

        # quarantine_code:
        # ERR - DUP RECORD -> non unique
        # ERR - INVALID COUNTY -> invalid county
        # ERR - NEW HOME NOT FULL MARKET VALUE -> not full market value
        self.data["quarantine_code"] = np.where(
            np.logical_and(self.data['not_full_market_price_ind'] == 1, self.data['new_home_ind'] == 1),
            "ERR - NEW HOME NOT FULL MARKET VALUE",
            np.where(self.data['address'].isin(duplicate_records['address']), "ERR - DUP RECORD",
                     (np.where(~self.data['county'].isin(self.county_list), "ERR - INVALID COUNTY", ""))))

        month_start = []
        for old_date in self.data["sales_date"]:
            dt_obj = dt.datetime.strptime(old_date, '%d/%m/%Y')
            new_date = """1/{}/{}""".format(dt_obj.month, dt_obj.year)
            month_start.append(new_date)
        self.data["month_start"] = month_start
