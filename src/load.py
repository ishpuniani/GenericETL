from io import StringIO

import pandas as pd
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
        if config.primary_keys is None:
            self.primary_keys = ["id"]
        else:
            self.primary_keys = config.primary_keys

    def _before_load(self):
        self.upsert_df = self.update_source.read()
        self.current_df = self.current_source.read()
        self.data = pd.DataFrame()

    def _load(self):
        print("Load")
        # Check duplicates by address, county
        # if any change, update current
        # remove from current if not present in upsert
        # add to current if only present in upsert
        print("current:: " + str(self.current_df.shape[0]))

        unique_transform = self.unique(self.upsert_df)
        print("unique transform:: " + str(unique_transform.shape[0]))
        # print(unique_transform.head())

        max_id = self.current_df['id'].max()
        new_rows = self.difference(unique_transform, self.current_df)
        print("new rows:: " + str(new_rows.shape[0]))
        # adding ID to new rows, incrementing after the max id present in the table
        new_rows["id"] = list(range(int(max_id+1), int(max_id + 1 + new_rows.shape[0])))
        # print(new_rows.head())

        outdated_rows = self.difference(self.current_df, unique_transform)
        print("outdated rows:: " + str(outdated_rows.shape[0]))
        # print(outdated_rows.head())

        self.data = self.difference(self.current_df, outdated_rows)
        self.data = self.data.append(new_rows)
        self.data = self.data.sort_values(by=["id"])

    def _after_load(self):
        data = self.data
        print("Load data:: " + str(data.shape[0]))
        # print(data.head())
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        self.destination.write(csv_buffer.getvalue(), backup=self.backup)
        print("Written loaded data")

    def unique(self, df):
        """
        Returns unique rows from dataframe
        :param df: input dataframe
        :return: unique rows in df
        """
        new_df = df.drop_duplicates(self.primary_keys)
        return new_df

    def difference(self, df1, df2):
        """
        Returns df1-df2 set operation
        :param df1: first pandas dataframe
        :param df2: second pandas dataframe
        :return: df1-df2 dataframe
        """
        return pd.concat([df1, df2, df2]).drop_duplicates(self.primary_keys, keep=False)
