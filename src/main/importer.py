import logging
import pandas as pd
from src.main.utils import Utils
import os.path
import csv


class Importer:
    """
    This class is used to import data from files.
    It reads a single dataset and returns a spark dataframe
    """

    def __init__(self):
        # todo set up logging
        Utils.get_logger()

    @staticmethod
    def check_file_exists(path):
        logging.info("Checking file exists")
        print()
        if not os.path.exists(path):
            raise FileExistsError(f'{path} doesn\'t exist!')

    def import_csv(self, path):
        """         
        :param path: path to file
        :return: pandas dataframe
        
        reads the csv into a pandas dataframe and returns it
        """
        logging.info(f"Importing csv file {path}")

        self.check_file_exists(path)

        df = pd.read_csv(path)

        return df

    def import_xlsx(self, path, sheet_name, column_range, header=0):
        """
        :param path: path to file
        :return:
        
        reads the file with pandas, then converts to spark dataframe
        
        """
        # Enable Arrow-based columnar data transfers
        logging.info(f"Importing XLSX file: {path}")
        self.check_file_exists(path)

        df = pd.read_excel(path, sheet_name, usecols=column_range, header=header).dropna()

        return df


if __name__ == '__main__':
    val = Importer().import_csv('test_data/csv/lookups/companies.csv')
    print(val.columns.values.tolist())
    pass