import logging
import pandas as pd
from src.main.utils import Utils
import os.path


class Importer:
    """
    This class is used to import data from files.
    It reads a single dataset and returns a spark dataframe
    """

    def __init__(self):
        # todo set up logging
        Utils.get_logger()
        self.spark = Utils.get_spark_session()

    @staticmethod
    def check_file_exists(path):
        logging.info("Checking file exists")
        print()
        if not os.path.exists(path):
            raise FileExistsError(f'{path} doesn\'t exist!')


    def import_csv(self, path):
        """         
        :param path: path to file
        :param spark: the spark session
        :return: spark dataframe 
        
        reads the csv into a spark dataframe and returns it
        """
        logging.info(f"Importing csv file {path}")

        self.check_file_exists(path)

        df = self.spark.read.format("csv").option("header", "true").load(path)

        # print(df)
        return df


    def import_xlsx(self, path, sheet_name, column_range, header=0):
        """
        :param path: path to file
        :param spark: the spark session
        :return: 
        
        reads the file with pandas, then converts to spark dataframe
        
        """
        # Enable Arrow-based columnar data transfers
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")

        logging.info(f"Importing XLSX file: {path}")

        self.check_file_exists(path)

        pd_dataframe = pd.read_excel(path, sheet_name, usecols=column_range, header=header).dropna()

        # Create a Spark DataFrame from a Pandas DataFrame using Arrow
        df = self.spark.createDataFrame(pd_dataframe)

        # print(df)
        return df


if __name__ == '__main__':

    pass