"""
Requirement
Append the central company id to the output file.
Produce original file + Parquet output
"""
from src.main.utils import Utils
from src.main.validator import Validator
from src.main.importer import Importer
import argparse
import logging
from os import path
from shutil import copyfile
# import xlrd
import pandas as pd

class Processor:

    def __init__(self):
        Utils.get_logger()

        parser = argparse.ArgumentParser(
            description='Provides some metrics on the number of lyrics in the artists songs')
        parser.add_argument("-d", "--data", type=str, required=True,
                            help="path to the sample data")

        parser.add_argument("-f", "--format", type=str, required=True,
                            choices=['xlsx', 'csv'],
                            help="path to the lookup data directory")

        parser.add_argument("-o", "--output", type=str, required=True,
                            help="path to the output directory")

        parser.add_argument("-l", "--lookups", type=str, required=False,
                            help="path to the lookup data directory")

        args = parser.parse_args()

        self.data_path = args.data
        self.file_format = args.format
        self.output_directory = args.output
        self.lookup_data = args.lookups

        if self.file_format == 'csv' and self.lookup_data is None:
            logging.info("Validating Args")
            raise AssertionError('lookup data directory must be provided for csv format')

        logging.info("Creating Spark Session")
        self.spark = Utils.get_spark_session()

        # variables to house the data
        self.data_df = None
        self.companies_df = None
        self.currencies_df = None
        self.countries_df = None

        # create the schemas
        # todo turn this into config files with null contraints, etc
        self.data_schema = {'decimal_1': float,
                            'decimal_2': float,
                            'decimal_3': float,
                            'decimal_4': float,
                            'decimal_5': float,
                            'decimal_6': float,
                            'decimal_7': float,
                            'country_code': str,
                            'currency_code': str,
                            'company_id': int}

        self.currencies_schema = {'currency_code': str,
                                  'currency_name': str}
        self.companies_schema = {'source_id': int,
                                 'central_company_id': int,
                                 'company_name': str}
        self.countries_schema = {'code': str,
                                 'name': str}

        self.clean_output_directory()

    def import_all_data(self):

        if self.file_format == 'csv':

            # import the data
            self.data_df = Importer().import_csv(self.data_path)
            self.companies_df = Importer().import_csv(path.join(self.lookup_data, "companies.csv"))
            self.currencies_df = Importer().import_csv(path.join(self.lookup_data, "currencies.csv"))
            self.countries_df = Importer().import_csv(path.join(self.lookup_data, "countries.csv"))

            #print(self.data_df)
            #print(self.companies_df)
            #print(self.currencies_df)
            #print(self.countries_df)

        elif self.file_format == 'xlsx':
            self.data_df = Importer().import_xlsx(self.data_path,
                                                  sheet_name='data',
                                                  column_range=range(0, 10),
                                                  header=0)

            self.companies_df = Importer().import_xlsx(self.data_path,
                                                       sheet_name='lookup',
                                                       column_range=range(3, 6),
                                                       header=1)

            self.currencies_df = Importer().import_xlsx(self.data_path,
                                                        sheet_name='lookup',
                                                        column_range=range(7, 9),
                                                        header=1)

            self.countries_df = Importer().import_xlsx(self.data_path,
                                                       sheet_name='lookup',
                                                       column_range=range(0, 2),
                                                       header=1
                                                       )
        else:
            raise ValueError(f'Invalid file format {self.file_format}')

    def perform_validations(self):
        """
        •             Validate data types
        •             Validate Country against a list
        •             Validate currency against a list
        •             Validate company against your lookup file using the company source id from you dummy data.
        """
        """ todo make this more generic """

        v = Validator()
        # validate countries data
        v.validate_column_names(self.countries_df, self.countries_schema)
        v.cast_dataframe(self.countries_df, self.countries_schema)

        # validate companies data
        v.validate_column_names(self.companies_df, self.companies_schema)
        v.cast_dataframe(self.companies_df, self.companies_schema)

        # validate currencies data
        v.validate_column_names(self.currencies_df, self.currencies_schema)
        v.cast_dataframe(self.currencies_df, self.currencies_schema)

        # validate sample data
        v.validate_column_names(self.data_df, self.data_schema)
        v.cast_dataframe(self.data_df, self.data_schema)

        # validate countries lookup data
        v.validate_val_in_list(self.data_df, 'country_code', self.countries_df['code'].tolist())

        # validate companies lookup data
        v.validate_val_in_list(self.data_df, 'company_id', self.companies_df['source_id'].tolist())

        # validate currencies lookup data
        v.validate_val_in_list(self.data_df, 'currency_code', self.currencies_df['currency_code'].tolist())

    def process(self):

        self.import_all_data()
        self.perform_validations()

        # add the central company id to the dataframe
        processed_data = self.process_data()
        self.output_original_file(processed_data)
        self.output_parquet(processed_data)

    def process_data(self):
        """ function to join dataframes using spark """
        # Enable Arrow-based columnar data transfers
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        # Create a Spark DataFrame from a Pandas DataFrame using Arrow
        sample_data = self.spark.createDataFrame(self.data_df)
        companies_data = self.spark.createDataFrame(self.companies_df)

        joined_data = sample_data.join(companies_data, sample_data.company_id == companies_data.source_id)
        cols = ['decimal_1',
                'decimal_2',
                'decimal_3',
                'decimal_4',
                'decimal_5',
                'decimal_6',
                'decimal_7',
                'country_code',
                'currency_code',
                'company_id',
                'central_company_id']

        #print(joined_data.select(*cols).show())
        return joined_data.select(*cols)

    def output_original_file(self, df):
        if self.file_format == 'csv':
            # move all data to single node so we can write out 1 csv
            df.repartition(1).write.csv(path.join(self.output_directory, "output.csv"), header='true')

        if self.file_format == 'xlsx':

            # copy Excel doc across to the output
            output_path = path.join(self.output_directory, "output.xlsx")
            copyfile(self.data_path, output_path)

            # open source excle
            xlsx = pd.ExcelFile(self.data_path)
            # convert spark output to pandas dataframe
            pd_df = df.toPandas()

            # open target output
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')

            # transfers all existing sheets
            for sheet in xlsx.sheet_names:
                sheet_data = xlsx.parse(sheet_name=sheet, index_col=0)
                sheet_data.to_excel(writer, sheet_name=sheet)

            pd_df.to_excel(writer, sheet_name='data', index=False)
            writer.save()
            writer.close()



    def output_parquet(self, df):
        df.write.parquet(path.join(self.output_directory, "output.parquet"))

    def clean_output_directory(self):
        #todo clean output directory
        pass

if __name__ == '__main__':
    try:
        Processor().process()
    except Exception as e:
        logging.exception(e)
        raise e