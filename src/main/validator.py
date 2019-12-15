"""
•             Validate data types
•             Validate Country against a list
•             Validate currency against a list
•             Validate company against your lookup file using the company source id from you dummy data.

"""

import logging
import pandas as pd
from src.main.utils import Utils
from src.main.importer import Importer
import os.path
from decimal import Decimal

class Validator:

    def __init__(self):
        Utils.get_logger()

    @staticmethod
    def validate_schema(df, schema):

        print(df)
        print(schema)

        total_rows['ColumnID'] = total_rows['ColumnID'].astype(str)

        #todo check columns

    @staticmethod
    def validate_column_names(df, schema):
        df_cols = df.columns.tolist()
        schema_cols = list(schema.keys())

        if len(df_cols) != len(schema_cols):
            raise AssertionError("Schema doesn't match, different number of columns!")
        elif df_cols != schema_cols:
            missing_cols = ', '.join(list(set(schema_cols) - set(df_cols)))
            raise AssertionError(f'Columns in schema missing from data: {missing_cols}')

    @staticmethod
    def validate_data_types(dt1, dt2):
        pass

    @staticmethod
    def data_type(dt1, dt2):
        """ checks if data type 1 is a """


    @staticmethod
    def validate_val_in_list(input_df, column_name, reference_list):

        # http://pandas-docs.github.io/pandas-docs-travis/#pandas.Series.isin
        # check column is in the reference list using isin()
        # ~ is negation
        invalid_rows = input_df[~input_df[column_name].isin(reference_list)]

        print(invalid_rows)

        if int(invalid_rows.count()) > 0:

            error_index_list = [str(x) for x in invalid_rows.index.tolist()]
            error_row_numbers = ', '.join(error_index_list)

            error_values_list = [str(x) for x in invalid_rows.iloc[:,0]]
            error_values = ', '.join(error_values_list)

            logging.error(f'Errors in the following rows {error_row_numbers} ')
            raise ValueError(f'The following values don\'t match the reference list: {error_values}')
