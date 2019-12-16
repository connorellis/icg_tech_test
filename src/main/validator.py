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
    def validate_column_names(df, schema):
        df_cols = df.columns.tolist()
        schema_cols = list(schema.keys())

        if len(df_cols) != len(schema_cols):
            raise AssertionError("Schema doesn't match, different number of columns!")
        elif df_cols != schema_cols:
            missing_cols = ', '.join(list(set(schema_cols) - set(df_cols)))
            raise AssertionError(f'Columns in schema missing from data: {missing_cols}')

    def validate_data_types(self, df, schema):
        for field in schema:
            self.validate_data_type(df, field, schema[field])

    @staticmethod
    def validate_data_type(df, column_name, datatype):
        """
            attempt to cast the column as the schema data type
        """
        for index, row in df.iterrows():
            value = row[column_name]
            try:
                # attempt data conversion to schema data type
                datatype(row[column_name])
            except ValueError as e:
                raise ValueError(f'Error in row {index + 1}, value {value}')

    @staticmethod
    def validate_val_in_list(input_df, column_name, reference_list):

        # http://pandas-docs.github.io/pandas-docs-travis/#pandas.Series.isin
        # check column is in the reference list using isin()
        # ~ is negation
        invalid_rows = input_df[~input_df[column_name].isin(reference_list)]

        if int(invalid_rows.shape[0]) > 0:

            error_index_list = [str(x) for x in invalid_rows.index.tolist()]
            error_row_numbers = ', '.join(error_index_list)

            error_values_list = [str(x) for x in invalid_rows.iloc[:,0]]
            error_values = ', '.join(error_values_list)

            logging.error(f'Errors in the following rows {error_row_numbers} ')
            raise ValueError(f'The following values don\'t match the reference list: {error_values}')


    def cast_dataframe(self, df, schema):

        # first validate the data types are correct
        self.validate_data_types(df, schema)

        for column in df.columns:
            df[column] = df[column].astype(schema[column])

        return df
