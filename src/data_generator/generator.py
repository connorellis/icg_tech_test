"""
Requirement:
"In addition for the csv a file containing a large number of records (up to ½ million)"

NOTE: This module doesn't have tests and doesn't use any validation as it is a one-off script to help
me generate the larger test data file. It isn't connected to the main processing flow, hence why it is
in its own package.
"""

import csv
import random
from decimal import Decimal
from src.main.importer import Importer


def create_test_csv(path):
    """
    This function creates a a test-data csv file, that we can use to test the other modules
    in the package
    :return: void 
    """

    number_of_rows = 500000

    column_list = ['decimal_1', 'decimal_2', 'decimal_3', 'decimal_4', 'decimal_5', 'decimal_6',
                   'decimal_7', 'country_code', 'currency_code', 'company_id']

    currency_codes = get_valid_currency_codes()
    company_ids = get_valid_company_ids()
    country_codes = get_valid_country_codes()

    with open(path, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(column_list)

        for x in range(0, number_of_rows):
            row = [
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                Decimal(random.randrange(0, 100)) / 10,
                random.choice(country_codes),
                random.choice(currency_codes),
                random.choice(company_ids)
            ]
            writer.writerow(row)


def get_valid_currency_codes():
    df = Importer().import_csv("test_data/csv/lookups/currencies.csv")
    valid_codes = [row['currency_code'] for row in df.collect()]
    return valid_codes


def get_valid_company_ids():
    df = Importer().import_csv("test_data/csv/lookups/companies.csv")
    valid_ids = [int(row['source_id']) for row in df.collect()]
    return valid_ids


def get_valid_country_codes():
    df = Importer().import_csv("test_data/csv/lookups/countries.csv")
    valid_codes = [row['code'] for row in df.collect()]
    return valid_codes


if __name__ == '__main__':
    import os

    print(os.getcwd())
    path = "test_data/csv/large_sample_data.csv"
    create_test_csv(path)
