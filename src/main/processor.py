"""
Requirement
Append the central company id to the output file.
Produce original file + Parquet output
"""



def main(input):
    pass




if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='Provides some metrics on the number of lyrics in the artists songs')
    #parser.add_argument("-a", "--artist", type=str, required=True,
                        help="Name of the artist")
    # Parse Command Line Arguments
    #args = parser.parse_args()
    #artist_name = args.artist


    main()

@staticmethod
    def get_valid_currency_codes():

        return valid_codes

    @staticmethod
    def get_valid_company_ids():
        df = Importer().import_csv("test_data/csv/lookups/companies.csv")
        valid_ids = [int(row['source_id']) for row in df.collect()]
        return valid_ids

    @staticmethod
    def get_valid_country_codes():
        df = Importer().import_csv("test_data/csv/lookups/countries.csv")
        valid_codes = [row['code'] for row in df.collect()]
        return valid_codes


# Enable Arrow-based columnar data transfers
self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")

logging.info(f"Importing XLSX file: {path}")

self.check_file_exists(path)

pd_dataframe = pd.read_excel(path, sheet_name, usecols=column_range, header=header).dropna()

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = self.spark.createDataFrame(pd_dataframe)

# print(df)
return df


schema = {
            'decimal_1': float,
            'decimal_2': float,
            'decimal_3': float,
            'decimal_4': float,
            'decimal_5': float,
            'decimal_6': float,
            'decimal_7': float,
            'country_code': str,
            'currency_code': str,
            'company_id': int
        }