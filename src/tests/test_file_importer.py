from src.main.importer import Importer
import unittest
import xlrd


class ImporterTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_big_csv(self):
        """ successful import of csv"""
        df = Importer().import_csv("test_data/csv/large_sample_data.csv")
        self.assertIsNotNone(df)
        self.assertEqual(df.shape[0], 500000)
        assert df.columns.values.tolist() == ['decimal_1',
                                              'decimal_2',
                                              'decimal_3',
                                              'decimal_4',
                                              'decimal_5',
                                              'decimal_6',
                                              'decimal_7',
                                              'country_code',
                                              'currency_code',
                                              'company_id']

    def test_sample_csv(self):
        """ successful import of csv"""
        df = Importer().import_csv("test_data/csv/csv_sample_data.csv")
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['decimal_1',
                                              'decimal_2',
                                              'decimal_3',
                                              'decimal_4',
                                              'decimal_5',
                                              'decimal_6',
                                              'decimal_7',
                                              'country_code',
                                              'currency_code',
                                              'company_id']

    def test_csv_companies_cols(self):
        """ successful import of csv"""
        df = Importer().import_csv("test_data/csv/lookups/companies.csv")
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['source_id', 'central_company_id', 'company_name']

    def test_csv_currencies_cols(self):
        """ successful import of csv"""
        df = Importer().import_csv("test_data/csv/lookups/currencies.csv")
        assert df.columns.values.tolist() == ['currency_code', 'currency_name']

    def test_csv_countries_cols(self):
        """ successful import of csv"""
        df = Importer().import_csv("test_data/csv/lookups/countries.csv")
        assert df.columns.values.tolist() == ['code', 'name']

    def test_csv_missing_file(self):
        """ test handling of missing csv """
        try:
            Importer().import_csv("test_data/csv/lookups/missing_file.csv")
        except Exception as e:
            assert e.__class__ == FileExistsError

    def test_xlsx_sample_data(self):
        """ check test data is imported and has correct columns """
        df = Importer().import_xlsx(path="test_data/xslx/excel_sample_data.xlsx",
                                    sheet_name='data',
                                    column_range=range(0, 10),
                                    header=0)
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['decimal_1',
                                              'decimal_2',
                                              'decimal_3',
                                              'decimal_4',
                                              'decimal_5',
                                              'decimal_6',
                                              'decimal_7',
                                              'country_code',
                                              'currency_code',
                                              'company_id']

    def test_xlsx_countries_cols(self):
        """ successful import of xlsx"""
        df = Importer().import_xlsx(path="test_data/xslx/excel_sample_data.xlsx",
                                    sheet_name='lookup',
                                    column_range=range(0, 2),
                                    header=1)
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['code', 'name']

    def test_xlsx_companies_cols(self):
        """ successful import of xlsx"""
        df = Importer().import_xlsx(path="test_data/xslx/excel_sample_data.xlsx",
                                    sheet_name='lookup',
                                    column_range=range(3, 6),
                                    header=1)
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['source_id', 'central_company_id', 'company_name']

    def test_xlsx_currencies_cols(self):
        """ successful import of xlsx"""
        df = Importer().import_xlsx(path="test_data/xslx/excel_sample_data.xlsx",
                                    sheet_name='lookup',
                                    column_range=range(7, 9),
                                    header=1)
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.shape[0], 1)
        assert df.columns.values.tolist() == ['currency_code', 'currency_name']

    def test_xlsx_missing_file(self):
        """ test correct handling of missing xlsx """
        try:
            Importer().import_xlsx("test_data/xslx/missing_file.xlsx",
                                   sheet_name='data',
                                   column_range=range(0, 10),
                                   header=0
            )
        except Exception as e:
            assert e.__class__ == FileExistsError

    def test_xlsx_missing_sheet(self):
        """ test correct handling of missing sheet """
        try:
            Importer().import_xlsx(path="test_data/xslx/excel_sample_data.xlsx",
                                   sheet_name='missing_sheet',
                                   column_range=range(0, 10),
                                   header=1)
        except Exception as e:
            print(e)
            assert e.__class__ == xlrd.biffh.XLRDError


if __name__ == '__main__':
    unittest.main()
