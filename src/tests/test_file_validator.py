from src.main.validator import Validator
import unittest
import pandas as pd


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_data_types_validation_success(self):
        """ check we can cast the column """

        schema = {
            'decimal_1': float,
            'text': str
        }
        df = pd.DataFrame(data=[(1.9, "str1"), (7.5, "str2")], columns=['decimal_1', 'text'])

        df = Validator().validate_data_types(df, schema)
        self.assertIsNone(df)

    def test_data_types_validation_fail(self):
        """ test we get an error with correct value and row number when schema conflicts with data"""

        schema = {
            'decimal_1': float,
            'text': str
        }
        df = pd.DataFrame(data=[(1.9, "str1"), ('foo', "str2")], columns=['decimal_1', 'text'])

        try:
            df = Validator().validate_data_types(df, schema)
        except Exception as e:
            assert "row 2" in str(e)
            assert "foo" in str(e)
            assert e.__class__ == ValueError

    def test_float_data_type_validation_success(self):
        """ check we can cast the column """

        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1.9, 2, 3), columns=['decimal_1'])

        df = Validator().validate_data_type(df, 'decimal_1', schema['decimal_1'])
        self.assertIsNone(df)

    def test_string_to_float_fail(self):
        """ test we get an error with correct value and row number when we try invalid conversion"""
        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1, "foo", 3), columns=['decimal_1'])

        try:
            df = Validator().validate_data_type(df, 'decimal_1', schema['decimal_1'])
        except Exception as e:
            assert "row 2" in str(e)
            assert "foo" in str(e)
            assert e.__class__ == ValueError

    def test_ref_data_validation_pass(self):
        """ Test the validation passes without error when the data matches values in the ref data """
        df = pd.DataFrame(data=(1, 2, 3), columns=['test'])

        val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        self.assertIsNone(val)

    def test_ref_data_validation_null_fail(self):
        """ Test the error message logs when the data is null for ref data field """
        df = pd.DataFrame(data=(1, 2, 3, None), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert "nan" in str(e)
            assert e.__class__ == ValueError


    def test_ref_data_validation_value_fail(self):
        """ Test the error message logs when the data contains values outside of the ref data """
        df = pd.DataFrame(data=(1, 8, 2, 3), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert '8' in str(e)
            assert e.__class__ == ValueError

    def test_ref_data_validation_multivalue_fail(self):
        """ Test the error message logs all the invalid rows in the list """

        df = pd.DataFrame(data=(1, 8, 2, 3, None), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert "nan" in str(e)
            assert '8' in str(e)
            assert e.__class__ == ValueError

    def test_column_name_validation_pass(self):
        """ test an example where the column names are correct """

        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1, 2, 3), columns=['decimal_1'])

        val = Validator().validate_column_names(df, schema)

    def test_column_name_validation_fail(self):
        """ test we get a useful error when the column name validation fails """

        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1, 2, 3), columns=['err_col'])

        try:
            val = Validator().validate_column_names(df, schema)
        except Exception as e:
            assert "decimal_1" in str(e).lower()
            assert e.__class__ == AssertionError

    def test_different_column_count(self):
        """ test we get an error when we have more columns in sample_data than in the schema """

        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=[(1, 2), (1, 2)], columns=['decimal_1', 'decimal_2'])

        try:
            val = Validator().validate_column_names(df, schema)
        except Exception as e:
            assert "number" in str(e).lower()
            assert e.__class__ == AssertionError

    def test_different_column_count2(self):
        """ test we get an error when we have more columns in schema than in the sample data """
        schema = {
            'decimal_1': float,
            'decimal_2': float
        }
        df = pd.DataFrame(data=(1, 2), columns=['decimal_1'])

        try:
            val = Validator().validate_column_names(df, schema)
        except Exception as e:
            assert "number" in str(e).lower()
            assert e.__class__ == AssertionError

    def test_cast_dataframe_pass(self):
        """ cast dataframe that is already correct schema """
        schema = {
            'decimal_1': float,
            'text': str
        }
        df = pd.DataFrame(data=[(1.9, "str1"), (7.5, "str2")], columns=['decimal_1', 'text'])

        df = Validator().cast_dataframe(df, schema)

        assert df['decimal_1'].dtypes == float
        assert df['text'].dtypes == "object"

    def test_cast_dataframe_string_to_int_pass(self):
        """ test successful conversion of """
        schema = {
            'decimal_1': float,
            'should_be_int': int
        }
        df = pd.DataFrame(data=[(1.9, "1"), (7.5, "2")], columns=['decimal_1', 'should_be_int'])

        assert df['should_be_int'].dtypes == "object"

        df = Validator().cast_dataframe(df, schema)

        assert df['decimal_1'].dtypes == float
        assert df['should_be_int'].dtypes == int


    def test_cast_dataframe_fail(self):
        """ test we get a fail when we try to case invalid string to float """
        schema = {
            'decimal_1': float,
            'text': str
        }
        df = pd.DataFrame(data=[(1.9, "str1"), ("foo", "str2")], columns=['decimal_1', 'text'])

        try:
            df = Validator().cast_dataframe(df, schema)
        except Exception as e:
            assert "row 2" in str(e)
            assert "foo" in str(e)
            assert e.__class__ == ValueError


if __name__ == '__main__':
    unittest.main()
