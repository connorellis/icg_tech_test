from src.main.validator import Validator
import unittest
import pandas as pd


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_data_type_validation_success(self):

        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1, 2, 3), columns=['decimal_1'])

        val = Validator().validate_column_names(df, schema)

    def test_ref_data_validation_pass(self):

        df = pd.DataFrame(data=(1, 2, 3), columns=['test'])

        val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        self.assertIsNone(val)


    def test_ref_data_validation_null_fail(self):

        df = pd.DataFrame(data=(1, 2, 3, None), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert "nan" in str(e)
            assert e.__class__ == ValueError


    def test_ref_data_validation_value_fail(self):

        df = pd.DataFrame(data=(1, 8, 2, 3), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert '8' in str(e)
            assert e.__class__ == ValueError

    def test_ref_data_validation_multivalue_fail(self):

        df = pd.DataFrame(data=(1, 8, 2, 3, None), columns=['test'])

        try:
            val = Validator().validate_val_in_list(df, 'test', [1, 2, 3, 4, 5])
        except Exception as e:
            assert "nan" in str(e)
            assert '8' in str(e)
            assert e.__class__ == ValueError


    def test_column_name_validation_pass(self):
        schema = {
            'decimal_1': float
        }
        df = pd.DataFrame(data=(1, 2, 3), columns=['decimal_1'])

        val = Validator().validate_column_names(df, schema)

    def test_column_name_validation_fail(self):
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

if __name__ == '__main__':
    unittest.main()
