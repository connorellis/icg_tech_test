# File Processing Pipeline - Connor Ellis
## Initial Setup


You will need to have python3.x installed, and the following modules:
* [venv](https://docs.python.org/3/library/venv.html) is used to create virtual environments

To activate the virtual environment and install the dependencies, run the following commands 
```bash
cd path/to/project

# setup new virtual environment 
python3 -m venv venv

# activate the virtual environment
# Note this was written on Ubuntu - you will need to run venv\Scripts\activate.bat if on Windows
source venv/bin/activate

# install dependencies
pip3 install -r requirements.txt
```

## Execution

```bash
# cd path/to/project/root

# run unit tests
python3 -m unittest  discover -v -s src/tests

# test csv processing
python3 -m src.main.processor -d test_data/csv/csv_sample_data.csv -f csv -l test_data/csv/lookups -o output

# test xlsx processing
python3 -m src.main.processor -d test_data/xslx/excel_sample_data.xlsx -f xlsx -o output
```

## Data Sources

* test_data/lookups/sample_data.csv was created manually
* test_data/lookups/countries.csv contains standard ISO codes and names and was downloaded from [here](https://datahub.io/core/country-list#resource-data)
    * Columns were reordered in Excel
* test_data/lookups/currencies.csv contains standard ISO codes and names and was downloaded from [here](https://datahub.io/JohnSnowLabs/iso-4217-currency-codes)
    * Tidied up in Excel
* test_data/lookups/companies.csv is manually created test data of some real companies with grave accents

## Notes

Apologies, I ran out of time to include testing for the processing module