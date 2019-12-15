# File Processing Pipeline - Connor Ellis
## Initial Setup


You will need to have python3.x installed, and the following modules:
* [venv](https://docs.python.org/3/library/venv.html) is used to create virtual environments
* [pybuilder](https://pybuilder.github.io/) is a build tool for Python

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
Todo
```bash

```

## Data Sources

* test_data/lookups/sample_data.csv was created manually
* test_data/lookups/countries.csv contains standard ISO codes and names and was downloaded from [here](https://datahub.io/core/country-list#resource-data)
    * Columns were reordered in Excel
* test_data/lookups/currencies.csv contains standard ISO codes and names and was downloaded from [here](https://datahub.io/JohnSnowLabs/iso-4217-currency-codes)
    * Tidied up in Excel
* test_data/lookups/companies.csv is manually created test data of some real companies with grave accents

