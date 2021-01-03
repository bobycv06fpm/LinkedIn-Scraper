# LinkedIn-Scraper

## About

A Python script that uses BeautifulSoup and Selenium to scrape information from LinkedIn members' profiles.

**NOTE:** The script requires Google Chrome and valid LinkedIn credentials in order to run. Currently, it is legal to scrape publicly available data from LinkedIn. However, it is advised that you create a fake ‘dummy’ account to use for scraping, as there is a risk that scraping will cause the account to be banned/blocked.

## Setup

### 1. Install modules:

`pip3 install -r requirements.txt`

**NOTE:** Steps 2-5 below refer to the bit of code beneath the import statements in `linkedin_scraper.py`.

### 2. Insert path to input file:

`INPUT_FILE = r"INSERT_FILE_PATH_HERE.xlsx"`
Make sure to use forward slashes, e.g., `this/is/my/path/`.

**NOTE:** The script expects an Excel (.xlsx) file for the input. Input should be formatted as follows:

| Name     | URL                               |
| -------- | --------------------------------- |
| John Doe | https://www.linkedin.com/in/. . . |
| Jane Doe | https://www.linkedin.com/in/. . . |

### 3. Insert sheet name:

`SHEET_NAME = r"INSERT_SHEET_NAME_HERE"`

### 4. Insert path to output folder:

`OUTPUT_FOLDER = r"INSERT_FOLDER_PATH_HERE"`
Make sure to use forward slashes, e.g., `this/is/my/path/`.

### 5. Insert login credentials:

```
account_credentials = [
	[r"INSERT_EMAIL_HERE", r"INSERT_PASSWORD_HERE"]
]
```

You can add more than one set of credentials to `account_credentials`, if desired—the script will open a separate thread for each set of credentials.

**NOTE:** It is advised that you create a fake 'dummy' account to use for scraping, as there is a risk that scraping will cause the account to be banned/blocked.

### 6. Execute the script:

`python3 linkedin_scraper.py`
