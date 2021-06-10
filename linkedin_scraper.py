#!/usr/bin/env python3

# =============================================================================
import csv
import datetime
import json
import logging
import math
import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# =============================================================================

# (Be sure to use FORWARD slashes when entering the paths below, e.g., "this/is/my/path/")

# 1. Insert path to the input file below (inside the quotation marks "")

# =================================#
# INSERT PATH TO INPUT FILE BELOW  # (Code expects an Excel (.xlsx) file)
# =================================# (Use FORWARD slashes, e.g., "this/is/my/path/")
INPUT_FILE = r"INSERT HERE"

# 2. Insert sheet name below (inside the quotation marks "")

# ===========================#
# INSERT NAME OF SHEET BELOW #
# ===========================#

SHEET_NAME = r"INSERT HERE"

# 3. Insert path to output folder below (inside the quotation marks "")

# =======================================#
# INSERT PATH TO OUTPUT DIRECTORY BELOW  # (By default, output will be saved to wherever this code is saved)
# =======================================# (Use FORWARD slashes, e.g., "this/is/my/path/")
OUTPUT_FOLDER = r"INSWERT HERE"

# 4. Insert LinkedIn account credentials inside the quotation marks below (DELETE INFORMATION BEFORE SHARING THIS CODE!)

# NOTE: It is advised that you create a fake 'dummy' account to use for scraping, as there
# is a risk that scraping will cause the account to be banned/blocked.

# NOTE: The scraper should be able to use your credentials to sign you in automatically.
# If this automation fails or is unpreferred, please sign in manually--if done promptly,
# the scraper should still be able to proceed normally.

account_credentials = [
    [r"USERNAME", r"PASSWORD"]
    # Add more credentials to this list, if desired; the scraper will open a separate thread for each set of credentials
]
NUM_THREADS = len(account_credentials)

# Construct output paths and store them as strings
Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
output_folder = Path(OUTPUT_FOLDER)
input_file_path_string = str(Path(INPUT_FILE).resolve())
overview_output_path_string = str((output_folder / "Overview.csv").resolve())
experience_output_path_string = str((output_folder / "Experience.csv").resolve())
education_output_path_string = str((output_folder / "Education.csv").resolve())
scraper_progress_output_path_string = str(Path("Scraper_Progress.csv").resolve())
# scraper_pkl_output_path_string = str((output_folder / "Scraper_Progress.pkl").resolve())

overview_output_columns = [
    "URL",
    "Name",
    "Photo",
    "Headline",
    "Location",
    "About",
    "First Job Start Year",
    "Work Years",
    "Work Months",
    "Num Unique Employers",
    "Scraping Notes",
    "Scraping Errors",
]

experience_output_columns = [
    "URL",
    "Name",
    "Job Title",
    "Employer",
    "Start Date",
    "End Date",
    "Description",
]

education_output_columns = [
    "URL",
    "Name",
    "School",
    "Degree",
    "Field of Study",
    "Description",
]


# If encountering a read/write error, the number of times the program will retry before exiting
MAX_TRIES = 10

# Max amount of entries per script run
MAX_ENTRIES = 10
ENTRIES_UNTIL_UPDATE = 10

PRESENT_DATE = datetime.today()


def handle_date(dates, start_dates, end_dates):
    if len(dates[0]) == 4:
        start_dates.append(datetime.strptime(dates[0], "%Y"))
    else:
        start_dates.append(datetime.strptime(dates[0], "%b %Y"))

    # There exists edge cases when assigning the end date that,
    # when handled, makes calculating total work experience length easier
    if len(dates) == 2:
        # End date is present
        if dates[1] == "Present":
            # make end date today's date
            end_dates.append(PRESENT_DATE)
        elif len(dates[1]) == 4:
            # Case: only a year is present
            # In this case, set to the end
            # of the previous year
            date = datetime.strptime(dates[1], "%Y")
            end_dates.append(date.replace(month=12, year=date.year - 1))
        else:
            # Case: year and month present
            end_dates.append(datetime.strptime(dates[1], "%b %Y"))
    else:
        # Case: when only a single date (start date) is listed
        if len(dates[0]) == 4:
            # Case: only a year is listed
            # Set end date to the end of the year unless
            # in the current year (then set to current date)
            date = datetime.strptime(dates[0], "%Y")
            if date.year == datetime.today().year:
                end_dates.append(PRESENT_DATE)
            else:
                end_dates.append(date.replace(month=12, year=date.year))
        else:
            # Case: month and year are listed
            # set end date to the same month
            end_dates.append(
                start_dates[-1].replace(
                    month=start_dates[-1].month, year=start_dates[-1].year
                )
            )


def print_error(exception, error_message):
    logging.info(exception)
    logging.info(error_message)
    logging.info("Retrying...")


def wait_until_present(by, identifier, d, time):
    try:
        WebDriverWait(d, time).until(EC.visibility_of_element_located((by, identifier)))
    except:
        return False
    return True


def handle_output_file(csv_file_path, column_names):
    if not os.path.exists(csv_file_path):
        tries = 0
        while tries < MAX_TRIES:
            try:
                with open(csv_file_path, "w+", newline="") as output_file:
                    writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)
                    writer.writerow(column_names)
                break
            except Exception as e:
                print_error(e, "Error creating output file")
                tries += 1
                continue
        if tries >= MAX_TRIES:
            logging.info("Creating output file failed. Exiting...")
            sys.exit(1)


def handle_output_files():
    handle_output_file(overview_output_path_string, overview_output_columns)
    handle_output_file(experience_output_path_string, experience_output_columns)
    handle_output_file(education_output_path_string, education_output_columns)


def write_to_files_helper_1(file_path, df):
    with open(file_path, "a", newline="") as output_file:
        writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)
        for row in df.values.tolist():
            writer.writerow(row)


def write_to_files_helper_2(file_path, name, new_data):
    entries = []
    with open(file_path, "r", newline="") as file:
        reader = csv.reader(
            file,
            quotechar='"',
            delimiter=",",
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True,
        )
        for row in reader:
            entries.append(row)
    with open(file_path, "w", newline="") as file:
        if isinstance(new_data, list):
            for new_row in new_data:
                line_to_overwrite = {str(name): new_row}
                writer = csv.writer(file, quoting=csv.QUOTE_ALL)
                for row in entries:
                    logging.info(row)
                    data = line_to_overwrite.get(row[0], row)
                    writer.writerow(data)
        else:
            line_to_overwrite = {str(name): new_data}
            writer = csv.writer(file, quoting=csv.QUOTE_ALL)
            for row in entries:
                logging.info(row)
                data = line_to_overwrite.get(row[0], row)
                writer.writerow(data)


def write_to_files(overview_entry, experience_entry, education_entry, name, attempted):
    logging.info("Writing to file...")
    output_semaphore.acquire()

    overview_df = pd.DataFrame([], columns=overview_output_columns)
    new_overview_row = []
    for key in [
        row_name.replace(" ", "_").lower() for row_name in overview_output_columns
    ]:
        new_overview_row.append(overview_entry.get(key))
    overview_df.loc[len(overview_df)] = new_overview_row

    experience_df = pd.DataFrame([], columns=experience_output_columns)
    new_experience_rows = []
    for i in range(len(experience_entry.get("job_title"))):
        new_experience_row = []
        for key in [
            row_name.replace(" ", "_").lower() for row_name in experience_output_columns
        ]:
            value = experience_entry.get(key)
            if isinstance(value, list):
                try:
                    new_experience_row.append(value[i])
                except Exception as e:
                    print(key)
                    print(value)
                    print(i)
                    sys.exit(1)
            else:
                new_experience_row.append(value)

        new_experience_rows.append(new_experience_row)
        experience_df.loc[len(experience_df)] = new_experience_row

    education_df = pd.DataFrame([], columns=education_output_columns)
    new_education_rows = []
    for i in range(len(education_entry.get("school"))):
        new_education_row = []
        for key in [
            row_name.replace(" ", "_").lower() for row_name in education_output_columns
        ]:
            value = education_entry.get(key)
            if isinstance(value, list):
                new_education_row.append(value[i])
            else:
                new_education_row.append(value)
        new_education_rows.append(new_education_row)
        education_df.loc[len(education_df)] = new_education_row

    tries = 0
    if not attempted:
        while tries < MAX_TRIES:
            try:
                write_to_files_helper_1(overview_output_path_string, overview_df)
                write_to_files_helper_1(experience_output_path_string, experience_df)
                write_to_files_helper_1(education_output_path_string, education_df)
                break
            except Exception as e:
                print_error(e, "Error writing to file (make sure file is not open)")
                tries += 1
                continue
    else:
        while tries < MAX_TRIES:
            try:
                write_to_files_helper_2(
                    overview_output_path_string, name, new_overview_row
                )
                write_to_files_helper_2(
                    experience_output_path_string, name, new_experience_rows
                )
                write_to_files_helper_2(
                    education_output_path_string, name, new_education_rows
                )
                break
            except Exception as e:
                print_error(e, "Error writing to file (make sure file is not open)")
                tries += 1
                continue
    output_semaphore.release()
    if tries >= MAX_TRIES:
        logging.info("Writing to file failed. Exiting...")
        return False
    logging.info("Done!")
    return True


def update_progress(status, scraper_progress, url):
    progress_semaphore.acquire()
    url_row = (scraper_progress.index[scraper_progress["URL"] == url].tolist())[0]
    scraper_progress.at[url_row, "Scraped?"] = status
    progress_semaphore.release()


def update_progress_file(scraper_progress):
    logging.info("Logging progress...")
    tries = 0
    progress_file_semaphore.acquire()
    while tries < MAX_TRIES:
        try:
            scraper_progress.to_csv(scraper_progress_output_path_string, index=False)
            break
        except Exception as e:
            print_error(e, "Error logging progress")
            tries += 1
            continue
    progress_file_semaphore.release()
    if tries >= MAX_TRIES:
        logging.info("Logging progress failed. Exiting...")
        return False
    logging.info("Done!")
    logging.info(
        "===================================================================\n"
    )
    return True


def has_experience(j):
    for k, v in j.items():
        for x in v:
            for k, v in x.items():
                if isinstance(v, (list)) and str(v).endswith("FullProfilePosition']"):
                    return True
                else:
                    continue
    return False


def has_education(j):
    for k, v in j.items():
        for x in v:
            for k, v in x.items():
                if isinstance(v, (list)) and str(v).endswith("FullProfileEducation']"):
                    return True
                else:
                    continue
    return False


def get_profile_data(j):
    profile_data = []
    for k, v in j.items():
        for x in v:
            for k, v in x.items():
                if isinstance(v, (list)) and str(v).startswith(
                    "['com.linkedin.voyager.dash.deco.identity.profile.FullProfileWithEntities'"
                ):
                    profile_data.append(x)
    return profile_data


def extract_profile_data_to_dict(profile_keys, profile_data, dict_to_populate):
    if isinstance(profile_data, dict):
        for k, v in profile_data.items():
            if k in profile_keys:
                dict_to_populate.update({k: v})
    elif isinstance(profile_data, list):
        for item in profile_data:
            extract_profile_data_to_dict(profile_keys, item, dict_to_populate)
    return dict_to_populate


def get_section_items(driver, section, entity, expand, name, entry):
    # Scrolls to section
    # Expands section
    # Loads section
    items = []
    try:
        attempts = 0
        while not wait_until_present(By.ID, section, driver, 2):
            if attempts > 20:
                raise Exception("Could not find " + name + " section")
            else:
                attempts += 1
                logging.info("Scrolling for " + name + " section...")
                driver.execute_script("window.scrollBy(0,200)")
        # Check for "Show more" button(s) in work experience section
        time.sleep(2)
        while True:
            try:
                if not wait_until_present(By.ID, section, driver, 15):
                    entry["scraping_errors"] = (
                        entry["scraping_errors"] + name + " did not load in time\n"
                    )
                    logging.info(name + " did not load in time")
                    break
                section_driver = driver.find_element_by_id(section)
                items = driver.find_elements_by_css_selector(entity)
                driver.execute_script("arguments[0].scrollIntoView();", items[-1])
                try:
                    see_more_jobs = section_driver.find_element_by_css_selector(expand)
                    driver.execute_script("arguments[0].scrollIntoView();", items[-3])
                    see_more_jobs.click()
                    time.sleep(1.5)
                    break
                # Must include stale element exception handler because in some cases,
                # if the profile has 20+ roles, clicking see more would reload the section
                # and cause our reference to become stale
                except StaleElementReferenceException:
                    items = driver.find_elements_by_css_selector(entity)
                    see_more_jobs = section_driver.find_element_by_css_selector(expand)
                    driver.execute_script("arguments[0].scrollIntoView();", items[-3])
                    see_more_jobs.click()
                    time.sleep(1.5)
            except Exception as e:
                break
        if not wait_until_present(By.ID, section, driver, 15):
            raise Exception(name + " did not load in time")
        else:
            items = driver.find_elements_by_css_selector(entity)
    except Exception as e:
        logging.info(e, exc_info=True)
        entry["scraping_errors"] = entry["scraping_errors"] + str(e) + "\n"
        return None
    return items


def parse_entries(driver, urls, names, scraper_progress):

    entries_scraped = 0  # Number of entries scraped by thread
    unupdated_entrees = 0

    for index in range(len(urls)):
        if entries_scraped >= MAX_ENTRIES:
            break
        if unupdated_entrees > 0 and unupdated_entrees % ENTRIES_UNTIL_UPDATE == 0:
            unupdated_entrees = 0
            update_progress_file(scraper_progress)

        url = urls[index]
        name = names[index]

        overview_entry = {
            "url": url,
            "name": name,
            "photo": "",
            "headline": "",
            "location": "",
            "about": "",
            "first_job_start_year": "",
            "work_years": "",
            "work_months": "",
            "num_unique_employers": "",
            "scraping_notes": "",
            "scraping_errors": "",
        }

        experience_entry = {
            "url": url,
            "name": name,
            "job_title": [],
            "employer": [],
            "start_date": [],
            "end_date": [],
            "description": [],
        }

        education_entry = {
            "url": url,
            "name": name,
            "school": [],
            "degree": [],
            "field_of_study": [],
            "description": [],
        }

        overview_entry["url"] = url
        overview_entry["name"] = name
        overview_entry["scraping_notes"] = ""
        overview_entry["sraping_erros"] = ""
        attempted = False

        # Check whether the url has already been scraped
        progress_semaphore.acquire()
        scrape_status = (
            scraper_progress.loc[(scraper_progress["URL"] == url), "Scraped?"]
            .values[0]
            .strip()
        )
        if scrape_status == "Yes" or scrape_status == "Attempting":
            progress_semaphore.release()
            continue
        elif scrape_status == "Attempted":
            attempted = True
        url_row = (scraper_progress.index[scraper_progress["URL"] == url].tolist())[0]
        scraper_progress.at[url_row, "Scraped?"] = "Attempting"
        progress_semaphore.release()

        # if entries_scraped != 0:
        #     logging.info('Waiting buffer start')
        #     time.sleep(120)
        #     logging.info('Waiting buffer ended')

        entries_scraped += 1
        unupdated_entrees += 1

        driver.get(url)

        logging.info(
            "==================================================================="
        )
        logging.info("Currently scraping:")
        logging.info("URL = " + url)
        logging.info("Name = " + str(name))

        if not wait_until_present(By.ID, "profile-content", driver, 15):
            overview_entry["scraping_errors"] = "Profile page did not load in time"
            logging.info("Profile page did not load in time")
            update_progress("Attempted", scraper_progress, url)
            if not write_to_files(
                overview_entry, experience_entry, education_entry, name, attempted
            ):
                unupdated_entrees = 0
                update_progress_file(scraper_progress)
                return
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")

        url_code_tag = soup.find_all("code")
        profile_json = []
        for utag in url_code_tag:
            if utag.find(text=re.compile("\\*profile")) and utag.find(
                text=re.compile("\\*elements")
            ):
                j_data = json.loads(utag.text)
                profile_json.append(j_data)
        if len(profile_json) == 0:
            overview_entry["scraping_errors"] = "Could not find profile json"
            logging.info("Could not find profile json")
            update_progress("Attempted", scraper_progress, url)
            if not write_to_files(
                overview_entry, experience_entry, education_entry, name, attempted
            ):
                unupdated_entrees = 0
                update_progress_file(scraper_progress)
                return
            continue

        del profile_json[-1]["data"]
        del profile_json[-1]["meta"]

        # --------------------------------- #
        # |         Basic Info            | #
        # --------------------------------- #

        profile_keys = ["headline", "summary"]
        profile_data = get_profile_data(profile_json[-1])
        profile_data_dict = {}

        extract_profile_data_to_dict(profile_keys, profile_data, profile_data_dict)

        overview_entry["headline"] = profile_data_dict.get("headline")
        overview_entry["about"] = profile_data_dict.get("summary")

        try:
            '''
            location = basic_info.find_element_by_css_selector(
                ".pv-top-card--list-bullet > li:nth-child(1)"
            ).text
            '''
            location = driver.find_element_by_css_selector(
                "div.pb2 > span.text-body-small.inline.t-black--light.break-words"
            ).text
            overview_entry["location"] = location
        except Exception as e:
            logging.info("Error getting location")
            overview_entry["scraping_notes"] = (
                overview_entry["scraping_notes"] + "Error getting location" + "\n"
            )
        try:
            image = driver.find_element_by_css_selector(
                'img[alt="' + overview_entry["name"] + '"]'
            )
            if "ghost-person" in image.get_attribute("class"):
                overview_entry["photo"] = str(None)
            else:
                overview_entry["photo"] = image.get_attribute("src")
        except Exception as e:
            logging.info("Error getting photo")
            overview_entry["scraping_notes"] = (
                overview_entry["scraping_notes"] + "Error getting photo" + "\n"
            )

        # --------------------------------- #
        # |     Experience section        | #
        # --------------------------------- #

        logging.info("Searching Experiences")

        driver_exp_all_items = []

        # Better to check json than spend the time to do the following expands and check
        if has_experience(profile_json[-1]):
            driver_exp_all_items = get_section_items(
                driver,
                "experience-section",
                ".pv-profile-section__card-item-v2",
                "button.pv-profile-section__see-more-inline",
                "Experience",
                overview_entry,
            )
            if driver_exp_all_items is not None:
                job_titles = []
                employers = []
                unique_employers = []
                start_dates = []
                end_dates = []
                valid_date_pairs = 0
                exp_descriptions = []

                exp_order = 1

                if len(driver_exp_all_items) == 0:
                    overview_entry["scraping_notes"] = (
                        overview_entry["scraping_notes"] + "No experiences listed\n"
                    )
                    logging.info("No experiences listed")
                else:
                    logging.info("Iterating experiences")

                for element in driver_exp_all_items:
                    driver.execute_script("arguments[0].scrollIntoView();", element)
                    # Check for multiple roles under one company
                    multi_role_element = True
                    try:
                        element.find_element(
                            By.XPATH, ".//ul[(@class='pv-entity__position-group mt2')]"
                        )
                    except NoSuchElementException as e:
                        multi_role_element = False
                    else:
                        pass

                    if multi_role_element:
                        # Get employer
                        try:
                            employer_element = element.find_element_by_css_selector(
                                ".pv-entity__company-summary-info"
                            )
                            employer = employer_element.find_element_by_xpath(
                                ".//h3/span[last()]"
                            ).text
                        except Exception as e:
                            employer = ""
                            logging.info(e)
                            logging.info(
                                "Error getting employer for job " + str(exp_order)
                            )
                            overview_entry["scraping_notes"] = (
                                overview_entry["scraping_notes"]
                                + "Error getting employer for job "
                                + str(exp_order)
                                + "\n"
                            )

                        roles = []

                        # Show more roles
                        while True:
                            try:
                                roles = element.find_elements_by_css_selector(
                                    ".pv-entity__role-details-container"
                                )
                                see_more_roles = roles.find_element_by_css_selector(
                                    ".button.pv-profile-section__see-more-inline pv-profile-section__text-truncate-toggle"
                                )
                                see_more_roles.click()
                            except Exception as e:
                                break

                        for role in roles:
                            employers.append(employer)
                            # Get job title
                            try:
                                job_header = role.find_element_by_tag_name("h3")
                                job_title = job_header.find_elements_by_tag_name(
                                    "span"
                                )[-1].text
                                job_titles.append(job_title)
                            except Exception as e:
                                job_titles.append("")
                                logging.info(e)
                                logging.info(
                                    "Error getting job title for job " + str(exp_order)
                                )
                                overview_entry["scraping_notes"] = (
                                    overview_entry["scraping_notes"]
                                    + "Error getting job title for job "
                                    + str(exp_order)
                                    + "\n"
                                )

                            # Get position start and end dates
                            try:
                                date_range_element = role.find_element_by_css_selector(
                                    ".pv-entity__date-range"
                                )
                                date_range = date_range_element.find_element_by_xpath(
                                    ".//span[last()]"
                                ).text
                                dates = [date.strip() for date in date_range.split("–")]
                                handle_date(dates, start_dates, end_dates)
                                valid_date_pairs += 1
                            except Exception as e:
                                start_dates.append("")
                                end_dates.append("")
                                logging.info(e)
                                logging.info(
                                    "Error getting start and end dates for job "
                                    + str(exp_order)
                                )
                                overview_entry["scraping_notes"] = (
                                    overview_entry["scraping_notes"]
                                    + "Error getting start and end dates for job "
                                    + str(exp_order)
                                    + "\n"
                                )

                            # Get description
                            try:
                                exp_description_section = (
                                    role.find_element_by_css_selector(
                                        ".pv-entity__description"
                                    )
                                )
                                # Check for "see more" button in description
                                see_more_button_present = False
                                try:
                                    see_more_desc = role.find_element_by_css_selector(
                                        ".inline-show-more-text__button"
                                    )
                                    if (
                                        see_more_desc.get_attribute("aria-expanded")
                                        == "false"
                                    ):
                                        see_more_desc.click()
                                    exp_description_section = (
                                        role.find_element_by_css_selector(
                                            ".pv-entity__description"
                                        )
                                    )
                                    see_more_button_present = True
                                    time.sleep(1.5)
                                except Exception as e:
                                    pass
                                # Remove "see less" link text from description
                                if see_more_button_present:
                                    exp_description = exp_description_section.text[:-8]
                                    exp_descriptions.append(exp_description)
                                else:
                                    exp_description = exp_description_section.text
                                    exp_descriptions.append(exp_description)
                            except Exception as e:
                                exp_descriptions.append("")
                                logging.info(
                                    "(No description listed for job "
                                    + str(exp_order)
                                    + ")"
                                )

                            exp_order = exp_order + 1
                    else:
                        pass
                        # Get job title
                        try:
                            job_title = element.find_element_by_tag_name("h3").text
                            job_titles.append(job_title)
                        except Exception as e:
                            job_titles.append("")
                            logging.info(e)
                            logging.info(
                                "Error getting job title for job " + str(exp_order)
                            )
                            overview_entry["scraping_notes"] = (
                                overview_entry["scraping_notes"]
                                + "Error getting job title for job "
                                + str(exp_order)
                                + "\n"
                            )

                        # Get employer
                        try:
                            employer = element.find_element_by_css_selector(
                                ".pv-entity__secondary-title"
                            ).text
                            employers.append(employer)
                        except Exception as e:
                            employers.append("")
                            logging.info(e)
                            logging.info(
                                "Error getting employer for job " + str(exp_order)
                            )
                            overview_entry["scraping_notes"] = (
                                overview_entry["scraping_notes"]
                                + "Error getting employer for job "
                                + str(exp_order)
                                + "\n"
                            )

                        # Get position start and end dates
                        try:
                            date_range_element = element.find_element_by_css_selector(
                                ".pv-entity__date-range"
                            )
                            date_range = date_range_element.find_element_by_xpath(
                                ".//span[last()]"
                            ).text
                            dates = [date.strip() for date in date_range.split("–")]
                            handle_date(dates, start_dates, end_dates)
                            valid_date_pairs += 1
                        except Exception as e:
                            start_dates.append("")
                            end_dates.append("")
                            logging.info(e)
                            logging.info(
                                "Error getting start and end dates for job "
                                + str(exp_order)
                            )
                            overview_entry["scraping_notes"] = (
                                overview_entry["scraping_notes"]
                                + "Error getting start and end for job "
                                + str(exp_order)
                                + "\n"
                            )

                        # Get description
                        try:
                            exp_description_section = (
                                element.find_element_by_css_selector(
                                    ".pv-entity__description"
                                )
                            )
                            # Check for "see more" button in description
                            see_more_button_present = False
                            try:
                                see_more_desc = element.find_element_by_css_selector(
                                    ".inline-show-more-text__button"
                                )
                                if (
                                    see_more_desc.get_attribute("aria-expanded")
                                    == "false"
                                ):
                                    see_more_desc.click()
                                exp_description_section = (
                                    element.find_element_by_css_selector(
                                        ".pv-entity__description"
                                    )
                                )
                                see_more_button_present = True
                                time.sleep(1.5)
                            except Exception as e:
                                pass
                            if see_more_button_present:
                                exp_description = exp_description_section.text[:-8]
                                exp_descriptions.append(exp_description)
                            else:
                                exp_description = exp_description_section.text
                                exp_descriptions.append(exp_description)
                        except Exception as e:
                            exp_descriptions.append("")
                            logging.info(
                                "(No description listed for job " + str(exp_order) + ")"
                            )

                        # Increment experience order
                        exp_order += 1

                # --------------------------------- #
                # |      Experience years         | #
                # --------------------------------- #

                if valid_date_pairs > 0:
                    # To calculate total work experience length, I
                    # Calculate total months worked (using 30.4 as the average
                    # amount of day in a month to simplify the code).

                    # If the start is Jan 20xx to May 20xx, the total months will
                    # be 5 as it calculates from the start of Jan to the end of May.
                    # For 20xx to 20yy, the total months will be (yy - xx) * 12,
                    # including the beginning of 20xx and excluding the first month
                    # of 20yy (this is how LinkedIn does it)
                    # For Month 20xx to 20xx, I consider the end date to be the end of
                    # 20xx (as does LinkedIn)

                    first_job_start_year = datetime.today()
                    total_mths = 0
                    start_anchor = start_dates[0]
                    for i, end_date in enumerate(end_dates):
                        if end_date == "" or start_dates[i] == "":
                            continue

                        if first_job_start_year > start_dates[i]:
                            first_job_start_year = start_dates[i]

                        dif = abs((end_date - start_dates[i]).days / 30.4) + 1
                        if i != 0:
                            if start_anchor != "" and end_date > start_anchor:
                                dif -= abs((end_date - start_anchor).days / 30.4) + 1
                                if start_dates[i] > start_anchor:
                                    continue
                                else:
                                    start_anchor = start_dates[i]
                            elif start_anchor != "" and end_date == start_anchor:
                                dif -= 1
                                start_anchor = start_dates[i]
                            else:
                                start_anchor = start_dates[i]
                        total_mths += dif
                    total_mths = round(total_mths)

                    work_months = total_mths % 12
                    work_years = math.floor(total_mths / 12)
                    overview_entry[
                        "first_job_start_year"
                    ] = first_job_start_year.strftime("%Y")
                    overview_entry["work_years"] = work_years
                    overview_entry["work_months"] = work_months
                else:
                    pass

                # Unique employers
                unique_employers = list(set(employers))
                overview_entry["num_unique_employers"] = len(unique_employers)

                experience_entry["url"] = url
                experience_entry["name"] = name
                experience_entry["job_title"] = job_titles
                experience_entry["employer"] = employers
                experience_entry["start_date"] = []
                for date in start_dates:
                    if isinstance(date, datetime):
                        experience_entry["start_date"].append(date.strftime("%m/%d/%Y"))
                    else:
                        experience_entry["start_date"].append(date)
                experience_entry["end_date"] = []
                for date in end_dates:
                    if isinstance(date, datetime):
                        if date.date() == PRESENT_DATE.date():
                            experience_entry["end_date"].append("Present")
                        else:
                            experience_entry["end_date"].append(
                                date.strftime("%m/%d/%Y")
                            )
                    else:
                        experience_entry["end_date"].append(date)
                experience_entry["description"] = exp_descriptions

        # --------------------------------- #
        # |      Education section        | #
        # --------------------------------- #

        logging.info("Searching education")

        driver_edu_all_items = []
        # Better to check json than spend the time to do the following expands and check
        if has_education(profile_json[-1]):
            driver_edu_all_items = get_section_items(
                driver,
                "education-section",
                ".pv-education-entity",
                "button.pv-profile-section__see-more-inline",
                "Education",
                overview_entry,
            )
            if driver_edu_all_items is not None:
                schools = []
                degrees = []
                fields_of_study = []
                edu_descriptions = []

                edu_order = 1

                if len(driver_edu_all_items) == 0:
                    overview_entry["scraping_notes"] = (
                        overview_entry["scraping_notes"] + "\nNo education listed"
                    )
                    logging.info("No education listed")
                else:
                    logging.info("Iterating education")

                for element in driver_edu_all_items:
                    driver.execute_script("arguments[0].scrollIntoView();", element)
                    # Get school
                    try:
                        school = element.find_element_by_tag_name("h3").text
                        schools.append(school)
                        # logging.info("School: " + school)
                    except Exception as e:
                        schools.append("")
                        logging.info(e)
                        logging.info(
                            "Error getting school for degree " + str(edu_order)
                        )
                        overview_entry["scraping_notes"] = (
                            overview_entry["scraping_notes"]
                            + "Error school for degree "
                            + str(edu_order)
                            + "\n"
                        )
                    degree_elements = element.find_elements_by_css_selector(
                        ".pv-entity__comma-item"
                    )
                    # Get degree
                    try:
                        degree = degree_elements[0].text
                        degrees.append(degree)
                        # logging.info("Degree: " + degree)
                    except Exception as e:
                        degrees.append("")
                        logging.info(
                            "(No degree listed for degree " + str(edu_order) + ")"
                        )
                    # Get field of study
                    try:
                        field_of_study = degree_elements[1].text
                        fields_of_study.append(field_of_study)
                        # logging.info("Field of study: " + field_of_study)
                    except Exception as e:
                        fields_of_study.append("")
                        logging.info(
                            "(No field of study listed for degree "
                            + str(edu_order)
                            + ")"
                        )
                    # Get description
                    try:
                        edu_description = element.find_element_by_css_selector(
                            ".pv-entity__description"
                        ).text
                        edu_descriptions.append(edu_description)
                        # logging.info("Description: " + edu_description)
                    except Exception as e:
                        edu_descriptions.append("")
                        logging.info(
                            "(No description listed for degree " + str(edu_order) + ")"
                        )

                    # Increment education order
                    edu_order += 1

                education_entry["url"] = url
                education_entry["name"] = name
                education_entry["school"] = schools
                education_entry["degree"] = degrees
                education_entry["field_of_study"] = fields_of_study
                education_entry["description"] = edu_descriptions

        # Write to file(s)
        if not write_to_files(
            overview_entry, experience_entry, education_entry, name, attempted
        ):
            unupdated_entrees = 0
            update_progress_file(scraper_progress)
            return

        # Update progress
        update_progress("Yes", scraper_progress, url)

    update_progress_file(scraper_progress)


def log_in(driver, credentials):
    driver.get("https://www.linkedin.com/login")

    def login_guest_home_page():
        driver.find_element_by_id("username").clear()
        username = driver.find_element_by_id("username")
        driver.find_element_by_id("password").clear()
        password = driver.find_element_by_id("password")
        username.send_keys(credentials[0])
        password.send_keys(credentials[1])
        driver.find_element_by_xpath(
            '//div[@class="login__form_action_container "]/button'
        ).click()
        if driver.title != "Sign In to LinkedIn":
            logging.info("[+] Login successful")
        try:
            driver.find_element_by_id(
                "error-for-username"
            ) or driver.find_element_by_id("error-for-password")
            logging.info(driver.find_element_by_id("error-for-username").text)
            logging.info(driver.find_element_by_id("error-for-password").text)
            login_guest_home_page()
        except:
            pass

    try:
        username = driver.find_element_by_id("login-email")
        password = driver.find_element_by_id("login-password")
        username.send_keys(credentials[0])
        password.send_keys(credentials[1])
        driver.find_element_by_id("login-submit").click()
        if driver.title != "Sign In to LinkedIn":
            logging.info("[+] Login successful")
        try:
            driver.find_element_by_id(
                "error-for-username"
            ) or driver.find_element_by_id("error-for-password")
            logging.info(driver.find_element_by_id("error-for-username").text)
            logging.info(driver.find_element_by_id("error-for-password").text)
            login_guest_home_page()
        except:
            pass
    except:
        login_guest_home_page()


def entry_thread(urls, names, scraper_progress, credentials):

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(ChromeDriverManager().install())

    driver.implicitly_wait(10)

    log_in(driver, credentials)
    # setting session coolies after login for requests lib
    request_cookies_browser = driver.get_cookies()
    s = requests.Session()
    # s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'
    s.max_redirects = 60
    c = [s.cookies.set(c["name"], c["value"]) for c in request_cookies_browser]
    logging.info("--> Setting session cookies from browser..")

    try:
        parse_entries(driver, urls, names, scraper_progress)
        driver.quit()
    except Exception as e:
        logging.info(e, exc_info=True)
        update_progress_file(scraper_progress)
        driver.quit()


# Main
progress_semaphore = threading.Semaphore()
progress_file_semaphore = threading.Semaphore()
output_semaphore = threading.Semaphore()

if __name__ == "__main__":
    # Start code
    main_df = pd.read_excel(input_file_path_string, sheet_name=SHEET_NAME)
    urls = main_df.URL.tolist()
    names = main_df.Name.tolist()
    names_with_no_urls_indices = []

    for i in range(len(urls)):
        url = urls[i]
        if not url:
            names_with_no_urls_indices.append(i)
            continue
        p = urlparse(str(url), scheme="http")
        if p.netloc:
            netloc = p.netloc
            path = p.path
        else:
            netloc = p.path
            path = ""
        if not netloc.startswith("www."):
            netloc = "www." + netloc
        p = p._replace(netloc=netloc, path=path)
        urls[i] = p.geturl()

    for index in names_with_no_urls_indices:
        del urls[index]
        del names[index]

    # If the output files don't already exist, create them
    handle_output_files()

    # We make a dataframe to store progress so that if something bad happens, we don't need to start over
    try:
        scraper_progress = pd.read_csv(output_folder / "scraper_progress.csv")
    except:
        scraper_progress = pd.DataFrame({"Name": names, "URL": urls})
        scraper_progress["Scraped?"] = "No"
        scraper_progress.to_csv(scraper_progress_output_path_string)

    logging.basicConfig(
        level=logging.INFO, format="%(relativeCreated)6d %(threadName)10s %(message)s"
    )

    threads = []
    for credentials in account_credentials:
        logging.info(
            "Creating parser thread with credentials: "
            + credentials[0]
            + " and "
            + credentials[1]
        )
        t = threading.Thread(
            target=entry_thread, args=(urls, names, scraper_progress, credentials)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logging.info("Exiting LinkedIn Scraper")
