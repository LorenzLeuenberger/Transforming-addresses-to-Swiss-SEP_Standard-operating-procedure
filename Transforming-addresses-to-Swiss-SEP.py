# title: "Transforming addresses to Swiss-SEP: Standard operating procedure"
# authors:
#   - Lorenz Leuenberger
#   - Anna Hartung
# date: 23.03.2026

# Introduction

# The Swiss neighbourhood index of socioeconomic position (Swiss-SEP) is an area-based measure of socioeconomic status for Switzerland. The Swiss-SEP was constructed by ([Panczak et al.](https://doi.org/10.1136/jech-2011-200699) in 2012 and re-validated in [2023](https://doi.org/10.57187/smw.2023.40028)). They used data from national censuses and micro-censuses on income, education, occupation and housing conditions to assign a socioeconomic position to 1.54 million overlapping neighborhoods. The Swiss-SEP is a value between 0-100, where a higher value corresponds to a higher socioeconomic position. Each Swiss-SEP value is linked to a geocode in Switzerland.

# The SwissPedHealth team at ISPM Bern created a reference dataset with all Swiss addresses and corresponding Swiss-SEP values, see description of previous script "Preparing reference dataset: Swiss-SEP and Swiss addresses".
# The reference dataset contains both Swiss-SEP values and distance to nearest Swiss-SEP. The Swiss-SEP values are integers in the range from 0 to 100 without a unit. The distance between geocodes of the addresses and the geocodes of the Swiss-SEP values are integers in meters.

# This reference dataset with addresses and corresponding Swiss-SEP variables is provided to the clinical data warehouses (CDWs).

# Purpose

# The purpose of this standard operating procedure is to standardize how the Swiss-SEP variables are added to patients based on patient addresses. This standard operating procedure is intended to be used in the CDWs for the SwissPedHealth project.



# Problem definition

# The reference dataset with official Swiss addresses and corresponding Swiss-SEP variables is shared with the CDWs.
# The goal is to add the Swiss-SEP variables to the patient addresses. Therefore, one needs to identify the official address in the reference dataset that matches the patient address, for every patient address.

# This SOP follows these steps:
#
# - Split patient addresses into street, number, postal code (plz), and city
# - Spell out common abbreviations
# - Remove special characters from patient addresses
# - Find patient addresses in reference dataset
#     - Simple dataset matching
#     - Allow spelling errors during dataset matching
# - Add Swiss-SEP variables to matched addresses
#
# The needed programming code for these steps is provided as R code and as python code.
# The SOP highlights tasks and shows the code for the particular tasks below the task prompt.






# Python code

# Required libraries and datasets

# The following python packages are required:

# install packages via python terminal
# pip install pandas
# pip install numpy
# pip install polyleven

# import packages
import pandas
import numpy
import string
import random
import re
import time
from polyleven import levenshtein
from concurrent.futures import ThreadPoolExecutor

# The following reference dataset of official Swiss addresses and corresponding Swiss-SEP variables is required.
# For the purpose to demonstrate this SOP, the Swiss-SEP variables are random variables.

# Import the reference dataset into a pandas dataframe.
# The csv file is separated by semicolon ";".

pandas.set_option('display.max_columns', None)
addresses_and_swiss_sep = pandas.read_csv('c:/Your-file-path/patient_addresses.csv', sep=";")

addresses_and_swiss_sep = addresses_and_swiss_sep.where(pandas.notnull, None)
addresses_and_swiss_sep = pandas.DataFrame(addresses_and_swiss_sep)
addresses_and_swiss_sep.head(5)

# The reference dataset consists of the following variables:

addresses_and_swiss_sep.info()

# The dataset with your patient addresses is required.
# Import your dataset of patient addresses into a pandas dataframe.
# This depends on the data format of your patient addresses.

patient_addresses = pandas.read_csv('c:/Your-file-path/patient_addresses.csv', sep=";")

patient_addresses = pandas.DataFrame(patient_addresses)



# Split patient addresses into street, number, plz, and city

# The first step is to split the address text into four fields:
# - street
# - number
# - plz (postal code)
# - city

# The plz can be identified as the last 4 digit number in the address text. The city can be split as the text after the plz. The street and the number can be split off as the text before the plz. To split the street and number, the last or second to last space is used.
# Because special characters will be removed in the next step, it is no problem if city names or street names still contain spaces.

# If there is a special pattern (e.g., patient names in front of street separated with a comma or new line) additional variables with a split street text are created.

# Adapt this part accordingly for your addresses.
# Only keep Swiss addresses: Postal code from 1000 to 9999.

# delete "Schweiz", "Suisse", or "Svizzera" when appearing in the address

patient_addresses['patient_address'] = patient_addresses['patient_address'].str.replace(r'\bSchweiz\b', '', case=False)
patient_addresses['patient_address'] = patient_addresses['patient_address'].str.replace(r'\bSuisse\b', '', case=False)
patient_addresses['patient_address'] = patient_addresses['patient_address'].str.replace(r'\bSvizzera\b', '', case=False)

# Extract the plz as the last 4 digit number

patient_addresses['plz'] = patient_addresses['patient_address'].str.extract(r'(\d{4})(?:(?!\d{4}).)*$')
patient_addresses['plz'] = patient_addresses['plz'].astype(float)

# Filter the patient addresses with Swiss postal codes: plz >= 1000 AND <= 9999

patient_addresses = patient_addresses[(patient_addresses['plz'] >= 1000) & (patient_addresses['plz'] <= 9999)]
patient_addresses['plz'] = patient_addresses['plz'].astype(str)

# split off the city as the string after the plz. Split off the street and number as the string before the plz.

patient_addresses[['street_number', 'city']] = patient_addresses.apply(
    lambda row: pandas.Series(
        re.split(re.search(r'(\d{4})(?:(?!\d{4}).)*$', row['patient_address']).group(1), row['patient_address'])),
    axis=1
)

# delete the space if the last character of the street_number is a space

patient_addresses['street_number'] = patient_addresses.apply(
    lambda row: row['street_number'][:-1] if row['street_number'][-1] == ' ' else row['street_number'],
    axis=1
)

# split the street and the number.

# Because some street names contain digits, and some numbers contain characters, we cannot only extract the digits. Split the number at the last space. If there is no space, try to find the digits.

# Because in python: numpy.nan == numpy.nan --> False, but None == None --> True, first work with None


patient_addresses['number'] = patient_addresses['street_number'].apply(
    lambda x:
    x.split()[-1] if ' ' in x else
    pandas.Series(x).str.extract(r'(\d.*)') if any(char.isdigit() for char in str(x)) else
    None)

patient_addresses['number'] = patient_addresses['number'].astype(str)

# splitting at the last space might split some numbers, e.g., "13 a".
# if the number does not contain a digit, split the street_number at the second to last space
# if this new number contains a digit, keep it.
# if this new number contains no digit, but the old number was of length 1, keep the old number. There exist some numbers with a single letter, e.g., "a".
# if none of the above is true, assign NA to the number.

patient_addresses['new_number'] = patient_addresses.apply(
    lambda row: ' '.join(row['street_number'].rsplit(' ', 2)[-2:]) if pandas.notna(row['number']) and all(
        char.isdigit() == False for char in str(row['number'])) else None,
    axis=1
)

patient_addresses['number'] = patient_addresses.apply(
    lambda row: row['number'] if pandas.notna(row['number']) and any(char.isdigit() for char in str(row['number'])) else
    row['new_number'] if pandas.notna(row['new_number']) and any(char.isdigit() for char in str(row['new_number'])) else
    row['number'] if pandas.notna(row['number']) and len(str(row['number'])) == 1 else
    None,
    axis=1

)

patient_addresses = patient_addresses.drop(columns=['new_number'])

# split off the street as the string before the number, if the number is not NA.

patient_addresses['street'] = patient_addresses.apply(
    lambda row: re.search(f'.*(?={row["number"]})', row['street_number']).group() if re.search(f'.*(?={row["number"]})',
                                                                                               row[
                                                                                                   'street_number']) != None else
    row['street_number'],
    axis=1
)

# if the street often contains added names or other text try to find a pattern and split the extra text off.
# in the test dataset we know that there are often patient names in the street. They are separated by a comma ",".

patient_addresses[['street_split_1', 'street_split_2']] = patient_addresses.apply(
    lambda row: pandas.Series([re.search('^[^,]*', row['street']).group(), re.search('(?<=,).*', row['street']).group()])
                 if pandas.notna(row['street']) and ',' in row['street']
                 else pandas.Series([None, None]),
    axis=1
)


# keep the part of the street without the added names or other text (use street_abbreviation_2 if it exists)
patient_addresses['street'] = patient_addresses.apply(
    lambda row: row['street'] if pandas.isna(row['street_split_2']) else row['street_split_2'],
    axis=1
)

# select the needed variables for address matching
patient_addresses = patient_addresses[['patient_ID', 'street', 'number', 'plz', 'city']]

# Set datatypes
patient_addresses['plz'] = patient_addresses['plz'].astype(float)


# Correct abbreviations in the patient addresses

# Although the reference dataset contains short versions of the street names with common abbreviations, not all of the street names are shortened.
# Therefore, new street variables in the patient addresses are created spelling out some of the common abbreviations in German, French, and Italian.
# The abbreviations are only spelled out if they end with a dot, are followed by a space or are at the end of the street name.


# Spell out common abbreviations in German, French, and Italian street names.


# create copies of the street variable
patient_addresses['street_abbreviation_1'] = patient_addresses['street']
patient_addresses['street_abbreviation_2'] = patient_addresses['street']

# Define abbreviation replacements (German)
abbreviations_german = {
    r"av(?:\.|\s|$)": "avenue",
    r"str(?:\.|\s|$)": "strasse",
    r"pl(?:\.|\s|$)": "platz",
    r"prom(?:\.|\s|$)": "promenade",
    r"st(?:\.|\s|$)": "sankt",
    r"w(?:\.|\s|$)": "weg"
}

# Apply replacements to street_abbreviation_1
for abbr, full in abbreviations_german.items():
    patient_addresses['street_abbreviation_1'] = patient_addresses['street_abbreviation_1'].str.replace(
        abbr, full, case=False, regex=True
    )

# Define abbreviation replacements (French/Italian)
abbreviations_french_italian = {
    r"all(?:\.|\s|$)": "allee",
    r"ave(?:\.|\s|$)": "avenue",
    r"blvd(?:\.|\s|$)": "boulevard",
    r"cab(?:\.|\s|$)": "cabane",
    r"ch(?:\.|\s|$)": "chemin",
    r"imp(?:\.|\s|$)": "impasse",
    r"pas(?:\.|\s|$)": "passage",
    r"pl(?:\.|\s|$)": "place",
    r"prom(?:\.|\s|$)": "promenade",
    r"pz(?:\.|\s|$)": "piazza",
    r"rte(?:\.|\s|$)": "route",
    r"st(?:\.|\s|$)": "saint",
    r"v(?:\.|\s|$)": "via"
}

# Apply replacements to street_abbreviation_2
for abbr, full in abbreviations_french_italian.items():
    patient_addresses['street_abbreviation_2'] = patient_addresses['street_abbreviation_2'].str.replace(
        abbr, full, case=False, regex=True
    )

# Leave newly created variables empty (NA) if no abbreviations were spelled out
patient_addresses['street_abbreviation_1'] = numpy.where(
    patient_addresses['street_abbreviation_1'] == patient_addresses['street'],
    None,
    patient_addresses['street_abbreviation_1']
)

patient_addresses['street_abbreviation_2'] = numpy.where(
    patient_addresses['street_abbreviation_2'] == patient_addresses['street'],
    None,
    patient_addresses['street_abbreviation_2']
)




# Remove special characters from the patient addresses

# To facilitate the address matching, special characters are removed from the street and city.
# Remove special characters from the street and city.

# Define a function to replace special characters and set to lower case

def replace_special_characters(text):
    if pandas.notna(text):
        new_text = re.sub(r'ä', 'ae', text)
        new_text = re.sub(r'á', 'a', new_text)
        new_text = re.sub(r'à', 'a', new_text)
        new_text = re.sub(r'â', 'a', new_text)
        new_text = re.sub(r'Ä', 'AE', new_text)
        new_text = re.sub(r'Á', 'A', new_text)
        new_text = re.sub(r'À', 'A', new_text)
        new_text = re.sub(r'Â', 'A', new_text)
        new_text = re.sub(r'ë', 'e', new_text)
        new_text = re.sub(r'é', 'e', new_text)
        new_text = re.sub(r'è', 'e', new_text)
        new_text = re.sub(r'ê', 'e', new_text)
        new_text = re.sub(r'Ë', 'E', new_text)
        new_text = re.sub(r'É', 'E', new_text)
        new_text = re.sub(r'È', 'E', new_text)
        new_text = re.sub(r'Ê', 'E', new_text)
        new_text = re.sub(r'ï', 'i', new_text)
        new_text = re.sub(r'í', 'i', new_text)
        new_text = re.sub(r'ì', 'i', new_text)
        new_text = re.sub(r'î', 'i', new_text)
        new_text = re.sub(r'Ï', 'I', new_text)
        new_text = re.sub(r'Í', 'I', new_text)
        new_text = re.sub(r'Ì', 'I', new_text)
        new_text = re.sub(r'Î', 'I', new_text)
        new_text = re.sub(r'ö', 'oe', new_text)
        new_text = re.sub(r'ó', 'o', new_text)
        new_text = re.sub(r'ò', 'o', new_text)
        new_text = re.sub(r'ô', 'o', new_text)
        new_text = re.sub(r'Ö', 'OE', new_text)
        new_text = re.sub(r'Ó', 'O', new_text)
        new_text = re.sub(r'Ò', 'O', new_text)
        new_text = re.sub(r'Ô', 'O', new_text)
        new_text = re.sub(r'ü', 'ue', new_text)
        new_text = re.sub(r'ú', 'u', new_text)
        new_text = re.sub(r'ù', 'u', new_text)
        new_text = re.sub(r'û', 'u', new_text)
        new_text = re.sub(r'Ü', 'UE', new_text)
        new_text = re.sub(r'Ú', 'U', new_text)
        new_text = re.sub(r'Ù', 'U', new_text)
        new_text = re.sub(r'Û', 'U', new_text)
        new_text = re.sub(r'ç', 'c', new_text)
        new_text = re.sub(r"[.]", '', new_text)
        new_text = re.sub(r",", "", new_text)
        new_text = re.sub(r' ', '', new_text)
        new_text = re.sub(r'-', '', new_text)
        new_text = re.sub(r'_', '', new_text)
        new_text = re.sub(r"'", '', new_text)
        new_text = re.sub(r"[+]", '', new_text)
        new_text = re.sub(r"[/]", '', new_text)
        new_text = re.sub(r"[(]", '', new_text)
        new_text = re.sub(r"[)]", '', new_text)
        new_text = new_text.lower()
        return new_text
    else:
        return text


patient_addresses["street"] = patient_addresses["street"].apply(replace_special_characters)
patient_addresses["street_abbreviation_1"] = patient_addresses["street_abbreviation_1"].apply(replace_special_characters)
patient_addresses["street_abbreviation_2"] = patient_addresses["street_abbreviation_2"].apply(replace_special_characters)

patient_addresses["city"] = patient_addresses["city"].apply(replace_special_characters)


# Because numbers can include special characters (e.g, "13a" or "13.1"), special characters are not removed from the number.
# Instead an integer number is created by deleting the characters and digits after the first non-digit character.

# Create an integer variable for the house number.
# Because in python missing values (None) cannot be set as datatype integer, use the datatype object for now.

# define function to remove characters and digits after the first non-digit character

def remove_after_non_digit(text):
    if pandas.isna(text):
        return None
    else:
        return re.sub(r'\D.*', '', text)


# create integer of the address number. In case the number starts with a non-digit character, the integer is set to None.

patient_addresses['number_int'] = patient_addresses['number'].apply(remove_after_non_digit)
patient_addresses['number_int'] = patient_addresses["number_int"].replace("", None)

# Create the number_int_reference for the reference dataframe the same way as for the patient dataframe, to ensure that NaN values are None-Type.

addresses_and_swiss_sep["number_int_reference"] = addresses_and_swiss_sep["number_reference"].apply(
    remove_after_non_digit)
addresses_and_swiss_sep['number_int_reference'] = addresses_and_swiss_sep["number_int_reference"].replace("", None)



# Find patient addresses in reference dataset

# The address matching will be done in two steps.
# In a first step, the datasets are matched by the matching function of the data.table format. This will find all perfectly matching addresses.
# In the second step, custom functions are used to find the best matching address in the reference dataset. This will allow for some differences between the patient and the reference addresses.

# Simple dataset matching: data.table

# First, patient addresses are found in the reference dataset with the matching function of the data.table format.
# Several rounds are used to match the addresses (e.g., matching based on plz and city, matching only based on plz).
# These rounds are repeated to match the street, street_abbreviation_1, and street_abbreviation_2 variables.

# select needed variables from the reference dataset

addresses_and_swiss_sep_reference = addresses_and_swiss_sep[
    ["address_id_reference", "street_reference", "street_short_reference", "number_reference", "number_int_reference",
     "plz_reference", "city_reference"]]

# start time to benchmark

start_time = time.perf_counter()

####################################################################################################################################################

# match on street = street_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_0 = pandas.merge(patient_addresses, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number", "plz", "city"],
                                           right_on=["street_reference", "number_reference", "plz_reference",
                                                     "city_reference"])

patient_addresses_results_0 = patient_addresses_results_0[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_0.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_0 = patient_addresses_results_0.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_1 = patient_addresses_results_0[patient_addresses_results_0["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_1 = pandas.merge(patient_addresses_1, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number_int", "plz", "city"],
                                           right_on=["street_reference", "number_int_reference", "plz_reference",
                                                     "city_reference"])

patient_addresses_results_1 = patient_addresses_results_1[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_1.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_1 = patient_addresses_results_1.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_2 = patient_addresses_results_1[patient_addresses_results_1["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_2 = pandas.merge(patient_addresses_2, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number", "plz", "city"],
                                           right_on=["street_short_reference", "number_reference", "plz_reference",
                                                     "city_reference"])

patient_addresses_results_2 = patient_addresses_results_2[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_2.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_2 = patient_addresses_results_2.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_3 = patient_addresses_results_2[patient_addresses_results_2["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_3 = pandas.merge(patient_addresses_3, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number_int", "plz", "city"],
                                           right_on=["street_short_reference", "number_int_reference", "plz_reference",
                                                     "city_reference"])

patient_addresses_results_3 = patient_addresses_results_3[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_3.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_3 = patient_addresses_results_3.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_4 = patient_addresses_results_3[patient_addresses_results_3["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number = number_reference, city = city_reference

patient_addresses_results_4 = pandas.merge(patient_addresses_4, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number", "city"],
                                           right_on=["street_reference", "number_reference", "city_reference"])

patient_addresses_results_4 = patient_addresses_results_4[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_4.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_4 = patient_addresses_results_4.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_5 = patient_addresses_results_4[patient_addresses_results_4["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_5 = pandas.merge(patient_addresses_5, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number_int", "city"],
                                           right_on=["street_reference", "number_int_reference", "city_reference"])

patient_addresses_results_5 = patient_addresses_results_5[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_5.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_5 = patient_addresses_results_5.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_6 = patient_addresses_results_5[patient_addresses_results_5["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_short_reference, number = number_reference, city = city_reference

patient_addresses_results_6 = pandas.merge(patient_addresses_6, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number", "city"],
                                           right_on=["street_short_reference", "number_reference", "city_reference"])

patient_addresses_results_6 = patient_addresses_results_6[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_6.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_6 = patient_addresses_results_6.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_7 = patient_addresses_results_6[patient_addresses_results_6["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_7 = pandas.merge(patient_addresses_7, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number_int", "city"],
                                           right_on=["street_short_reference", "number_int_reference",
                                                     "city_reference"])

patient_addresses_results_7 = patient_addresses_results_7[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_7.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_7 = patient_addresses_results_7.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_8 = patient_addresses_results_7[patient_addresses_results_7["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number = number_reference, plz = plz_reference

patient_addresses_results_8 = pandas.merge(patient_addresses_8, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number", "plz"],
                                           right_on=["street_reference", "number_reference", "plz_reference"])

patient_addresses_results_8 = patient_addresses_results_8[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_8.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_8 = patient_addresses_results_8.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_9 = patient_addresses_results_8[patient_addresses_results_8["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_9 = pandas.merge(patient_addresses_9, addresses_and_swiss_sep_reference, how="left",
                                           left_on=["street", "number_int", "plz"],
                                           right_on=["street_reference", "number_int_reference", "plz_reference"])

patient_addresses_results_9 = patient_addresses_results_9[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_9.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_9 = patient_addresses_results_9.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_10 = patient_addresses_results_9[patient_addresses_results_9["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_short_reference, number = number_reference, plz = plz_reference

patient_addresses_results_10 = pandas.merge(patient_addresses_10, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street", "number", "plz"],
                                            right_on=["street_short_reference", "number_reference", "plz_reference"])

patient_addresses_results_10 = patient_addresses_results_10[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_10.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_10 = patient_addresses_results_10.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_11 = patient_addresses_results_10[patient_addresses_results_10["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_11 = pandas.merge(patient_addresses_11, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street", "number_int", "plz"],
                                            right_on=["street_short_reference", "number_int_reference",
                                                      "plz_reference"])

patient_addresses_results_11 = patient_addresses_results_11[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_11.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_11 = patient_addresses_results_11.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_12 = patient_addresses_results_11[patient_addresses_results_11["matched_address_id"].isna()]

####################################################################################################################################################

# match on street = street_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_12 = pandas.merge(patient_addresses_12, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "plz", "city"],
                                            right_on=["street_reference", "number_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_12 = patient_addresses_results_12[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_12.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_12 = patient_addresses_results_12.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_13 = patient_addresses_results_12[patient_addresses_results_12["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_13 = pandas.merge(patient_addresses_13, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "plz", "city"],
                                            right_on=["street_reference", "number_int_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_13 = patient_addresses_results_13[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_13.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_13 = patient_addresses_results_13.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_14 = patient_addresses_results_13[patient_addresses_results_13["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_14 = pandas.merge(patient_addresses_14, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "plz", "city"],
                                            right_on=["street_short_reference", "number_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_14 = patient_addresses_results_14[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_14.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_14 = patient_addresses_results_14.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_15 = patient_addresses_results_14[patient_addresses_results_14["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_15 = pandas.merge(patient_addresses_15, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "plz", "city"],
                                            right_on=["street_short_reference", "number_int_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_15 = patient_addresses_results_15[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_15.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_15 = patient_addresses_results_15.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_16 = patient_addresses_results_15[patient_addresses_results_15["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number = number_reference, city = city_reference

patient_addresses_results_16 = pandas.merge(patient_addresses_16, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "city"],
                                            right_on=["street_reference", "number_reference", "city_reference"])

patient_addresses_results_16 = patient_addresses_results_16[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_16.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_16 = patient_addresses_results_16.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_17 = patient_addresses_results_16[patient_addresses_results_16["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_17 = pandas.merge(patient_addresses_17, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "city"],
                                            right_on=["street_reference", "number_int_reference", "city_reference"])

patient_addresses_results_17 = patient_addresses_results_17[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_17.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_17 = patient_addresses_results_17.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_18 = patient_addresses_results_17[patient_addresses_results_17["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_short_reference, number = number_reference, city = city_reference

patient_addresses_results_18 = pandas.merge(patient_addresses_18, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "city"],
                                            right_on=["street_short_reference", "number_reference", "city_reference"])

patient_addresses_results_18 = patient_addresses_results_18[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_18.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_18 = patient_addresses_results_18.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_19 = patient_addresses_results_18[patient_addresses_results_18["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_19 = pandas.merge(patient_addresses_19, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "city"],
                                            right_on=["street_short_reference", "number_int_reference",
                                                      "city_reference"])

patient_addresses_results_19 = patient_addresses_results_19[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_19.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_19 = patient_addresses_results_19.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_20 = patient_addresses_results_19[patient_addresses_results_19["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number = number_reference, plz = plz_reference

patient_addresses_results_20 = pandas.merge(patient_addresses_20, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "plz"],
                                            right_on=["street_reference", "number_reference", "plz_reference"])

patient_addresses_results_20 = patient_addresses_results_20[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_20.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_20 = patient_addresses_results_20.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_21 = patient_addresses_results_20[patient_addresses_results_20["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_21 = pandas.merge(patient_addresses_21, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "plz"],
                                            right_on=["street_reference", "number_int_reference", "plz_reference"])

patient_addresses_results_21 = patient_addresses_results_21[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_21.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_21 = patient_addresses_results_21.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_22 = patient_addresses_results_21[patient_addresses_results_21["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_short_reference, number = number_reference, plz = plz_reference

patient_addresses_results_22 = pandas.merge(patient_addresses_22, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number", "plz"],
                                            right_on=["street_short_reference", "number_reference", "plz_reference"])

patient_addresses_results_22 = patient_addresses_results_22[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_22.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_22 = patient_addresses_results_22.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_23 = patient_addresses_results_22[patient_addresses_results_22["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_23 = pandas.merge(patient_addresses_23, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_1", "number_int", "plz"],
                                            right_on=["street_short_reference", "number_int_reference",
                                                      "plz_reference"])

patient_addresses_results_23 = patient_addresses_results_23[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_23.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_23 = patient_addresses_results_23.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_24 = patient_addresses_results_23[patient_addresses_results_23["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_24 = pandas.merge(patient_addresses_24, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "plz", "city"],
                                            right_on=["street_reference", "number_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_24 = patient_addresses_results_24[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_24.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_24 = patient_addresses_results_24.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_25 = patient_addresses_results_24[patient_addresses_results_24["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_25 = pandas.merge(patient_addresses_25, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "plz", "city"],
                                            right_on=["street_reference", "number_int_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_25 = patient_addresses_results_25[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_25.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_25 = patient_addresses_results_25.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_26 = patient_addresses_results_25[patient_addresses_results_25["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

patient_addresses_results_26 = pandas.merge(patient_addresses_26, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "plz", "city"],
                                            right_on=["street_short_reference", "number_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_26 = patient_addresses_results_26[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_26.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_26 = patient_addresses_results_26.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_27 = patient_addresses_results_26[patient_addresses_results_26["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

patient_addresses_results_27 = pandas.merge(patient_addresses_27, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "plz", "city"],
                                            right_on=["street_short_reference", "number_int_reference", "plz_reference",
                                                      "city_reference"])

patient_addresses_results_27 = patient_addresses_results_27[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_27.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_27 = patient_addresses_results_27.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_28 = patient_addresses_results_27[patient_addresses_results_27["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number = number_reference, city = city_reference

patient_addresses_results_28 = pandas.merge(patient_addresses_28, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "city"],
                                            right_on=["street_reference", "number_reference", "city_reference"])

patient_addresses_results_28 = patient_addresses_results_28[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_28.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_28 = patient_addresses_results_28.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_29 = patient_addresses_results_28[patient_addresses_results_28["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_29 = pandas.merge(patient_addresses_29, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "city"],
                                            right_on=["street_reference", "number_int_reference", "city_reference"])

patient_addresses_results_29 = patient_addresses_results_29[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_29.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_29 = patient_addresses_results_29.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_30 = patient_addresses_results_29[patient_addresses_results_29["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_short_reference, number = number_reference, city = city_reference

patient_addresses_results_30 = pandas.merge(patient_addresses_30, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "city"],
                                            right_on=["street_short_reference", "number_reference", "city_reference"])

patient_addresses_results_30 = patient_addresses_results_30[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_30.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_30 = patient_addresses_results_30.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_31 = patient_addresses_results_30[patient_addresses_results_30["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, city = city_reference

patient_addresses_results_31 = pandas.merge(patient_addresses_31, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "city"],
                                            right_on=["street_short_reference", "number_int_reference",
                                                      "city_reference"])

patient_addresses_results_31 = patient_addresses_results_31[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_31.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_31 = patient_addresses_results_31.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_32 = patient_addresses_results_31[patient_addresses_results_31["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number = number_reference, plz = plz_reference

patient_addresses_results_32 = pandas.merge(patient_addresses_32, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "plz"],
                                            right_on=["street_reference", "number_reference", "plz_reference"])

patient_addresses_results_32 = patient_addresses_results_32[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_32.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_32 = patient_addresses_results_32.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_33 = patient_addresses_results_32[patient_addresses_results_32["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_33 = pandas.merge(patient_addresses_33, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "plz"],
                                            right_on=["street_reference", "number_int_reference", "plz_reference"])

patient_addresses_results_33 = patient_addresses_results_33[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_33.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_33 = patient_addresses_results_33.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_34 = patient_addresses_results_33[patient_addresses_results_33["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_short_reference, number = number_reference, plz = plz_reference

patient_addresses_results_34 = pandas.merge(patient_addresses_34, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number", "plz"],
                                            right_on=["street_short_reference", "number_reference", "plz_reference"])

patient_addresses_results_34 = patient_addresses_results_34[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_34.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_34 = patient_addresses_results_34.dropna(subset=["matched_address_id"])

# select non matched addresses

patient_addresses_35 = patient_addresses_results_34[patient_addresses_results_34["matched_address_id"].isna()]

####################################################################################################################################################

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference

patient_addresses_results_35 = pandas.merge(patient_addresses_35, addresses_and_swiss_sep_reference, how="left",
                                            left_on=["street_abbreviation_2", "number_int", "plz"],
                                            right_on=["street_short_reference", "number_int_reference",
                                                      "plz_reference"])

patient_addresses_results_35 = patient_addresses_results_35[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city",
     "address_id_reference"]]

patient_addresses_results_35.rename(columns={'address_id_reference': 'matched_address_id'}, inplace=True)

#  select matched addresses

patient_addresses_matched_35 = patient_addresses_results_35.dropna(subset=["matched_address_id"])

####################################################################################################################################################

# create one dataframe of all the matched addresses

dfs = [patient_addresses_matched_0,
       patient_addresses_matched_1,
       patient_addresses_matched_2,
       patient_addresses_matched_3,
       patient_addresses_matched_4,
       patient_addresses_matched_5,
       patient_addresses_matched_6,
       patient_addresses_matched_7,
       patient_addresses_matched_8,
       patient_addresses_matched_9,
       patient_addresses_matched_10,
       patient_addresses_matched_11,
       patient_addresses_matched_12,
       patient_addresses_matched_13,
       patient_addresses_matched_14,
       patient_addresses_matched_15,
       patient_addresses_matched_16,
       patient_addresses_matched_17,
       patient_addresses_matched_18,
       patient_addresses_matched_19,
       patient_addresses_matched_20,
       patient_addresses_matched_21,
       patient_addresses_matched_22,
       patient_addresses_matched_23,
       patient_addresses_matched_24,
       patient_addresses_matched_25,
       patient_addresses_matched_26,
       patient_addresses_matched_27,
       patient_addresses_matched_28,
       patient_addresses_matched_29,
       patient_addresses_matched_30,
       patient_addresses_matched_31,
       patient_addresses_matched_32,
       patient_addresses_matched_33,
       patient_addresses_matched_34,
       patient_addresses_matched_35]

patient_addresses_matched = pandas.concat([df for df in dfs if not df.empty], ignore_index=True)

# Keep only one matched reference address per patient address.
# You might encounter that multiple reference addresses are matched to a patient address if a patient reported a house number (e.g. "10"), but the reference dataset includes only house numbers with characters (e.g. "10a", "10b").
# Keep the last matched address, to be consistent with the R code of this SOP.

patient_addresses_matched = patient_addresses_matched.drop_duplicates(subset=["patient_ID", "street", "number", "plz", "city"], keep="last")

patient_addresses_matched = patient_addresses_matched[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city", "matched_address_id"]]

patient_addresses_matched["score"] = 0

# select the non matched addresses

patient_addresses_non_matched = patient_addresses_results_35[patient_addresses_results_35["matched_address_id"].isna()]

# remove dataframes from environment
del patient_addresses_matched_0, patient_addresses_matched_1, patient_addresses_matched_2, patient_addresses_matched_3, patient_addresses_matched_4, patient_addresses_matched_5, patient_addresses_matched_6, patient_addresses_matched_7, patient_addresses_matched_8, patient_addresses_matched_9, patient_addresses_matched_10, patient_addresses_matched_11, patient_addresses_matched_12, patient_addresses_matched_13, patient_addresses_matched_14, patient_addresses_matched_15, patient_addresses_matched_16, patient_addresses_matched_17, patient_addresses_matched_18, patient_addresses_matched_19, patient_addresses_matched_20, patient_addresses_matched_21, patient_addresses_matched_22, patient_addresses_matched_23, patient_addresses_matched_24, patient_addresses_matched_25, patient_addresses_matched_26, patient_addresses_matched_27, patient_addresses_matched_28, patient_addresses_matched_29, patient_addresses_matched_30, patient_addresses_matched_31, patient_addresses_matched_32, patient_addresses_matched_33, patient_addresses_matched_34, patient_addresses_matched_35

del patient_addresses_results_0, patient_addresses_results_1, patient_addresses_results_2, patient_addresses_results_3, patient_addresses_results_4, patient_addresses_results_5, patient_addresses_results_6, patient_addresses_results_7, patient_addresses_results_8, patient_addresses_results_9, patient_addresses_results_10, patient_addresses_results_11, patient_addresses_results_12, patient_addresses_results_13, patient_addresses_results_14, patient_addresses_results_15, patient_addresses_results_16, patient_addresses_results_17, patient_addresses_results_18, patient_addresses_results_19, patient_addresses_results_20, patient_addresses_results_21, patient_addresses_results_22, patient_addresses_results_23, patient_addresses_results_24, patient_addresses_results_25, patient_addresses_results_26, patient_addresses_results_27, patient_addresses_results_28, patient_addresses_results_29, patient_addresses_results_30, patient_addresses_results_31, patient_addresses_results_32, patient_addresses_results_33, patient_addresses_results_34, patient_addresses_results_35

del patient_addresses_1, patient_addresses_2, patient_addresses_3, patient_addresses_4, patient_addresses_5, patient_addresses_6, patient_addresses_7, patient_addresses_8, patient_addresses_9, patient_addresses_10, patient_addresses_11, patient_addresses_12, patient_addresses_13, patient_addresses_14, patient_addresses_15, patient_addresses_16, patient_addresses_17, patient_addresses_18, patient_addresses_19, patient_addresses_20, patient_addresses_21, patient_addresses_22, patient_addresses_23, patient_addresses_24, patient_addresses_25, patient_addresses_26, patient_addresses_27, patient_addresses_28, patient_addresses_29, patient_addresses_30, patient_addresses_31, patient_addresses_32, patient_addresses_33, patient_addresses_34, patient_addresses_35

# report the results

end_time = time.perf_counter()

print("Time for execution:", end_time - start_time, "seconds")

print("number of matched addresses: ", len(patient_addresses_matched))

print("number of non-matched addresses: ", len(patient_addresses_non_matched))



# Allow spelling errors during dataset matching

# Second, custom functions are defined to find the best matching addresses in the reference dataset.
# Briefly, a similarity score between the patient address and a reference address is calculated based on the similarity of the street, number, plz, and city. This similarity score is calculated for all reference addresses that match the plz or city of the patient address. This process is repeated for every patient address.

# Create new variables in the dataset of non-matched patient addresses and the reference dataset.

# Adapt the remaining patient addresses and the reference dataset for the custom functions.


patient_addresses_36 = patient_addresses_non_matched

patient_addresses_36 = patient_addresses_36[
    ["patient_ID", "street", "street_abbreviation_1", "street_abbreviation_2", "number", "number_int", "plz", "city"]]

patient_addresses_36["street_len"] = patient_addresses_36["street"].str.len()

patient_addresses_36["street_abbreviation_1_len"] = patient_addresses_36["street_abbreviation_1"].str.len()

patient_addresses_36["street_abbreviation_2_len"] = patient_addresses_36["street_abbreviation_2"].str.len()

patient_addresses_36["city_len"] = patient_addresses_36["city"].str.len()

# select the needed variables from the original reference dataset

addresses_and_swiss_sep_reference = addresses_and_swiss_sep[
    ["address_id_reference", "street_reference", "street_len_reference", "street_short_reference",
     "street_short_len_reference", "number_reference", "number_int_reference", "plz_reference", "city_reference",
     "city_len_reference"]]

# Now, change the all NA values from None to numpy.nan

patient_addresses_36 = patient_addresses_36.where(pandas.notnull, numpy.nan)

patient_addresses_36["number_int"] = patient_addresses_36["number_int"].astype(float)

addresses_and_swiss_sep_reference = addresses_and_swiss_sep_reference.where(pandas.notnull, numpy.nan)

addresses_and_swiss_sep_reference["number_int_reference"] = addresses_and_swiss_sep_reference[
    "number_int_reference"].astype(float)


# Define the functions to find the best matching addresses in the reference dataset.

# define function to check similarity of two rows with address information

def calculate_similarity(street, street_abbreviation_1, street_abbreviation_2, number, city, street_reference, street_short_reference,
                         number_reference, city_reference, street_len_difference, street_short_len_difference,
                         street_abbreviation_1_len_difference, street_abbreviation_1_short_len_difference,
                         street_abbreviation_2_len_difference, street_abbreviation_2_short_len_difference, number_difference,
                         plz_difference, city_len_difference):
    # Try to minimize the cases when a Levenshtein distance is calculated because this is compute-intensive
    # Street score

    if (street == street_reference) or (pandas.isna(street) and pandas.isna(street_reference)):
        street_score = 0
    elif (street in street_reference) and (street_len_difference == 1):
        street_score = 10
    elif (street in street_reference):
        street_score = 20
    elif (street_reference in street):
        street_score = 20
    elif street_len_difference < 5:
        street_score = 10 * levenshtein(street, street_reference, 4)
    else:
        street_score = 50

    # Street short score

    if street_score > 20:
        if (street == street_short_reference) or (pandas.isna(street) and pandas.isna(street_short_reference)):
            street_short_score = 0
        elif (street in street_short_reference) and (street_short_len_difference == 1):
            street_short_score = 10
        elif (street in street_short_reference):
            street_short_score = 20
        elif (street_short_reference in street):
            street_short_score = 20
        elif street_short_len_difference < 5:
            street_short_score = 10 * levenshtein(street, street_short_reference, 4)
        else:
            street_short_score = 50
    else:
        street_short_score = 50

    street_score = min(street_score, street_short_score)

    # Street split 1
    if (street_score < 50 or pandas.isna(street_abbreviation_1)):
        street_abbreviation_1_score = 50
    else:
        if (street_abbreviation_1 == street_reference) or (pandas.isna(street_abbreviation_1) and pandas.isna(street_reference)):
            street_abbreviation_1_score = 0
        elif (street_abbreviation_1 in street_reference) and (street_abbreviation_1_len_difference == 1):
            street_abbreviation_1_score = 10
        elif (street_abbreviation_1 in street_reference):
            street_abbreviation_1_score = 20
        elif (street_reference in street_abbreviation_1):
            street_abbreviation_1_score = 20
        elif (street_abbreviation_1_len_difference < 5):
            street_abbreviation_1_score = 10 * levenshtein(street_abbreviation_1, street_reference, 4)
        else:
            street_abbreviation_1_score = 50

    if (street_score < 50 or street_abbreviation_1_score < 20 or pandas.isna(street_abbreviation_1)):
        street_abbreviation_1_short_score = 50
    else:
        if (street_abbreviation_1 == street_short_reference) or (
                pandas.isna(street_abbreviation_1) and pandas.isna(street_short_reference)):
            street_abbreviation_1_short_score = 0
        elif (street_abbreviation_1 in street_short_reference) and (street_abbreviation_1_short_len_difference == 1):
            street_abbreviation_1_short_score = 10
        elif (street_abbreviation_1 in street_short_reference):
            street_abbreviation_1_short_score = 20
        elif (street_short_reference in street_abbreviation_1):
            street_abbreviation_1_short_score = 20
        elif (street_abbreviation_1_short_len_difference < 5):
            street_abbreviation_1_short_score = 10 * levenshtein(street_abbreviation_1, street_short_reference, 4)
        else:
            street_abbreviation_1_short_score = 50

    street_score < - min(street_score, street_abbreviation_1_score, street_abbreviation_1_short_score)

    # Street split 2
    if (street_score < 50 or pandas.isna(street_abbreviation_2)):
        street_abbreviation_2_score = 50
    else:
        if (street_abbreviation_2 == street_reference) or (pandas.isna(street_abbreviation_2) and pandas.isna(street_reference)):
            street_abbreviation_2_score = 0
        elif (street_abbreviation_2 in street_reference) and (street_abbreviation_2_len_difference == 1):
            street_abbreviation_2_score = 10
        elif (street_abbreviation_2 in street_reference):
            street_abbreviation_2_score = 20
        elif (street_reference in street_abbreviation_2):
            street_abbreviation_2_score = 20
        elif (street_abbreviation_2_len_difference < 5):
            street_abbreviation_2_score = 10 * levenshtein(street_abbreviation_2, street_reference, 4)
        else:
            street_abbreviation_2_score = 50

    if (street_score < 50 or street_abbreviation_2_score < 20 or pandas.isna(street_abbreviation_2)):
        street_abbreviation_2_short_score = 50
    else:
        if (street_abbreviation_2 == street_short_reference) or (
                pandas.isna(street_abbreviation_2) and pandas.isna(street_short_reference)):
            street_abbreviation_2_short_score = 0
        elif (street_abbreviation_2 in street_short_reference) and (street_abbreviation_2_short_len_difference == 1):
            street_abbreviation_2_short_score = 10
        elif (street_abbreviation_2 in street_short_reference):
            street_abbreviation_2_short_score = 20
        elif (street_short_reference in street_abbreviation_2):
            street_abbreviation_2_short_score = 20
        elif (street_abbreviation_2_short_len_difference < 5):
            street_abbreviation_2_short_score = 10 * levenshtein(street_abbreviation_2, street_short_reference, 4)
        else:
            street_abbreviation_2_short_score = 50

    street_score < - min(street_score, street_abbreviation_2_score, street_abbreviation_2_short_score)

    # Number score
    if (pandas.isna(number) and pandas.isna(number_reference)):
        number_score = 0
    elif (pandas.isna(number) or pandas.isna(number_reference)):
        number_score = 20
    elif number == number_reference:
        number_score = 0
    elif number_difference == 0:
        number_score = 0.5
    elif pandas.isna(number_difference):
        number_score = 10 * levenshtein(str(number), str(number_reference))
        if number_score > 20:
            number_score = 20
    elif number_difference <= 20:
        number_score = number_difference
    else:
        number_score = 20

    # PLZ score

    if plz_difference <= 10:
        plz_score = plz_difference
    elif plz_difference <= 100:
        plz_score = 20
    else:
        plz_score = 30

    # City score

    if (city == city_reference) or (pandas.isna(city) and pandas.isna(city_reference)):
        city_score = 0
    elif (city in city_reference) and (city_len_difference == 1):
        city_score = 10
    elif (city in city_reference):
        city_score = 20
    elif city_len_difference < 3:
        city_score = 10 * levenshtein(city, city_reference, 2)
    else:
        city_score = 30

    # If the plz or city is correct, the other information is not that important anymore

    if plz_score == 0:
        city_score = city_score * 0.5

    if city_score == 0:
        plz_score = plz_score * 0.5

    overall_score = street_score + number_score + plz_score + city_score

    return overall_score


# Define function to run on one row. In this function we search addresses when the plz or the city matches.

def address_match_row(row1, addresses_reference=addresses_and_swiss_sep_reference,
                      calculate_similarity=calculate_similarity):
    # Set best score and best_address_id. The best_score value serves as a cutoff. This is set to 50, because if a street does not match at all, we do not want this to be a match.

    best_score = 50
    best_address_id = numpy.nan

    street = row1["street"]
    street_len = row1["street_len"]
    street_abbreviation_1 = row1["street_abbreviation_1"]
    street_abbreviation_1_len = row1["street_abbreviation_1_len"]
    street_abbreviation_2 = row1["street_abbreviation_2"]
    street_abbreviation_2_len = row1["street_abbreviation_2_len"]
    number_int = row1["number_int"]
    number = row1["number"]
    plz = row1["plz"]
    city = row1["city"]
    city_len = row1["city_len"]

    # filter the reference DataFrame based on the plz and the city name.
    addresses_reference_filtered = addresses_reference[
        (addresses_reference['plz_reference'] == plz) | (addresses_reference['city_reference'] == city)]
    row1['time_intermediate_1'] = time.perf_counter()

    # Create additional variables in the reference DataFrame for calculate_similarity function
    addresses_reference_compare = addresses_reference_filtered
    addresses_reference_compare = addresses_reference_compare.assign(
        street_len_difference=lambda x: abs(x.street_len_reference - street_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        street_short_len_difference=lambda x: abs(x.street_short_len_reference - street_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        street_abbreviation_1_len_difference=lambda x: abs(x.street_len_reference - street_abbreviation_1_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        street_abbreviation_1_short_len_difference=lambda x: abs(x.street_short_len_reference - street_abbreviation_1_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        street_abbreviation_2_len_difference=lambda x: abs(x.street_len_reference - street_abbreviation_2_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        street_abbreviation_2_short_len_difference=lambda x: abs(x.street_short_len_reference - street_abbreviation_2_len))
    addresses_reference_compare = addresses_reference_compare.assign(
        number_difference=lambda x: abs(x.number_int_reference - number_int))
    addresses_reference_compare = addresses_reference_compare.assign(
        plz_difference=lambda x: abs(x.plz_reference - plz))
    addresses_reference_compare = addresses_reference_compare.assign(
        city_len_difference=lambda x: abs(x.city_len_reference - city_len))

    # Iterate over the rows in the reference DataFrame and calculate similarity. Don't iterate over the rows if the dataframe is empty.
    if (len(addresses_reference_compare) == 0):
        best_score = numpy.nan
        best_address_id = numpy.nan
    else:
        for index2, row2 in addresses_reference_compare.iterrows():
            street_reference = row2["street_reference"]
            street_short_reference = row2["street_short_reference"]
            number_reference = row2["number_reference"]
            city_reference = row2["city_reference"]
            street_len_difference = row2["street_len_difference"]
            street_short_len_difference = row2["street_short_len_difference"]
            street_abbreviation_1_len_difference = row2["street_abbreviation_1_len_difference"]
            street_abbreviation_1_short_len_difference = row2["street_abbreviation_1_short_len_difference"]
            street_abbreviation_2_len_difference = row2["street_abbreviation_2_len_difference"]
            street_abbreviation_2_short_len_difference = row2["street_abbreviation_2_short_len_difference"]
            number_difference = row2["number_difference"]
            plz_difference = row2["plz_difference"]
            city_len_difference = row2["city_len_difference"]

            score = calculate_similarity(street, street_abbreviation_1, street_abbreviation_2, number, city, street_reference,
                                         street_short_reference, number_reference, city_reference,
                                         street_len_difference, street_short_len_difference,
                                         street_abbreviation_1_len_difference, street_abbreviation_1_short_len_difference,
                                         street_abbreviation_2_len_difference, street_abbreviation_2_short_len_difference,
                                         number_difference, plz_difference, city_len_difference)

            if score < best_score:
                best_score = score
                best_address_id = row2["address_id_reference"]

            # Break if it is clear that the best_score can't get lower than the current best_score
            if best_score <= 0.5:
                break

    row1["score"] = best_score
    row1["matched_address_id"] = best_address_id

    return row1


# Apply the function address_match_row to find patient addresses in the reference dataset.

start_time = time.perf_counter()


####################################################################################################################################################


# match with the custom functions. Parallel version to run on several cores.

def apply_function_to_row(row):
    return address_match_row(row, addresses_and_swiss_sep_reference)


with ThreadPoolExecutor() as executor:
    # Use submit() to apply the function to each row in parallel
    futures = [executor.submit(apply_function_to_row, row) for _, row in patient_addresses_36.iterrows()]

# Retrieve results from futures

results = [future.result() for future in futures]

# Create a new DataFrame from the results

patient_addresses_results_36 = pandas.DataFrame(results)

# select matched addresses

patient_addresses_matched_36 = patient_addresses_results_36.dropna(subset=["matched_address_id"])
patient_addresses_matched_36 = patient_addresses_matched_36[
    ["patient_ID", "street", "number", "number_int", "plz", "city", "matched_address_id"]]

# select non-matched addresses

patient_addresses_non_matched_36 = patient_addresses_results_36[
    patient_addresses_results_36["matched_address_id"].isna()]

# create dataframe and select results

end_time = time.perf_counter()

print("Time for execution:", end_time - start_time, "seconds")
print("number of matched addresses: ", len(patient_addresses_matched_36))
print("number of non-matched addresses: ", len(patient_addresses_non_matched_36))





# Add Swiss-SEP variables

# Now the matched address details and the Swiss-SEP variables can be added.

# Create a dataset with all matched addresses.

patient_addresses_matched = pandas.concat([patient_addresses_matched, patient_addresses_matched_36])

# Add the details of the reference addresses and the Swiss-SEP variables.

# join datasets on address id

patient_addresses_matched = pandas.merge(patient_addresses_matched, addresses_and_swiss_sep, how="left",
                                         left_on="matched_address_id", right_on="address_id_reference")

# select variables: you can also choose geocode_E_address and geocode_N_address, if you want to save geocodes of your patients addresses.

patient_addresses_matched = patient_addresses_matched[
    ["patient_ID", "street", "number", "plz", "city", "street_original", "number_reference", "plz_reference",
     "city_original", "swiss_sep_1", "swiss_sep_2", "swiss_sep_3", "distance_address_swiss_sep"]]

# This dataset of patient addresses with the added Swiss-SEP variables can now be used to create the Swiss-SEP concept for the patients. The variables for the Swiss Socio Economic Position concept are:

# - hasValue: integer without unit
# - hasDistance: integer in meters
# - hasVersion: string ("Version 1.0", "Version 2.0", "Version 3.0)

# For patients where the Swiss-SEP variables could not be matched, the concept can be left empty.




