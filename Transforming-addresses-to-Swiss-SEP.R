# title: "Transforming addresses to Swiss-SEP: Standard operating procedure"
# authors:
#   - Lorenz Leuenberger
#   - Anna Hartung
# date: 13.03.2026
 
# Introduction ------------------------------------------------------------------------------------------------------------------------------

# The Swiss neighbourhood index of socioeconomic position (Swiss-SEP) is an area-based measure of socioeconomic status for Switzerland. The Swiss-SEP was constructed by ([Panczak et al.](https://doi.org/10.1136/jech-2011-200699) in 2012 and re-validated in [2023](https://doi.org/10.57187/smw.2023.40028)). They used data from national censuses and micro-censuses on income, education, occupation and housing conditions to assign a socioeconomic position to 1.54 million overlapping neighborhoods. The Swiss-SEP is a value between 0-100, where a higher value corresponds to a higher socioeconomic position. Each Swiss-SEP value is linked to a geocode in Switzerland.
# The SwissPedHealth team at ISPM Bern created a reference dataset with all Swiss addresses and corresponding Swiss-SEP values, see description of previous script "Preparing reference dataset: Swiss-SEP and Swiss addresses".
# The reference dataset contains both Swiss-SEP values and distance to nearest Swiss-SEP. The Swiss-SEP values are integers in the range from 0 to 100 without a unit. The distance between geocodes of the addresses and the geocodes of the Swiss-SEP values are integers in meters.
# This reference dataset with addresses and corresponding Swiss-SEP variables is provided to the clinical data warehouses (CDWs).

## Purpose ------------------------------------------------------------------------------------------------------------------------------

# The purpose of this standard operating procedure is to standardize how the Swiss-SEP variables are added to patients based on patient addresses. This standard operating procedure is intended to be used in the CDWs for the SwissPedHealth project.

## Problem definition ------------------------------------------------------------------------------------------------------------------------------
# The reference dataset with official Swiss addresses and corresponding Swiss-SEP variables is shared with the CDWs.
# The goal is to add the Swiss-SEP variables to the patient addresses. Therefore, one needs to identify the official address in the reference dataset that matches the patient address, for every patient address.

# This SOP follows these steps:

# - Split patient addresses into street, number, postal code (plz), and city
# - Spell out common abbreviations
# - Remove special characters from patient addresses
# - Find patient addresses in reference dataset
#     - Simple dataset matching
#     - Allow spelling errors during dataset matching
# - Add Swiss-SEP variables to matched addresses
  
# The needed programming code for these steps is provided as R code and as python code.
# The SOP highlights tasks and shows the code for the particular tasks below the task prompt.

# R code ------------------------------------------------------------------------------------------------------------------------------

## Required libraries and datasets ------------------------------------------------------------------------------------------------------------------------------

# The following R libraries are required:

# install the required R packages

install.packages("tidyverse")
install.packages("data.table")
install.packages("stringdist")
install.packages("gtsummary")
install.packages("future.apply")
install.packages("stringr")
install.packages("stringi")


# load the required packages

library(tidyverse)
library(data.table)
library(stringdist)
library(gtsummary)
library(future.apply)
library(stringr)
library(stringi)



# The following reference dataset of official Swiss addresses and corresponding Swiss-SEP variables is required.

# For the purpose to demonstrate this SOP, the Swiss-SEP variables are random variables.

# Import the reference dataset into an R dataframe.
# The csv file is separated by semicolon ";".


addresses_and_swiss_sep <- read_delim("Y:/Your-file-path/addresses_and_swiss_sep.csv", delim = ";")

 
# The reference dataset consists of the following variables:

str(addresses_and_swiss_sep)

 
# The dataset with your patient addresses is required.
# Import your dataset of patient addresses into a R dataframe.
# This depends on the data format of your patient addresses.

patient_addresses <- read_delim("c:/Your-file-path/patient_addresses.csv", delim = ";")


## Split patient addresses into street, number, plz, and city --------------------------------------------------------------------------------------

# The first step is to split the address text into four fields:
# - street
# - number
# - plz (postal code)
# - city

# In the test addresses, there exists one variable with the whole address text.
# The plz can be identified as the last 4 digit number in the address text. The city can be split as the text after the plz. The street and the number can be split off as the text before the plz. To split the street and number, the last or second to last space is used.
# Because special characters will be removed in the next step, it is no problem if city names or street names still contain spaces.
# 
# If there is a special pattern (e.g., patient names in front of street separated with a comma or new line) additional variables with a split street text are created.

# Adapt this part accordingly for your addresses.
# Only keep Swiss addresses: Postal code from 1000 to 9999.

# delete "Schweiz", "Suisse", or "Svizzera" when appearing in the address

patient_addresses <- patient_addresses %>%
  mutate(patient_address = gsub("\\bSchweiz\\b", "", patient_address, ignore.case = TRUE),
         patient_address = gsub("\\bSuisse\\b", "", patient_address, ignore.case = TRUE),
         patient_address = gsub("\\bSvizzera\\b", "", patient_address, ignore.case = TRUE))

# identify the last 4 digit number as the plz
patient_addresses <- patient_addresses %>%
  rowwise() %>%
  mutate(plz = ifelse(length(regmatches(patient_address, gregexpr("[0-9]{4}", patient_address))[[1]]) > 0,
                      tail(regmatches(patient_address, gregexpr("[0-9]{4}", patient_address))[[1]], n = 1),
                      NA))

# Filter the patient addresses with Swiss postal codes: plz >= 1000 AND <= 9999
patient_addresses <- patient_addresses %>% 
  filter(plz >= 1000 & plz <= 9999)


# split off the city as the string after the plz. Split off the street and number as the string before the plz.
patient_addresses <- patient_addresses %>%
  rowwise() %>%
  mutate(city = case_when(is.na(plz)==FALSE ~ str_extract(patient_address, paste0("(?<=\\b", str_escape(plz), "\\b).*")),
                          TRUE ~ NA),
         street_number = case_when(is.na(plz)==FALSE ~ str_extract(patient_address, paste0(".*(?=", str_escape(plz), ")")),
                          TRUE ~ NA))

# delete the space if the last character of the street_number is a space
patient_addresses <- patient_addresses %>%
  rowwise() %>% 
  mutate(street_number = case_when(str_sub(street_number, -1) == " " ~ str_sub(street_number, 1, -2)))

# split the street and the number.
#Because some street names contain digits, and some numbers contain characters, we cannot only extract the digits. Split the number at the last space. If there is no space, try to find the digits.

patient_addresses <- patient_addresses %>%
  rowwise() %>% 
  mutate(number = ifelse(str_detect(street_number, " "), tail(str_split(street_number, " ")[[1]], n = 1), str_extract(street_number, "\\d.*")))

# splitting at the last space might split some numbers, e.g., "13 a".
# if the number does not contain a digit, split the street_number at the second to last space
# if this new number contains a digit, keep it.
# if this new number contains no digit, but the old number was of length 1, keep the old number. There exist some numbers with a single letter, e.g., "a".
# if none of the above is true, assign NA to the number.

patient_addresses <- patient_addresses %>% 
  rowwise() %>% 
  mutate(new_number = case_when(str_detect(number, "\\d")==FALSE ~ tail(str_split(street_number, "\\s")[[1]], n = 2) %>% paste(collapse = " "),
                                TRUE ~ NA)) %>% 
  mutate(number = case_when(str_detect(number, "\\d")==TRUE ~ number,
                            str_detect(new_number, "\\d")==TRUE ~ new_number,
                            str_detect(new_number, "\\d")==FALSE & nchar(number)==1 ~ number,
                            TRUE ~ NA)) %>% 
  select(-new_number)

# split off the street as the string before the number, if the number is not NA.

patient_addresses <- patient_addresses %>%
  rowwise() %>% 
  mutate(street = case_when(is.na(number)==FALSE ~ str_extract(street_number, paste0(".*(?=", str_escape(number), ")")),
                            TRUE ~ street_number))

# if the street often contains added names or other text try to find a pattern and split the extra text off.
# in the test dataset we know that there are often patient names in the street. They are separated by a comma ",".

patient_addresses <- patient_addresses |>
  rowwise() |> 
  mutate(street_split_1 = case_when(str_detect(street, ",")==TRUE ~ str_extract(street, "^[^,]*"),
                                    TRUE ~ NA),
         street_split_2 = case_when(str_detect(street, ",")==TRUE ~str_extract(street, "(?<=,).*"),
                                    TRUE ~ NA))

# keep the part of the street without the added names or other text.

patient_addresses <- patient_addresses |> 
  mutate(street = case_when(is.na(street_split_2) ~ street,
                            TRUE ~ street_split_2)) |> 
  select(-c(street_split_1, street_split_2))



# select needed variables for address matching
patient_addresses <- patient_addresses |>
  ungroup() |> 
  select(patient_ID, street, number, plz, city)

# set datatype of the plz to integer

patient_addresses <- patient_addresses |>
  mutate(plz = as.integer(plz))

## Correct abbreviations in the patient addresses -------------------------------------------------------------------------------

# Although the reference dataset contains short versions of the street names with common abbreviations, not all of the street names are shortened.
# Therefore, new street variables in the patient addresses are created spelling out some of the common abbreviations in German, French, and Italian.
# The abbreviations are only spelled out if they end with a dot, are followed by a space or are at the end of the street name.



# Spell out common abbreviations in German, French, and Italian street names.


# create copies of the street variable
patient_addresses <- patient_addresses |>
  mutate(street_abbreviation_1 = street,
         street_abbreviation_2 = street)

# Define abbreviation replacements (German)
abbreviations_german <- c(
  "av(?:\\.|\\s|$)" = "avenue",
  "str(?:\\.|\\s|$)" = "strasse",
  "pl(?:\\.|\\s|$)" = "platz",
  "prom(?:\\.|\\s|$)" = "promenade",
  "st(?:\\.|\\s|$)" = "sankt",
  "w(?:\\.|\\s|$)" = "weg")

# Apply replacements to street_abreviation_1
for (abbr in names(abbreviations_german)) {
  patient_addresses$street_abbreviation_1 <- gsub(abbr, abbreviations_german[abbr], patient_addresses$street_abbreviation_1, ignore.case = TRUE)
}

# Define abbreviation replacements (French & Italian)
abbreviations_french_italian <- c(
  "all(?:\\.|\\s|$)" = "allee",
  "ave(?:\\.|\\s|$)" = "avenue",
  "blvd(?:\\.|\\s|$)" = "boulevard",
  "cab(?:\\.|\\s|$)" = "cabane",
  "ch(?:\\.|\\s|$)" = "chemin",
  "imp(?:\\.|\\s|$)" = "impasse",
  "pas(?:\\.|\\s|$)" = "passage",
  "pl(?:\\.|\\s|$)" = "place",
  "prom(?:\\.|\\s|$)" = "promenade",
  "pz(?:\\.|\\s|$)" = "piazza",
  "rte(?:\\.|\\s|$)" = "route",
  "st(?:\\.|\\s|$)" = "saint",
  "v(?:\\.|\\s|$)" = "via"
)

# Apply replacements to street_abreviation_2
for (abbr in names(abbreviations_french_italian)) {
  patient_addresses$street_abbreviation_2 <- gsub(abbr, abbreviations_french_italian[abbr], patient_addresses$street_abbreviation_2, ignore.case = TRUE)
}


# leave the newly created variables empty if no abbreviations were spelled out.

patient_addresses <- patient_addresses |> 
  mutate(street_abbreviation_1 = case_when(street_abbreviation_1 == street ~ NA,
                                           TRUE ~ street_abbreviation_1),
         street_abbreviation_2 = case_when(street_abbreviation_2 == street ~ NA,
                                           TRUE ~ street_abbreviation_2))





## Remove special characters from the patient addresses ---------------------------------------------------------------------------------------

# To facilitate the address matching, special characters are removed from the street and city.

# Define a function to replace special characters

replace_special_characters <- function(text){
  
  new_text = gsub('ä', 'ae', text) 
  new_text = gsub('á', 'a', new_text) 
  new_text = gsub('à', 'a', new_text) 
  new_text = gsub('â', 'a', new_text) 
  new_text = gsub('Ä', 'AE', new_text) 
  new_text = gsub('Á', 'A', new_text)
  new_text = gsub('À', 'A', new_text)
  new_text = gsub('Â', 'A', new_text)
  new_text = gsub('ë', 'e', new_text)
  new_text = gsub('é', 'e', new_text)
  new_text = gsub('è', 'e', new_text)
  new_text = gsub('ê', 'e', new_text)
  new_text = gsub('Ë', 'E', new_text)
  new_text = gsub('É', 'E', new_text)
  new_text = gsub('È', 'E', new_text)
  new_text = gsub('Ê', 'E', new_text)
  new_text = gsub('ï', 'i', new_text)
  new_text = gsub('í', 'i', new_text)
  new_text = gsub('ì', 'i', new_text)
  new_text = gsub('î', 'i', new_text)
  new_text = gsub('Ï', 'I', new_text)
  new_text = gsub('Í', 'I', new_text)
  new_text = gsub('Ì', 'I', new_text)
  new_text = gsub('Î', 'I', new_text)
  new_text = gsub('ö', 'oe', new_text)
  new_text = gsub('ó', 'o', new_text)
  new_text = gsub('ò', 'o', new_text)
  new_text = gsub('ô', 'o', new_text)
  new_text = gsub('Ö', 'OE', new_text)
  new_text = gsub('Ó', 'O', new_text)
  new_text = gsub('Ò', 'O', new_text)
  new_text = gsub('Ô', 'O', new_text)
  new_text = gsub('ü', 'ue', new_text)
  new_text = gsub('ú', 'u', new_text)
  new_text = gsub('ù', 'u', new_text)
  new_text = gsub('û', 'u', new_text)
  new_text = gsub('Ü', 'UE', new_text)
  new_text = gsub('Ú', 'U', new_text)
  new_text = gsub('Ù', 'U', new_text)
  new_text = gsub('Û', 'U', new_text)
  new_text = gsub('ç', "c", new_text)
  new_text = gsub("[.]", '', new_text)
  new_text = gsub(",", "", new_text)
  new_text = gsub(' ', '', new_text)
  new_text = gsub('-', '', new_text)
  new_text = gsub('_', '', new_text)
  new_text = gsub("'", "", new_text)
  new_text = gsub("[+]","", new_text)
  new_text = gsub("[/]","", new_text)
  new_text = gsub("[(]","", new_text)
  new_text = gsub("[)]","", new_text)
  
  return(new_text)
}

# remove special characters from street and city

patient_addresses <- patient_addresses %>%
  mutate(street = replace_special_characters(street),
         street_abbreviation_1 = replace_special_characters(street_abbreviation_1),
         street_abbreviation_2 = replace_special_characters(street_abbreviation_2),
         city = replace_special_characters(city))


# set text to lower case

patient_addresses <- patient_addresses %>%
  rowwise() %>%
  mutate(street = tolower(street),
         street_abbreviation_1 = tolower(street_abbreviation_1),
         street_abbreviation_2 = tolower(street_abbreviation_2),
         city = tolower(city)) %>%
  ungroup()


# Because numbers can include special characters (e.g, "13a" or "13.1"), special characters are not removed from the number. Instead an integer number is created by deleting the characters and digits after the first non-digit character.

# define function to remove characters and digits after the first non-digit character

remove_after_non_digit <- function(text) {
  gsub("\\D.*", "", text)
}

# create integer of the address number. In case the number starts with a non-digit character, the integer is set to NA.

patient_addresses <- patient_addresses %>%
   mutate(number_int = remove_after_non_digit(number),
          number_int = case_when(number_int == "" ~ NA,
                                            TRUE ~ number_int),
          number_int = as.integer(number_int))


## Find patient addresses in reference dataset ----------------------------------------------------------------------------------------

# The address matching will be done in two steps.
# In a first step, the datasets are matched by the matching function of the data.table format. This will find all perfectly matching addresses.
# In the second step, custom functions are used to find the best matching address in the reference dataset. This will allow for some differences between the patient and the reference addresses.

### Simple dataset matching: data.table ---------------------------------------------------------------------------------------------------

# First, patient addresses are found in the reference dataset with the matching function of the data.table format.
# Several rounds are used to match the addresses (e.g., matching based on plz and city, matching only based on plz).
# These rounds are repeated to match the street, street_abbreviation_1, and street_abbreviation_2 variables.

# select needed variables from the reference dataset

addresses_and_swiss_sep_reference <- addresses_and_swiss_sep %>%
  select(address_id_reference, street_reference, street_short_reference, number_reference, number_int_reference, plz_reference, city_reference)

# tansform dataframes into data.tables

patient_addresses_tbl <- data.table(patient_addresses)

addresses_reference_tbl <- data.table(addresses_and_swiss_sep_reference)


# start time to benchmark

start_time <- Sys.time()

#_______________________________________________________________________________________________________________

# match on street = street_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_0 <- data.frame(patient_addresses_tbl)

patient_addresses_matched_0 <- patient_addresses_results_0 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_1 <- patient_addresses_results_0 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_1_tbl <- data.table(patient_addresses_1)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_1_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_1 <- data.frame(patient_addresses_1_tbl)

patient_addresses_matched_1 <- patient_addresses_results_1 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_2 <- patient_addresses_results_1 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_2_tbl <- data.table(patient_addresses_2)

#_______________________________________________________________________________________________________________

# match on street = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_2_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_2 <- data.frame(patient_addresses_2_tbl)

patient_addresses_matched_2 <- patient_addresses_results_2 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_3 <- patient_addresses_results_2 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_3_tbl <- data.table(patient_addresses_3)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_3_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_3 <- data.frame(patient_addresses_3_tbl)

patient_addresses_matched_3 <- patient_addresses_results_3 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_4 <- patient_addresses_results_3 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_4_tbl <- data.table(patient_addresses_4)

#_______________________________________________________________________________________________________________


# match on street = street_reference, number = number_reference, city = city_reference

setDT(patient_addresses_4_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_4 <- data.frame(patient_addresses_4_tbl)

patient_addresses_matched_4 <- patient_addresses_results_4 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_5 <- patient_addresses_results_4 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_5_tbl <- data.table(patient_addresses_5)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_5_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_5 <- data.frame(patient_addresses_5_tbl)

patient_addresses_matched_5 <- patient_addresses_results_5 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_6 <- patient_addresses_results_5 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_6_tbl <- data.table(patient_addresses_6)

#_______________________________________________________________________________________________________________

# match on street = street_short_reference, number = number_reference, city = city_reference

setDT(patient_addresses_6_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_6 <- data.frame(patient_addresses_6_tbl)

patient_addresses_matched_6 <- patient_addresses_results_6 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_7 <- patient_addresses_results_6 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_7_tbl <- data.table(patient_addresses_7)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_7_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_7 <- data.frame(patient_addresses_7_tbl)

patient_addresses_matched_7 <- patient_addresses_results_7 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_8 <- patient_addresses_results_7 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_8_tbl <- data.table(patient_addresses_8)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_8_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_8 <- data.frame(patient_addresses_8_tbl)

patient_addresses_matched_8 <- patient_addresses_results_8 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_9 <- patient_addresses_results_8 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_9_tbl <- data.table(patient_addresses_9)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_9_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_9 <- data.frame(patient_addresses_9_tbl)

patient_addresses_matched_9 <- patient_addresses_results_9 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_10 <- patient_addresses_results_9 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_10_tbl <- data.table(patient_addresses_10)

#_______________________________________________________________________________________________________________

# match on street = street_short_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_10_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_10 <- data.frame(patient_addresses_10_tbl)

patient_addresses_matched_10 <- patient_addresses_results_10 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_11 <- patient_addresses_results_10 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_11_tbl <- data.table(patient_addresses_11)

#_______________________________________________________________________________________________________________

# match on street = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_11_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_11 <- data.frame(patient_addresses_11_tbl)

patient_addresses_matched_11 <- patient_addresses_results_11 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_12 <- patient_addresses_results_11 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_12_tbl <- data.table(patient_addresses_12)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_12_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_12 <- data.frame(patient_addresses_12_tbl)

patient_addresses_matched_12 <- patient_addresses_results_12 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_13 <- patient_addresses_results_12 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_13_tbl <- data.table(patient_addresses_13)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_13_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_13 <- data.frame(patient_addresses_13_tbl)

patient_addresses_matched_13 <- patient_addresses_results_13 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_14 <- patient_addresses_results_13 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_14_tbl <- data.table(patient_addresses_14)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_14_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_14 <- data.frame(patient_addresses_14_tbl)

patient_addresses_matched_14 <- patient_addresses_results_14 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_15 <- patient_addresses_results_14 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_15_tbl <- data.table(patient_addresses_15)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_15_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_15 <- data.frame(patient_addresses_15_tbl)

patient_addresses_matched_15 <- patient_addresses_results_15 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_16 <- patient_addresses_results_15 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_16_tbl <- data.table(patient_addresses_16)

#_______________________________________________________________________________________________________________


# match on street_abbreviation_1 = street_reference, number = number_reference, city = city_reference

setDT(patient_addresses_16_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_16 <- data.frame(patient_addresses_16_tbl)

patient_addresses_matched_16 <- patient_addresses_results_16 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_17 <- patient_addresses_results_16 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_17_tbl <- data.table(patient_addresses_17)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_17_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_17 <- data.frame(patient_addresses_17_tbl)

patient_addresses_matched_17 <- patient_addresses_results_17 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_18 <- patient_addresses_results_17 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_18_tbl <- data.table(patient_addresses_18)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_short_reference, number = number_reference, city = city_reference

setDT(patient_addresses_18_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_18 <- data.frame(patient_addresses_18_tbl)

patient_addresses_matched_18 <- patient_addresses_results_18 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_19 <- patient_addresses_results_18 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_19_tbl <- data.table(patient_addresses_19)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_19_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_19 <- data.frame(patient_addresses_19_tbl)

patient_addresses_matched_19 <- patient_addresses_results_19 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_20 <- patient_addresses_results_19 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_20_tbl <- data.table(patient_addresses_20)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_20_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_20 <- data.frame(patient_addresses_20_tbl)

patient_addresses_matched_20 <- patient_addresses_results_20 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_21 <- patient_addresses_results_20 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_21_tbl <- data.table(patient_addresses_21)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_21_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_21 <- data.frame(patient_addresses_21_tbl)

patient_addresses_matched_21 <- patient_addresses_results_21 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_22 <- patient_addresses_results_21 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_22_tbl <- data.table(patient_addresses_22)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_short_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_22_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_22 <- data.frame(patient_addresses_22_tbl)

patient_addresses_matched_22 <- patient_addresses_results_22 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_23 <- patient_addresses_results_22 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_23_tbl <- data.table(patient_addresses_23)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_1 = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_23_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_1 = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_23 <- data.frame(patient_addresses_23_tbl)

patient_addresses_matched_23 <- patient_addresses_results_23 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_24 <- patient_addresses_results_23 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_24_tbl <- data.table(patient_addresses_24)


#_______________________________________________________________________________________________________________


# match on street_abbreviation_2 = street_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_24_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_24 <- data.frame(patient_addresses_24_tbl)

patient_addresses_matched_24 <- patient_addresses_results_24 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_25 <- patient_addresses_results_24 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_25_tbl <- data.table(patient_addresses_25)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_25_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_25 <- data.frame(patient_addresses_25_tbl)

patient_addresses_matched_25 <- patient_addresses_results_25 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_26 <- patient_addresses_results_25 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_26_tbl <- data.table(patient_addresses_26)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_short_reference, number = number_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_26_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number = "number_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_26 <- data.frame(patient_addresses_26_tbl)

patient_addresses_matched_26 <- patient_addresses_results_26 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_27 <- patient_addresses_results_26 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_27_tbl <- data.table(patient_addresses_27)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference, city = city_reference

setDT(patient_addresses_27_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_27 <- data.frame(patient_addresses_27_tbl)

patient_addresses_matched_27 <- patient_addresses_results_27 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_28 <- patient_addresses_results_27 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_28_tbl <- data.table(patient_addresses_28)

#_______________________________________________________________________________________________________________


# match on street_abbreviation_2 = street_reference, number = number_reference, city = city_reference

setDT(patient_addresses_28_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_28 <- data.frame(patient_addresses_28_tbl)

patient_addresses_matched_28 <- patient_addresses_results_28 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_29 <- patient_addresses_results_28 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_29_tbl <- data.table(patient_addresses_29)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_29_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_29 <- data.frame(patient_addresses_29_tbl)

patient_addresses_matched_29 <- patient_addresses_results_29 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_30 <- patient_addresses_results_29 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_30_tbl <- data.table(patient_addresses_30)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_short_reference, number = number_reference, city = city_reference

setDT(patient_addresses_30_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number = "number_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_30 <- data.frame(patient_addresses_30_tbl)

patient_addresses_matched_30 <- patient_addresses_results_30 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_31 <- patient_addresses_results_30 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_31_tbl <- data.table(patient_addresses_31)

#_______________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, city = city_reference

setDT(patient_addresses_31_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number_int = "number_int_reference", city = "city_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_31 <- data.frame(patient_addresses_31_tbl)

patient_addresses_matched_31 <- patient_addresses_results_31 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_32 <- patient_addresses_results_31 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_32_tbl <- data.table(patient_addresses_32)

#__________________________________________________________________________________________________________________________________
# match on street_abbreviation_2 = street_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_32_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_32 <- data.frame(patient_addresses_32_tbl)

patient_addresses_matched_32 <- patient_addresses_results_32 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_33 <- patient_addresses_results_32 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_33_tbl <- data.table(patient_addresses_33)

#__________________________________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_33_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_33 <- data.frame(patient_addresses_33_tbl)

patient_addresses_matched_33 <- patient_addresses_results_33 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_34 <- patient_addresses_results_33 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_34_tbl <- data.table(patient_addresses_34)

#__________________________________________________________________________________________________________________________________
# match on street_abbreviation_2 = street_short_reference, number = number_reference, plz = plz_reference

setDT(patient_addresses_34_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number = "number_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_34 <- data.frame(patient_addresses_34_tbl)

patient_addresses_matched_34 <- patient_addresses_results_34 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)

# select non matched addresses and create data.table

patient_addresses_35 <- patient_addresses_results_34 %>%
  filter(is.na(matched_address_id)==TRUE)

patient_addresses_35_tbl <- data.table(patient_addresses_35)

#__________________________________________________________________________________________________________________________________

# match on street_abbreviation_2 = street_reference, number_int = number_int_reference, plz = plz_reference

setDT(patient_addresses_35_tbl)[addresses_reference_tbl, matched_address_id := i.address_id_reference, on = c(street_abbreviation_2 = "street_short_reference", number_int = "number_int_reference", plz = "plz_reference")]

# create dataframe of results and select matched addresses

patient_addresses_results_35 <- data.frame(patient_addresses_35_tbl)

patient_addresses_matched_35 <- patient_addresses_results_35 %>%
  filter(is.na(matched_address_id)==FALSE) %>%
  mutate(score = 0)


#__________________________________________________________________________________________________________________________________

# create one dataframe of all the matched addresses

patient_addresses_matched <- rbind(patient_addresses_matched_0,
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
                                   patient_addresses_matched_35)

patient_addresses_matched <- patient_addresses_matched %>%
  select(-number_int)

# select the non matched addresses

patient_addresses_non_matched <- patient_addresses_results_35 %>%
  filter(is.na(matched_address_id)==TRUE)

# remove dataframes from memory

rm(patient_addresses_matched_0, patient_addresses_matched_1, patient_addresses_matched_2, patient_addresses_matched_3, patient_addresses_matched_4, patient_addresses_matched_5, patient_addresses_matched_6, patient_addresses_matched_7, patient_addresses_matched_8, patient_addresses_matched_9, patient_addresses_matched_10, patient_addresses_matched_11, patient_addresses_matched_12, patient_addresses_matched_13, patient_addresses_matched_14, patient_addresses_matched_15, patient_addresses_matched_16, patient_addresses_matched_17, patient_addresses_matched_18, patient_addresses_matched_19, patient_addresses_matched_20, patient_addresses_matched_21, patient_addresses_matched_22, patient_addresses_matched_23, patient_addresses_matched_24, patient_addresses_matched_25, patient_addresses_matched_26, patient_addresses_matched_27, patient_addresses_matched_28, patient_addresses_matched_29, patient_addresses_matched_30, patient_addresses_matched_31, patient_addresses_matched_32, patient_addresses_matched_33, patient_addresses_matched_34, patient_addresses_matched_35)

rm(patient_addresses_results_0, patient_addresses_results_1, patient_addresses_results_2, patient_addresses_results_3, patient_addresses_results_4, patient_addresses_results_5, patient_addresses_results_6, patient_addresses_results_7, patient_addresses_results_8, patient_addresses_results_9, patient_addresses_results_10, patient_addresses_results_11, patient_addresses_results_12, patient_addresses_results_13, patient_addresses_results_14, patient_addresses_results_15, patient_addresses_results_16, patient_addresses_results_17, patient_addresses_results_18, patient_addresses_results_19, patient_addresses_results_20, patient_addresses_results_21, patient_addresses_results_22, patient_addresses_results_23, patient_addresses_results_24, patient_addresses_results_25, patient_addresses_results_26, patient_addresses_results_27, patient_addresses_results_28, patient_addresses_results_29, patient_addresses_results_30, patient_addresses_results_31, patient_addresses_results_32, patient_addresses_results_33, patient_addresses_results_34, patient_addresses_results_35)

rm(patient_addresses_1, patient_addresses_2, patient_addresses_3, patient_addresses_4, patient_addresses_5, patient_addresses_6, patient_addresses_7, patient_addresses_8, patient_addresses_9, patient_addresses_10, patient_addresses_11, patient_addresses_12, patient_addresses_13, patient_addresses_14, patient_addresses_15, patient_addresses_16, patient_addresses_17, patient_addresses_18, patient_addresses_19, patient_addresses_20, patient_addresses_21, patient_addresses_22, patient_addresses_23, patient_addresses_24, patient_addresses_25, patient_addresses_26, patient_addresses_27, patient_addresses_28, patient_addresses_29, patient_addresses_30, patient_addresses_31, patient_addresses_32, patient_addresses_33, patient_addresses_34, patient_addresses_35)

rm(patient_addresses_tbl, patient_addresses_1_tbl, patient_addresses_2_tbl, patient_addresses_3_tbl, patient_addresses_4_tbl, patient_addresses_5_tbl, patient_addresses_6_tbl, patient_addresses_7_tbl, patient_addresses_8_tbl, patient_addresses_9_tbl, patient_addresses_10_tbl, patient_addresses_11_tbl, patient_addresses_12_tbl, patient_addresses_13_tbl, patient_addresses_14_tbl, patient_addresses_15_tbl, patient_addresses_16_tbl, patient_addresses_17_tbl, patient_addresses_18_tbl, patient_addresses_19_tbl, patient_addresses_20_tbl, patient_addresses_21_tbl, patient_addresses_22_tbl, patient_addresses_23_tbl, patient_addresses_24_tbl, patient_addresses_25_tbl, patient_addresses_26_tbl, patient_addresses_27_tbl, patient_addresses_28_tbl, patient_addresses_29_tbl, patient_addresses_30_tbl, patient_addresses_31_tbl, patient_addresses_32_tbl, patient_addresses_33_tbl, patient_addresses_34_tbl, patient_addresses_35_tbl)

# report the results

end_time <- Sys.time()
print(end_time - start_time)

print(paste("number of matched addresses: ", patient_addresses_matched %>% nrow()))

print(paste("number of non-matched addresses: ", patient_addresses_non_matched %>% nrow()))


### Allow spelling errors during dataset matching ----------------------------------------------------------------------------------------

# Second, custom functions are defined to find the best matching addresses in the reference dataset.
# Briefly, a similarity score between the patient address and a reference address is calculated based on the similarity of the street, number, plz, and city. This similarity score is calculated for all reference addresses that match the plz or city of the patient address. This process is repeated for every patient address.

# Create new variables in the dataset of non-matched patient addresses and the reference dataset.

# adapt patient_addresses

patient_addresses_36 <- patient_addresses_non_matched %>%
  mutate(street_len = nchar(street),
         street_abbreviation_1_len = nchar(street_abbreviation_1),
         street_abbreviation_2_len = nchar(street_abbreviation_2),
         city_len = nchar(city)) %>% 
  select(-matched_address_id)

# select the needed variables from the original reference dataset

addresses_and_swiss_sep_reference <- addresses_and_swiss_sep %>%
  select(address_id_reference, street_reference, street_len_reference, street_short_reference, street_short_len_reference, number_reference, number_int_reference, plz_reference, city_reference, city_len_reference)


# Define the functions to find the best matching addresses in the reference dataset.

# Define function to check similarity of two rows with address information
calculate_similarity <- function(street, street_abbreviation_1, street_abbreviation_2, number, city, street_reference, street_short_reference, number_reference, city_reference, street_len_difference, street_short_len_difference, street_abbreviation_1_len_difference,  street_abbreviation_1_short_len_difference, street_abbreviation_2_len_difference,  street_abbreviation_2_short_len_difference, number_difference, plz_difference, city_len_difference){
  # Try to minimize the cases when a Levenshtein distance is calculated because this is compute-intensive
  
  # Street score
  if (is.na(street) & is.na(street_reference)){
    street_score <- 0
  } else if (is.na(street)| is.na(street_reference)){
    street_score <- 50
  } else if (street == street_reference){
    street_score <- 0
  } else if (grepl(street, street_reference) & (street_len_difference == 1)) {
    street_score <- 10
  } else if (grepl(street, street_reference)){
    street_score <- 20
  } else if (grepl(street_reference, street)){
    street_score <- 20
  } else if (street_len_difference < 5) {
    street_score <- 10 * stringdist(street, street_reference, method = "lv")
    if (street_score > 50){
      street_score <- 50
    }
  } else {
    street_score <- 50
  }
  
  # Street short score
  if (street_score > 20) {
    if (is.na(street) & is.na(street_short_reference)){
      street_short_score <- 0
    } else if (is.na(street)| is.na(street_short_reference)){
      street_short_score <- 50
    } else if (street == street_short_reference){
      street_short_score <- 0
    } else if (grepl(street, street_short_reference) & (street_short_len_difference == 1)) {
      street_short_score <- 10
    } else if (grepl(street, street_short_reference)){
      street_short_score <- 20
    } else if (grepl(street_short_reference, street)){
      street_short_score <- 20
    } else if (street_short_len_difference < 5) {
      street_short_score <- 10 * stringdist(street, street_short_reference, method = "lv")
      if (street_short_score > 50){
      street_short_score <- 50
    }
    } else {
      street_short_score <- 50
    }
  } else {
    street_short_score <- 50
  }
  
  street_score <- min(street_score, street_short_score)
  
  # Street split 1
  if (street_score == 50 & is.na(street_abbreviation_1)==FALSE){
    if (is.na(street_abbreviation_1) & is.na(street_reference)){
      street_abbreviation_1_score <- 0
    } else if (is.na(street_abbreviation_1)| is.na(street_reference)){
      street_abbreviation_1_score <- 50
    } else if (street_abbreviation_1 == street_reference){
      street_abbreviation_1_score <- 0
    } else if (grepl(street_abbreviation_1, street_reference) & (street_abbreviation_1_len_difference == 1)) {
      street_abbreviation_1_score <- 10
    } else if (grepl(street_abbreviation_1, street_reference)){
      street_abbreviation_1_score <- 20
    } else if (grepl(street_reference, street_abbreviation_1)){
      street_abbreviation_1_score <- 20
    } else if (street_len_difference < 5) {
      street_abbreviation_1_score <- 10 * stringdist(street_abbreviation_1, street_reference, method = "lv")
      if (street_abbreviation_1_score > 50){
        street_abbreviation_1_score <- 50
      }
    } else {
      street_abbreviation_1_score <- 50
    }
  } else {
    street_abbreviation_1_score <- 50
  }
  
  
  if (street_score == 50 & street_abbreviation_1_score > 20 & is.na(street_abbreviation_1)==FALSE) {
    if (is.na(street_abbreviation_1) & is.na(street_short_reference)){
      street_abbreviation_1_short_score <- 0
    } else if (is.na(street_abbreviation_1)| is.na(street_short_reference)){
      street_abbreviation_1_short_score <- 50
    } else if (street_abbreviation_1 == street_short_reference){
      street_abbreviation_1_short_score <- 0
    } else if (grepl(street_abbreviation_1, street_short_reference) & (street_abbreviation_1_short_len_difference == 1)) {
      street_abbreviation_1_short_score <- 10
    } else if (grepl(street_abbreviation_1, street_short_reference)){
      street_abbreviation_1_short_score <- 20
    } else if (grepl(street_short_reference, street_abbreviation_1)){
      street_abbreviation_1_short_score <- 20
    } else if (street_short_len_difference < 5) {
      street_abbreviation_1_short_score <- 10 * stringdist(street_abbreviation_1, street_short_reference, method = "lv")
      if (street_abbreviation_1_short_score > 50){
        street_abbreviation_1_short_score <- 50
      }
    } else {
      street_abbreviation_1_short_score <- 50
    }
  } else {
    street_abbreviation_1_short_score <- 50
  }
  
  street_score <- min(street_score, street_abbreviation_1_score, street_abbreviation_1_short_score)
  
# Street split 2
  if (street_score == 50 & is.na(street_abbreviation_2)==FALSE){
    if (is.na(street_abbreviation_2) & is.na(street_reference)){
      street_abbreviation_2_score <- 0
    } else if (is.na(street_abbreviation_2)| is.na(street_reference)){
      street_abbreviation_2_score <- 50
    } else if (street_abbreviation_2 == street_reference){
      street_abbreviation_2_score <- 0
    } else if (grepl(street_abbreviation_2, street_reference) & (street_abbreviation_2_len_difference == 1)) {
      street_abbreviation_2_score <- 10
    } else if (grepl(street_abbreviation_2, street_reference)){
      street_abbreviation_2_score <- 20
    } else if (grepl(street_reference, street_abbreviation_2)){
      street_abbreviation_2_score <- 20
    } else if (street_len_difference < 5) {
      street_abbreviation_2_score <- 10 * stringdist(street_abbreviation_2, street_reference, method = "lv")
      if (street_abbreviation_2_score > 50){
        street_abbreviation_2_score <- 50
      }
    } else {
      street_abbreviation_2_score <- 50
    }
  } else {
    street_abbreviation_2_score <- 50
  }
  
  
  if (street_score == 50 & street_abbreviation_2_score > 20 & is.na(street_abbreviation_2)==FALSE) {
    if (is.na(street_abbreviation_2) & is.na(street_short_reference)){
      street_abbreviation_2_short_score <- 0
    } else if (is.na(street_abbreviation_2)| is.na(street_short_reference)){
      street_abbreviation_2_short_score <- 50
    } else if (street_abbreviation_2 == street_short_reference){
      street_abbreviation_2_short_score <- 0
    } else if (grepl(street_abbreviation_2, street_short_reference) & (street_abbreviation_2_short_len_difference == 1)) {
      street_abbreviation_2_short_score <- 10
    } else if (grepl(street_abbreviation_2, street_short_reference)){
      street_abbreviation_2_short_score <- 20
    } else if (grepl(street_short_reference, street_abbreviation_2)){
      street_abbreviation_2_short_score <- 20
    } else if (street_short_len_difference < 5) {
      street_abbreviation_2_short_score <- 10 * stringdist(street_abbreviation_2, street_short_reference, method = "lv")
      if (street_abbreviation_2_short_score > 50){
        street_abbreviation_2_short_score <- 50
      }
    } else {
      street_abbreviation_2_short_score <- 50
    }
  } else {
    street_abbreviation_2_short_score <- 50
  }
  
  street_score <- min(street_score, street_abbreviation_2_score, street_abbreviation_2_short_score)
  
  
  # Number score
  if (is.na(number) & is.na(number_reference)) {
    number_score <- 0
  } else if (is.na(number) | is.na(number_reference)) {
    number_score <- 20
  } else if (number == number_reference) {
    number_score <- 0
  } else if (is.na(number_difference)) {
    number_score <- 10*stringdist(number, number_reference, method = "lv")
    if (number_score > 20){
      number_score <- 20
    }
  } else if (number_difference == 0) {
    number_score <- 0.5
  } else if (number_difference <= 20) {
    number_score <- number_difference
  } else {
    number_score <- 20
  }
  
  # PLZ score
  if (plz_difference <= 10) {
    plz_score <- plz_difference
  } else if (plz_difference <= 100) {
    plz_score <- 20
  } else {
    plz_score <- 30
  }
  
  # City score
  if (is.na(city) & is.na(city_reference)){
    city_score <- 0
  } else if (is.na(city)| is.na(city_reference)){
    city_score <- 50
  } else if (city == city_reference){
    city_score <- 0
  } else if (grepl(city, city_reference) & (city_len_difference == 1)) {
    city_score <- 10
  } else if (grepl(city, city_reference)){
    city_score <- 20
  } else if (grepl(city_reference, city)){
    city_score <- 20
  } else if (city_len_difference < 3) {
      city_score <- 10 * stringdist(city, city_reference, method = "lv")
      if (city_score > 30){
      city_score <- 30
    }
  } else {
      city_score <- 30
  }
  
  # If the plz or city is correct, the other information is not that important anymore
  if (plz_score == 0) {
    city_score <- city_score * 0.5
  }
  
  if (city_score == 0) {
    plz_score <- plz_score * 0.5
  }
  
  overall_score <- street_score + number_score + plz_score + city_score
  
  return(overall_score)
}

# Define function to run on one row. In this function we search addresses when the plz or the city matches.

address_match_row <- function(street, street_abbreviation_1, street_abbreviation_2, street_len, street_abbreviation_1_len, street_abbreviation_2_len, number_int, number, plz, city, city_len, addresses_reference = addresses_and_swiss_sep_reference){
  
  # Set best score and best_address_id
  best_score <- 50
  best_address_id <- NA
  
  
  
  # filter the reference DataFrame based on the plz and the city name.
  
  addresses_reference <- addresses_reference[addresses_reference$plz_reference == plz | addresses_reference$city_reference == city,]
  
  # Create additional variables in the reference DataFrame for calculate_similarity function
  addresses_reference$street_len_difference <- abs(addresses_reference$street_len_reference - street_len)
  addresses_reference$street_short_len_difference <- abs(addresses_reference$street_short_len_reference - street_len)
  addresses_reference$street_abbreviation_1_len_difference <- abs(addresses_reference$street_len_reference - street_abbreviation_1_len)
  addresses_reference$street_abbreviation_1_short_len_difference <- abs(addresses_reference$street_short_len_reference - street_abbreviation_1_len)
  addresses_reference$street_abbreviation_2_len_difference <- abs(addresses_reference$street_len_reference - street_abbreviation_2_len)
  addresses_reference$street_abbreviation_2_short_len_difference <- abs(addresses_reference$street_short_len_reference - street_abbreviation_2_len)
  addresses_reference$number_difference <- abs(addresses_reference$number_int_reference - number_int)
  addresses_reference$plz_difference <- abs(addresses_reference$plz_reference - plz)
  addresses_reference$city_len_difference <- abs(addresses_reference$city_len_reference - city_len)
  
  # Iterate over the rows in the reference DataFrame and calculate similarity. Don't iterate over the rows if the dataframe is empty.
  
  if (nrow(addresses_reference) == 0) {
    best_score <- NA
    best_address_id <- NA
  } else {
    for (index2 in 1:nrow(addresses_reference)) {
      row2 <- addresses_reference[index2, ]
      street_reference <- row2$street_reference
      street_short_reference <- row2$street_short_reference
      number_reference <- row2$number_reference
      city_reference <- row2$city_reference
      street_len_difference <- row2$street_len_difference
      street_short_len_difference <- row2$street_short_len_difference
      street_abbreviation_1_len_difference <- row2$street_abbreviation_1_len_difference
      street_abbreviation_1_short_len_difference <- row2$street_abbreviation_1_short_len_difference
      street_abbreviation_2_len_difference <- row2$street_abbreviation_2_len_difference
      street_abbreviation_2_short_len_difference <- row2$street_abbreviation_2_short_len_difference
      number_difference <- row2$number_difference
      plz_difference <- row2$plz_difference
      city_len_difference <- row2$city_len_difference
    
      score <- calculate_similarity(street, street_abbreviation_1, street_abbreviation_2, number, city, street_reference, street_short_reference, number_reference, city_reference, street_len_difference, street_short_len_difference, street_abbreviation_1_len_difference,  street_abbreviation_1_short_len_difference, street_abbreviation_2_len_difference,  street_abbreviation_2_short_len_difference, number_difference, plz_difference, city_len_difference)
      
    
      if (score < best_score) {
        best_score <- score
        best_address_id <- row2$address_id_reference
      }
    
      # Break if it is clear that the best_score can't get lower than the current best_score
      if (best_score <= 0.5) {
        break
      }
    
    }
  }
  

  result <- paste(best_address_id, best_score, collapse= " ")
  
  return(result)
  
}


# Apply the function address_match_row to find patient addresses in the reference dataset.

# create a data.table

patient_addresses_36_tbl <- data.table(patient_addresses_36)

# start time to benchmark
start_time <- Sys.time()

#__________________________________________________________________________________________________________________________________

# Use a parallel version of the apply function in the future.apply package

plan(multisession)

# match with the custom functions
patient_addresses_36_tbl[, match := future_mapply(address_match_row, street = street, street_abbreviation_1 = street_abbreviation_1, street_abbreviation_2 = street_abbreviation_2, street_len = street_len, street_abbreviation_1_len = street_abbreviation_1_len, street_abbreviation_2_len = street_abbreviation_2_len, number_int = number_int, number = number, plz = plz, city = city, city_len = city_len)]

# create dataframe and select results

patient_addresses_results_36 <- data.frame(patient_addresses_36_tbl) %>% 
  mutate(matched_address_id = as.numeric(word(match, start=1, end=1, sep=" ")),
         score = as.numeric(word(match, start=2, end=2, sep=" "))) %>%
  select(-match)

#__________________________________________________________________________________________________________________________________

# select matched addresses

patient_addresses_matched_36 <- patient_addresses_results_36 %>%
  filter(is.na(matched_address_id)==FALSE)

# tidy matched dataframe

patient_addresses_matched_36 <- patient_addresses_matched_36 %>%
  select(-c(street_len, street_abbreviation_1_len, street_abbreviation_2_len, number_int, city_len))


# select non-matched addresses

patient_addresses_non_matched_36 <- patient_addresses_results_36 %>%
  filter(is.na(matched_address_id)==TRUE)


end_time <- Sys.time()
print(end_time - start_time)

print(paste("number of matched addresses: ", patient_addresses_matched_36 %>% nrow()))


print(paste("number of non-matched addresses: ", patient_addresses_non_matched_36 %>% nrow()))


## Add Swiss-SEP variables --------------------------------------------------------------------------------------------------------

# Now the matched address details and the Swiss-SEP variables can be added.
 
# Create a dataset with all matched addresses.

patient_addresses_matched <- rbind(patient_addresses_matched, patient_addresses_matched_36)


# Add the details of the reference addresses and the Swiss-SEP variables.

# join datasets on address id
patient_addresses_matched <- left_join(patient_addresses_matched, addresses_and_swiss_sep, by=c("matched_address_id" = "address_id_reference"))

# select variables: you can also choose geocode_E_address and geocode_N_address, if you want to save geocodes of your patients addresses. 

patient_addresses_matched <- patient_addresses_matched %>%
  select(patient_ID, street, number, plz, city, street_original, number_reference, plz_reference, city_original, swiss_sep_1, swiss_sep_2, swiss_sep_3, distance_address_swiss_sep)


# This dataset of patient addresses with the added Swiss-SEP variables can now be used to create the Swiss-SEP concept for the patients. The variables for the Swiss Socio Economic Position concept are:

# - hasValue: integer without unit
# - hasDistance: integer in meters
# - hasVersion: string ("Version 1.0", "Version 2.0", "Version 3.0)

# For patients where the Swiss-SEP variables could not be matched, the concept can be left empty.


