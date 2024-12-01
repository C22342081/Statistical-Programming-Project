# ========================================================================
# Title: Advertising Data Processor
# Description: Processes advertising data by importing, transforming,
#              and joining datasets, adjusting times to UTC manually,
#              and outputting processed data to CSV files.
# Author: Susan Locatelli - C22342081
# ========================================================================

# Set the working directory to your project folder

setwd("C:/Users/susan/OneDrive - Technological University Dublin/Desktop/STSATISTICAL_PROGRAMMING/Programming Project 1")

# Verify the working directory

print(getwd())

# ------------------ Load Necessary Libraries ------------------

library(tidyverse)  # Provides data manipulation functions

# ------------------ Define Timezone Offsets ------------------

# Create a named list mapping timezones to their UTC offsets in hours
timezone_offsets <- list(
  "UTC" = 0,           # Coordinated Universal Time
  "Eastern time" = 5,  # UTC - 5 hours
  "Central time" = 6,  # UTC - 6 hours
  "Mountain time" = 7, # UTC - 7 hours
  "Pacific time" = 8   # UTC - 8 hours
)

# ------------------ Function Definitions ------------------

# Function: days_in_month

# Purpose: Calculate the number of days in a given month and year,
#          accounting for leap years.

# Parameters:
#   - month (integer): Month as a number (1-12).
#   - year (integer): Year as a four-digit number (e.g., 2018).

# Returns:
#   - Integer representing the number of days in the specified month.

days_in_month <- function(month, year) {
  
  # Check if the month is one with 31 days
  
  if (month %in% c(1, 3, 5, 7, 8, 10, 12)) {
    return(31)
  }
  
  # Check if the month is one with 30 days
  
  else if (month %in% c(4, 6, 9, 11)) {
    return(30)
  }
  
  # Handle February separately, considering leap years
  
  else if (month == 2) {
    
    # Leap year if divisible by 4 and (not divisible by 100 or divisible by 400)
    
    if ((year %% 4 == 0 && year %% 100 != 0) || (year %% 400 == 0)) {
      return(29)  # Leap year
    } else {
      return(28)  # Common year
    }
  }
  
  # Invalid month input
  
  else {
    stop(paste("Invalid month:", month))
  }
}

# Function: parse_date

# Purpose: Parse a date string into day, month, and year integers.

# Parameters:
#   - date_str (string): Date in "DD/MM/YYYY" format.

# Returns:
#   - List containing day, month, and year as integers.

parse_date <- function(date_str) {
  date_parts <- unlist(strsplit(date_str, "/"))
  day <- as.integer(date_parts[1])
  month <- as.integer(date_parts[2])
  year <- as.integer(date_parts[3])
  return(list(day = day, month = month, year = year))
}

# Function: parse_time

# Purpose: Parse a time string into hour, minute, and second integers.

# Parameters:
#   - time_str (string): Time in "HH:MM:SS" format.

# Returns:
#   - List containing hour, minute, and second as integers.

parse_time <- function(time_str) {
  time_parts <- unlist(strsplit(time_str, ":"))
  hour <- as.integer(time_parts[1])
  minute <- as.integer(time_parts[2])
  second <- as.integer(time_parts[3])
  return(list(hour = hour, minute = minute, second = second))
}

# Function: adjust_to_utc

# Purpose: Manually adjust local date and time to UTC without using date-time libraries.

# Parameters:
#   - date_str (string): Date in "DD/MM/YYYY" format.
#   - time_str (string): Time in "HH:MM:SS" format.
#   - timezone_str (string): Timezone identifier (e.g., "Eastern time").

# Returns:
#   - List containing adjusted date and time strings in UTC.

adjust_to_utc <- function(date_str, time_str, timezone_str) {
  
  # ------------------ Parse Date and Time ------------------
  
  date_components <- parse_date(date_str)
  time_components <- parse_time(time_str)
  
  # Extract individual components
  
  day <- date_components$day
  month <- date_components$month
  year <- date_components$year
  
  hour <- time_components$hour
  minute <- time_components$minute
  second <- time_components$second
  
  # ------------------ Retrieve Timezone Offset ------------------
  
  offset <- timezone_offsets[[timezone_str]]
  
  # Error handling for unrecognized timezones
  
  if (is.null(offset)) {
    stop(paste("Unknown timezone:", timezone_str))
  }
  
  # ------------------ Adjust Time to UTC ------------------
  
  # Add the timezone offset to the hour to get UTC hour
  
  hour_utc <- hour + offset
  
  # Initialize variables for adjusted date components
  
  new_day <- day
  new_month <- month
  new_year <- year
  
  # ------------------ Handle Hour Overflow ------------------
  
  # While hour is 24 or more, subtract 24 hours and increment the day
  
  while (hour_utc >= 24) {
    hour_utc <- hour_utc - 24
    new_day <- new_day + 1
    
    # Check if day exceeds the number of days in the current month
    
    if (new_day > days_in_month(new_month, new_year)) {
      new_day <- 1
      new_month <- new_month + 1
      
      # If month exceeds 12, reset to January and increment the year
      
      if (new_month > 12) {
        new_month <- 1
        new_year <- new_year + 1
      }
    }
  }
  
  # ------------------ Handle Hour Underflow ------------------
  
  # While hour is negative, add 24 hours and decrement the day
  
  while (hour_utc < 0) {
    hour_utc <- hour_utc + 24
    new_day <- new_day - 1
    
    # If day is less than 1, move to the previous month
    
    if (new_day < 1) {
      new_month <- new_month - 1
      
      # If month is less than 1, reset to December and decrement the year
      
      if (new_month < 1) {
        new_month <- 12
        new_year <- new_year - 1
      }
      # Set day to the last day of the new month
      
      new_day <- days_in_month(new_month, new_year)
    }
  }
  
  # ------------------ Format Adjusted Date and Time ------------------
  
  # Format date and time strings with leading zeros where necessary
  
  date_utc <- sprintf("%02d/%02d/%04d", new_day, new_month, new_year)
  time_utc <- sprintf("%02d:%02d:%02d", hour_utc, minute, second)
  
  # ------------------ Return Adjusted Date and Time ------------------
  
  return(list(date = date_utc, time = time_utc))
}

# Function: process_data

# Purpose: Process impressions or clicks data by adjusting date/time to UTC
#          and merging with campaigns and advertisers data.

# Parameters:
#   - data (data.frame): Data frame containing impressions or clicks data.
#   - campaigns_adv (data.frame): Data frame of campaigns joined with advertisers.

# Returns:
#   - Data frame with adjusted date/time and merged campaign/advertiser data.

process_data <- function(data, campaigns_adv) {
  
  # ------------------ Adjust Date and Time to UTC ------------------
  
  # Apply the adjust_to_utc function to each row
  
  data_utc <- data %>%
    rowwise() %>%  # Operate row by row
    mutate(
      adjusted = list(adjust_to_utc(date, time, timezone)),  # Adjust date/time
      date_utc = adjusted$date,  # Extract adjusted date
      time_utc = adjusted$time   # Extract adjusted time
    ) %>%
    ungroup() %>%
    select(-adjusted)  # Remove temporary 'adjusted' column
  
  # ------------------ Merge with Campaigns and Advertisers ------------------
  
  # Merge the data with the campaigns and advertisers information
  
  data_processed <- merge(data_utc, campaigns_adv, by = "campaign_id", all.x = TRUE)
  
  # ------------------ Return Processed Data ------------------
  
  return(data_processed)
}

# ------------------ Data Import ------------------

# Function: import_data

# Purpose: Import data from CSV and TSV files into data frames.
#          Modified to include 'fill = TRUE' to handle incomplete final lines.

# Parameters:
#   - file_path (string): Path to the data file.
#   - sep (string): Separator used in the file (default is comma).

# Returns:
#   - Data frame containing the imported data.

import_data <- function(file_path, sep = ",") {
  data <- read.csv(file_path, sep = sep, stringsAsFactors = FALSE, fill = TRUE)
  return(data)
}

# Import advertisers data

advertisers <- import_data("advertiser.csv")

# Import campaigns data

campaigns <- import_data("campaigns.csv")

# Import impressions data (tab-separated)

impressions <- import_data("impressions.tsv", sep = "\t")

# Import clicks data (tab-separated)

clicks <- import_data("clicks.tsv", sep = "\t")

# ------------------ Data Preparation ------------------

# Function: prepare_campaigns_data

# Purpose: Prepare campaigns data by renaming columns and merging with advertisers.

# Parameters:
#   - campaigns (data.frame): Data frame of campaigns data.
#   - advertisers (data.frame): Data frame of advertisers data.

# Returns:
#   - Data frame of campaigns merged with advertisers.

prepare_campaigns_data <- function(campaigns, advertisers) {
  
  # Rename columns for consistency
  
  colnames(advertisers)[colnames(advertisers) == "ID"] <- "advertiser_id"
  colnames(campaigns)[colnames(campaigns) == "id"] <- "campaign_id"
  
  # Merge campaigns with advertisers
  
  campaigns_adv <- merge(campaigns, advertisers, by = "advertiser_id", all.x = TRUE)
  return(campaigns_adv)
}

# Prepare campaigns data

campaigns_adv <- prepare_campaigns_data(campaigns, advertisers)

# ------------------ Data Transformation ------------------

# Process impressions data

impressions_processed <- process_data(impressions, campaigns_adv)

# Process clicks data

clicks_processed <- process_data(clicks, campaigns_adv)

# ------------------ Output ------------------

# Function: export_data

# Purpose: Export processed data to a CSV file.

# Parameters:
#   - data (data.frame): Data frame to export.
#   - file_name (string): Name of the output CSV file.

# Returns:
#   - None.

export_data <- function(data, file_name) {
  write.csv(data, file_name, row.names = FALSE)
}

# Export processed impressions data

export_data(impressions_processed, "impressions_processed.csv")

# Export processed clicks data

export_data(clicks_processed, "clicks_processed.csv")

# ========================================================================
# End of Script
# ========================================================================

