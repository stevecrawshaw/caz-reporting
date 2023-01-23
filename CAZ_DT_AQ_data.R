# Libraries
p <- c("tidyverse", "glue", "janitor", "labelled", "config", "lubridate", "DBI", "odbc")
library("pacman")
p_load(char = p)

# Source import functions

source("../airquality_GIT/ods-import-httr2.R")

# API Keys and variables

apikey <- get(value = "ods-api",
              file = "data/ods_api.yml",
              config = "default")

to_date <- "2021-12-31"

from_date <- to_date %>% 
    as.Date() %>% 
    year() %>% 
    as.character() %>% 
    paste0("-01-01")

# Get air monitoring sites
aqm <- import_ods(dataset = "air-quality-monitoring-sites")

# get the tube data for the time period
raw_tubes <- import_ods(dataset = "no2-tubes-raw",
                        date_col = "mid_date",
                        dateon = from_date,
                        dateoff = to_date,
                        apikey = apikey)

# pivot

pivot_tubes_month <- function(raw_tubes){
    raw_tubes %>% 
        mutate(month = month(mid_date, label = TRUE, abbr = FALSE)) %>% 
        pivot_wider(id_cols = siteid,
                    names_from = month,
                    values_from = concentration, values_fn = mean)
}

month_tubes <- pivot_tubes_month(raw_tubes)

con <- dbConnect(odbc::odbc(), .connection_string = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=S:\\SUSTAIN\\Sustain-Common\\SCCCS\\write_gis_r\\tube_database\\access_2010_versions\\no2_data.accdb;")

# get start and end dates for each site from the Access database
site_dates_tbl <- tbl(con, "data") %>% 
    group_by(LocID) %>% 
    summarise(sitestart = min(dateOn),
              siteend = max(dateOff)) %>% 
    collect() 

# format dates nicely for the spreadsheet
nice_date <- function(date_time){
    date_time %>% 
    as.Date() %>% 
    strftime(format = "%d/%m/%Y")
    }

site_dates_formatted_tbl <- site_dates_tbl %>% 
    mutate(across(where(is.POSIXct), nice_date)) %>% 
    rename(siteid = LocID)
    
dbDisconnect(con)
