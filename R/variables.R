# need to set db connection objects outside the targets plan
packages = c( "tidyverse",
              "lubridate",
              "DBI",
              "odbc",
              "config")

pacman::p_load(char = packages)
# Variables ----
(datestart <-  "2022-01-01")
(dateend <-  "2022-03-31")
#is_annual is a global variable as targets doesn't parse the target as logical
(is_annual <- as.integer(difftime(dateend, datestart)) >= 364)
year_meas = year(datestart)
bias_adjustment_factor = 0L

# db connection objects need to be global vars
connect_access <- connect.access()
connect_envista = connect.envista()