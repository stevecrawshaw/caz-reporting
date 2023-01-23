library("pacman")
p_load(tidyverse, glue, janitor, lubridate, DBI, odbc, config)

get.final.tbl <- function() {
    #read the table which holds contin meta data for the envista database
    read_delim(file = "S:/SUSTAIN/Sustain-Common/SCCCS/write_gis_r/R Projects/air_quality_data_management/data/finalContinTablerhtemp.csv",
               delim = ",",
               col_types = "ciiciccc") %>%
        return()
}

connect.envista <- function() {
    con_params <- config::get(file = "S:/SUSTAIN/Sustain-Common/SCCCS/write_gis_r/R Projects/L4/project-1-reporting-pipeline/R/config.yml", config = "envista") # credentials
    con_params$driver
    #make connection to Envista using details in the config file
    dbConnect(
        odbc(),
        Driver = con_params$driver,
        Server = con_params$server,
        Database = con_params$database,
        UID = con_params$uid,
        PWD = con_params$pwd,
        Port = con_params$port
    ) %>%
        return()
    
}

final_tbl <- get.final.tbl()

conn <- connect.envista()

sd <- as.POSIXct("2021-12-31 23:59:00")

get.no2.envista.meta.tbl <- function(final_tbl){

    no2_hour_live_tbl <- final_tbl %>% 
    filter(minutes == 60,
           pollutant == "no2",
           siteid %in% c(203, 215, 463, 270, 501, 672)) %>% 
    select(siteid, value, status, table)
    
    return(no2_hour_live_tbl)
}

get.annual.no2 <- function(siteid,
                           value,
                           status,
                           table,
                           date = "2021-01-01 00:00:00"){
    
    dateon <- make_datetime(year = year(as.POSIXct(date)),
                            month = 1L,
                            day = 1L,
                            hour = 0L,
                            min = 0L,
                            sec = 0,
                            tz = "UTC")
    dateoff <- dateon + years(1)
    
    hrs <- difftime(dateoff, dateon, units = "hours") %>% 
       as.integer()
    
    tbl(conn, table) %>% 
        filter(between(Date_Time, dateon, dateoff)) %>% 
        select(date_time = Date_Time,
               value = all_of(value),
               status = all_of(status)) %>% 
        mutate(no2 = if_else(status == 1, value, NA_real_),
               value = NULL,
               status = NULL) %>% 
        collect() %>% 
        summarise(siteid = siteid,
                  no2_mean = mean(no2, na.rm = TRUE),
                  datacap = round((n() / hrs) * 100, 1)) %>%
        return()
}


pmap_dfr(no2_hour_live_tbl, get.annual.no2)
