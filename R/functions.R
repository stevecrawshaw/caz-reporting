# Functions ----

# helper  functions
date.format <- function(x){
    x %>% as.POSIXct() %>% 
    as.Date() %>% 
    format("%d/%m/%Y") %>% 
        return()
}

# is.annual <- function(datestart, dateend) {
#     # if is.annual = T then we retrieve the NO2 data for DT and continuous
#     days <-
#         (as.Date(dateend) - as.Date(datestart)) %>% as.integer()
#     if_else(days >= 365, TRUE, FALSE) %>%
#         return()
# }

write.output.report.csv <- function(object, datestart, dateend, is_annual){
    date_part <- glue("{datestart}_{dateend}")
    path <- "data/output_reports/"
    
    objname <- deparse(substitute(object))
    filename_stem <- glue("{path}{objname}_{date_part}")
    
     if(is_annual == TRUE){
         filename <- glue("{filename_stem}_annual.csv")
         write_csv(object, filename, na = "")
    } else {
        filename <- glue("{filename_stem}_period.csv")
        write_csv(object, filename, na = "")
        }
    return(filename)
}


#   database connect ----
connect.access <- function() {
    return(
        dbConnect(odbc::odbc(),
                  .connection_string = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=S:\\SUSTAIN\\Sustain-Common\\SCCCS\\write_gis_r\\tube_database\\access_2010_versions\\no2_data.accdb;")
    )
}


connect.envista <- function() {
    con_params <-
        config::get(file = "../L4/project-1-reporting-pipeline/R/config.yml",
                    config = "envista") # credentials
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

#   Get roads data ----

make.roads.aqms.sf <- function(roads_sf, aqms_sf) {
    # get the index of roads which are nearest to the monitor sites
    index_roads <- st_nearest_feature(aqms_sf, roads_sf)
    # subset the roads data
    nearest_roads <- roads_sf[index_roads, ]
    # add the monitoring site ID as a column
    roads_aqms_sf <- nearest_roads %>%
        add_column(site_id = aqms_sf$site_id)
    
    return(roads_aqms_sf)
}

make.junctions.aqms.sf <- function(nodes_sf, aqms_sf) {
    junctions <- nodes_sf %>%
        filter(formOfNode == "junction")
    # find the nearest junction
    near_junction_index <- st_nearest_feature(aqms_sf, junctions)
    
    near_junction <- junctions[near_junction_index, ]
    # compute distance to the nearest junction
    junction_distance <-
        st_distance(aqms_sf, near_junction, by_element = TRUE)
    # make junction table with siteids and distance to nearest junction
    junction_aq_sites <- near_junction %>%
        add_column(site_id = aqms_sf$site_id,
                   distance = junction_distance)
    return(junction_aq_sites)
}

make.pcm.aqms.sf <- function(pcm_sf, aqms_sf) {
    near_pcm_index <- st_nearest_feature(aqms_sf, pcm_sf %>%
                                             select(-starts_with("RN")))
    
    nearest_pcm <- pcm_sf[near_pcm_index,]
    
    pcm_aqms_sf <- nearest_pcm %>%
        add_column(site_id = aqms_sf$site_id)
    return(pcm_aqms_sf)
}

#   Get AQ data ----

get.site.dates.tbl <- function(connect_access) {
    # get start and end dates for each site from the Access database
    site_dates_tbl <- tbl(connect_access, "data") %>%
        group_by(LocID) %>%
        summarise(
            site_start_date = min(dateOn, na.rm = TRUE),
            site_end_date = max(dateOff, na.rm = TRUE)
        ) %>%
        collect()
    return(site_dates_tbl)
}

get.raw.tubes.tbl <- function(connect_access, datestart, dateend) {
    # get the tube data from the access database
    # for the dates - mid_dates fall between start and end dates
    # group by site_id and average (for colocated sites)
    raw_tubes_tbl <- tbl(connect_access, "data") %>%
        mutate(mid_date = dateOn + as.integer((dateOff - dateOn) / 2)) %>%
        filter(between(mid_date, as.Date(datestart), as.Date(dateend))) %>%
        transmute(
            site_id = LocID,
            on_date = dateOn,
            off_date = dateOff,
            mid_date,
            no2 = concentration,
        ) %>%
        collect()
    
    return(raw_tubes_tbl)
}

get.aqms.tbl <- function(connect_access) {
    aqms_tbl <- tbl(connect_access, "locations") %>%
        collect()
    return(aqms_tbl)
}

get.envista.meta.tbl <- function() {
    #read the table which holds contin meta data for the envista database
    read_delim(file = "S:/SUSTAIN/Sustain-Common/SCCCS/write_gis_r/R Projects/air_quality_data_management/data/finalContinTablerhtemp.csv",
               delim = ",",
               col_types = "ciiciccc") %>%
        return()
}

get.no2.envista.meta.tbl <- function(final_tbl) {
    no2_envista_meta_tbl <- final_tbl %>%
        filter(minutes == 60,
               pollutant == "no2",
               siteid %in% c(203, 215, 463, 270, 501, 672)) %>%
        select(siteid, value, status, table)
    
    return(no2_envista_meta_tbl)
}

get.no2.continuous.tbl <- function(siteid,
                           value,
                           status,
                           table) {
    
    dateon <- make_datetime(
        year = year_meas,
        month = 1L,
        day = 1L,
        hour = 0L,
        min = 0L,
        sec = 0,
        tz = "UTC"
    )
    dateoff <- dateon + years(1)

    tbl(connect_envista, table) %>%
        filter(between(Date_Time, dateon, dateoff)) %>%
        select(
            date_time = Date_Time,
            value = all_of(value),
            status = all_of(status)
        ) %>%
        mutate(
            no2 = if_else(status == 1, value, NA_real_),
            value = NULL,
            status = NULL
        ) %>%
        collect() %>%
        mutate(site_id = siteid) %>% 
          return()
}


make.no2.continuous.tbl <- function(no2_envista_meta_tbl, get.no2.continuous.tbl){

return(pmap_dfr(no2_envista_meta_tbl, get.no2.continuous.tbl))

}



get.annual.dt.tbl <- function(connect_access, year_meas) {
    tbl(connect_access, "tbl_final_ba_annual") %>%
        filter(dYear == year_meas) %>%
        select(site_id = LocID,
               year = dYear,
               no2 = final_adjusted_conc) %>%
        collect()
}

#   Wrangle AQ data ----

make.monthly.contin.tbl <- function(no2_continuous_tbl, dateend){
    
    dateendPOS <- as.POSIXct(as.Date(dateend))
    
    monthly_contin_table <- no2_continuous_tbl %>% 
        filter(date_time <= dateendPOS) %>% 
        mutate(month = month(date_time, label = TRUE, abbr = FALSE)) %>% 
        pivot_wider(id_cols = site_id,
                    names_from = month,
                    values_from = no2,
                    values_fn = ~mean(.x, na.rm = TRUE))
    
    return(monthly_contin_table)
}

make.annual.contin.summary.tbl <- function(no2_continuous_tbl, datestart){
    
    dateon <- make_datetime(
        year = year(as.POSIXct(datestart)),
        month = 1L,
        day = 1L,
        hour = 0L,
        min = 0L,
        sec = 0,
        tz = "UTC"
    )
    
    dateoff <- dateon + years(1)
    
    hrs <- difftime(dateoff, dateon, units = "hours") %>%
        as.integer()

    annual_contin_summary_table <- no2_continuous_tbl %>% 
        group_by(site_id) %>% 
        na.omit(no2) %>% 
        summarise(measurement_start_date = min(date_time, na.rm = TRUE),
                  measurement_end_date = max(date_time, na.rm = TRUE) - minutes(1),
                  no2 = mean(no2, na.rm = TRUE),
                  datacap = ((n() / hrs) * 100) %>% round(1)) %>% 
        mutate(across(where(is.POSIXct), date.format))
    return(annual_contin_summary_table)
}
pivot.tubes.month <- function(raw_tubes_tbl) {
    raw_tubes_tbl %>%
        mutate(month = month(mid_date, label = TRUE, abbr = FALSE)) %>%
        pivot_wider(
            id_cols = site_id,
            names_from = month,
            values_from = no2,
            values_fn = ~mean(.x, na.rm = TRUE) %>% round(2)
        ) %>%
        return()
}

make.dt.annual.datacap.tbl <- function(raw_tubes_tbl, aqms_tbl, year_meas){
    
    if(leap_year(year_meas)) daysinyear <-  366 else daysinyear <-  365
    
    dt_annual_datacap_tbl <- raw_tubes_tbl %>% 
        right_join(aqms_tbl %>%
                       transmute(site_id = SiteID,
                                 d_t = if_else(Duplicate_Triplicate == "D", 2, 3)), 
                   by = "site_id") %>% 
        mutate(numtubes = replace_na(d_t, 1),
               days = (off_date - on_date) %>% as.integer()) %>% 
        group_by(site_id) %>% 
        summarise(dt_dc = (sum(days / numtubes, na.rm = TRUE) / daysinyear)
                  * 100 %>% 
                      round(1)) %>% 
        filter(dt_dc != 0L)
    return(dt_annual_datacap_tbl)
    
}

# create spatial DF from aqms_tbl
make.aqms.sf <- function(aqms_tbl) {
    aqms_tbl %>%
        select(site_id = SiteID, Easting, Northing) %>%
        filter(!is.na(Easting), !is.na(Northing)) %>%
        st_as_sf(coords = c("Easting", "Northing"), crs = 27700) %>%
        return()
}

make.lat.lon <- function(aqms_sf) {
    aqms_sf %>%
        st_transform(crs = 4326) %>%
        mutate(lat = st_coordinates(.)[, 2],
               lon = st_coordinates(.)[, 1]) %>%
        st_drop_geometry() %>%
        return()
}

format.site.dates_tbl <- function(site_dates_tbl) {

    site_dates_tbl %>%
        mutate(across(where(is.POSIXct), ~format(as.Date(.x), "%d/%m/%Y"))) %>%
        rename(site_id = LocID) %>%
        return(site_dates_tbl)
}

make.measurement.minmax.dates.tbl <- function(raw_tubes_tbl) {
    raw_tubes_tbl %>%
        group_by(site_id) %>%
        summarise(
            measurement_start_date = min(on_date) %>%
                format("%d/%m/%Y"),
            measurement_end_date = max(off_date) %>%
                format("%d/%m/%Y")
        ) %>%
        return()
}

make.all.sites.tbl <- function(aqms_tbl,
                               site_lat_lon_tbl,
                               roads_aqms_sf,
                               pcm_aqms_sf,
                               junctions_aqms_sf){
    
    all_sites_tbl <- aqms_tbl %>%
        clean_names() %>%
        inner_join(site_lat_lon_tbl, by = c("site_id" = "site_id")) %>%
        inner_join(
            roads_aqms_sf %>%
                select(
                    site_id,
                    road_name = name1,
                    road_number = roadNumber
                ) %>%
                st_drop_geometry(),
            by = "site_id"
        ) %>%
        inner_join(
            pcm_aqms_sf %>%
                select(site_id,
                       census_id = CENSUSID15) %>%
                st_drop_geometry(),
            by = "site_id"
        ) %>%
        inner_join(
            junctions_aqms_sf %>%
                transmute(
                    site_id,
                    junction_distance = distance %>%
                        as.integer()
                ) %>%
                st_drop_geometry(),
            by = "site_id"
        )
    
    return(all_sites_tbl)
}

make.all.sites.labelled <- function(all_sites_tbl){
    
    all_sites_tbl %>%
        transmute(
            network_id = if_else(survey != "CAZ" | is.na(survey), "LAQM", "CAZ"),
            site_id = site_id,
            site_name = location,
            local_authority = "Bristol City Council",
            site_type_laqm = laqm_locationclass,
            site_type_aqsr = map_chr(location_class, ~str_split_1(.x, " ")[2]),
            area_type = map_chr(location_class, ~str_split_1(.x, " ")[1]),
            monitoring_technique = "passive sampling",
            year_of_measurement = year_meas,
            annual_mean_no2_concentration_mg_m3 = NA_real_,
            measurement_site_latitude_decimal_degrees = lat,
            measurement_site_longitude_decimal_degrees = lon,
            census_id_if_known = census_id,
            road_name_on_which_the_site_is_located = road_name,
            road_number_on_which_the_site_is_located = road_number,
            inlet_height_m_above_ground_level = tube_height,
            inlet_positioned_away_from_emission_sources = "Y",
            distance_of_inlet_from_nearest_obstructions_i_e_buildings_balconies_trees_and_other_obstacles_m = obstruction_m,
            flow_around_inlet_free_in_arc_of_270_y_n = if_else(inlet_flow_clear, "Y", "N"),
            distance_from_major_junction_m = junction_distance,
            distance_from_kerbside_m = tube_kerb,
            measurements_representative_of_air_quality_for_a_street_segment_100_m_in_length_y_n = "N",
            annual_data_capture_percent = NA_real_,
            annulisation_site_details_continuous_analyser_name_type_of_monitor_and_approximate_distance_from_the_location_centre = NA_character_,
            sampling_method = "Passive adsorbent",
            measurement_method = "Chemiluminescent",
            sampling_time_unit = "month",
            co_located_with_continuous_analyser_y_n =
                if_else(!is.na(colocated), "Y", "N"),
            co_located_with_diffusion_tubes = "Y",
            bias_adjustment_factor = NA_real_,
            local_or_national_bias_adjustment_factor = "local"
        )
}

make.dt.period.report.tbl <- function(all_sites_labelled_tbl,
                                      measurement_minmax_tbl,
                                      formatted_site_dates_tbl,
                                      month_tubes_tbl){
    dt_period_report_tbl <- all_sites_labelled_tbl %>% 
        left_join(measurement_minmax_tbl, by = "site_id", ) %>%
        left_join(formatted_site_dates_tbl, by = "site_id") %>% 
        right_join(month_tubes_tbl, by = "site_id") %>% 
        relocate(ends_with("_date"), .after = starts_with("annu")) %>% 
        select(- co_located_with_diffusion_tubes,
               - measurement_method)
    return(dt_period_report_tbl)
}

make.dt.annual.report.tbl <- function(dt_period_report_tbl,
                                      annual_dt_tbl,
                                      dt_annual_datacap_tbl){
    dt_annual_report_tbl <- dt_period_report_tbl %>%
        right_join(annual_dt_tbl %>% select(site_id, no2), by = "site_id") %>%
        right_join(dt_annual_datacap_tbl, by = "site_id") %>% 
        mutate(annual_mean_no2_concentration_mg_m3 = no2,
               annual_data_capture_percent = dt_dc,
               no2 = NULL,
               dt_dc = NULL,
               bias_adjustment_factor = {{bias_adjustment_factor}})
    return(dt_annual_report_tbl)
}

make.contin.site.start.end.tbl <- function(all_sites_tbl, annual_contin_summary_tbl){
    
    contin_site_start_end_tbl <- all_sites_tbl %>% 
        inner_join(annual_contin_summary_tbl, by = "site_id") %>% 
        transmute(site_id,
                  site_start = date.format(date_start),
                  site_end = date.format(date_end))
    return(contin_site_start_end_tbl)
}

make.contin.period.report.tbl <- function(all_sites_labelled_tbl,
                                          contin_site_start_end_tbl,
                                          monthly_contin_tbl){
    
    contin_period_report_tbl <- all_sites_labelled_tbl %>% 
        right_join(contin_site_start_end_tbl, by = "site_id") %>% 
        inner_join(monthly_contin_tbl, by = "site_id") %>% 
        mutate(monitoring_technique = "Automatic analyser",
               sampling_time_unit = "Hour",
               sampling_method = NULL,
               co_located_with_continuous_analyser_y_n = NULL,
               measurement_start_date = NA_character_,
               measurement_end_date = NA_character_,
               bias_adjustment_factor = NULL,
               local_or_national_bias_adjustment_factor = NULL
        ) %>% 
        relocate(ends_with("_date"), .after = starts_with("annu"))
    
    return(contin_period_report_tbl)
    
}

make.contin.annual.report.tbl <- function(all_sites_labelled_tbl,
                                          contin_site_start_end_tbl,
                                          annual_contin_summary_tbl,
                                          monthly_contin_tbl){
    
    contin_annual_report_tbl <- all_sites_labelled_tbl %>% 
        right_join(contin_site_start_end_tbl, by = "site_id") %>% 
        inner_join(annual_contin_summary_tbl, by = "site_id") %>% 
        inner_join(monthly_contin_tbl %>% 
                       select(site_id, starts_with("measurement")),
                   by = "site_id") %>% 
        mutate(monitoring_technique = "Automatic analyser",
               sampling_time_unit = "Hour",
               sampling_method = NULL,
               co_located_with_continuous_analyser_y_n = NULL,
               annual_mean_no2_concentration_mg_m3 = no2,
               annual_data_capture_percent = datacap,
               datacap = NULL,
               no2 = NULL,
               bias_adjustment_factor = NULL,
               local_or_national_bias_adjustment_factor = NULL
               
        ) %>% 
        relocate(ends_with("_date"), .after = starts_with("annu"))
    
    return(contin_annual_report_tbl)
    
}

# 
# get.xl.labels.names <- function(path = "data/Annex 1 Air Quality Monitoring Reporting Template (1).xlsx",
#                                 instrument_type = c("DT", "continuous")) {
#     range = if_else(instrument_type == "DT",
#                     "B6:AT7",
#                     "B15:AR16")
#     
#     dt_template <- read_excel(path = path,
#                               sheet = "i) Monitoring Checklist",
#                               range = range)
#     
#     labels <- names(dt_template)
#     
#     names <- make_clean_names(labels)
#     return(list("labels" = labels,
#                 "names" = names))
# }
# 
# make.empty.report.tbl <-
#     function(names_labels_list = names_labels_dt_list) {
#         names <- names_labels_list$names
#         empty_dt_tbl = data.frame(matrix(nrow = 0,
#                                          ncol = length(names))) %>%
#             as_tibble() %>%
#             set_names(names)
#         
#     }

