# TESTING -----

# Variables ----
packages = c( "tidyverse",
              "glue",
              "janitor",
              "labelled",
              "lubridate",
              "DBI",
              "odbc",
              "sf",
              "readxl",
              "config")

pacman::p_load(char = packages)

# Data ----
#   Road data ----

# read the road links 

roads_sf <- st_read("data/ST_RoadLink.shp")

# read the monitors make.aqms.sf

# read the pcm links
pcm_sf <- st_read("data/clippedpcm.shp", crs = 27700) 

# get the nodes
nodes_sf <- st_read("data/ST_RoadNode.shp")

# Get tube and site data ----

connect_access <- connect.access()

aqms_tbl <- get.aqms.tbl(connect_access)
raw_tubes_tbl <- get.raw.tubes.tbl(connect_access, datestart, dateend)
site_dates_tbl <- get.site.dates.tbl(connect_access)
annual_dt_tbl <- get.annual.dt.tbl(connect_access, year_meas)


dbDisconnect(connect_access)

# Get contin data ----

# The next bit - collecting continuous summary data is only done if is.annual
connect_envista = connect.envista()

envista_meta_tbl <- get.envista.meta.tbl()
no2_envista_meta_tbl <- get.no2.envista.meta.tbl(envista_meta_tbl)

no2_continuous_tbl <- pmap_dfr(no2_envista_meta_tbl, get.no2.continuous.tbl)
dbDisconnect(connect_envista)

monthly_contin_tbl <- make.monthly.contin.tbl(no2_continuous_tbl, dateend)

annual_contin_summary_tbl <- make.annual.contin.summary.tbl(no2_continuous_tbl,
                                                            datestart)


# wrangle the AQ data from sites and tubes ----

aqms_sf <- make.aqms.sf(aqms_tbl)

site_lat_lon_tbl <- make.lat.lon(aqms_sf)

measurement_minmax_tbl <-
    make.measurement.minmax.dates.tbl(raw_tubes_tbl)

month_tubes_tbl <- pivot.tubes.month(raw_tubes_tbl)


formatted_site_dates_tbl <- format.site.dates_tbl(site_dates_tbl)

roads_aqms_sf <- make.roads.aqms.sf(roads_sf, aqms_sf)

junctions_aqms_sf <- make.junctions.aqms.sf(nodes_sf, aqms_sf)

pcm_aqms_sf <- make.pcm.aqms.sf(pcm_sf, aqms_sf)

# may not need this stuff **didn't include in targets----
names_labels_dt_list <- get.xl.labels.names(instrument_type = "DT")

names_labels_continuous_list <-
    get.xl.labels.names(instrument_type = "continuous")

empty_report_dt_tbl <- make.empty.report.tbl(names_labels_dt_list)
###

# assemble reports ----

all_sites_tbl <- make.all.sites.tbl(aqms_tbl,
                                    site_lat_long_tbl,
                                    roads_aqms_sf,
                                    pcm_aqms_sf,
                                    junctions_aqms_sf)



all_sites_labelled_tbl <- make.all.sites.labelled(all_sites_tbl)

# this is the quarterly diffusion tube report
# use cumulative data, i.e. start date should always be YYYY-01-01
dt_period_report_tbl  <- make.dt.period.report.tbl(all_sites_labelled_tbl,
                                                   measurement_minmax_tbl,
                                                   formatted_site_dates_tbl,
                                                   month_tubes_tbl)

dt_annual_datacap_tbl <- make.dt.annual.datacap.tbl(raw_tubes_tbl, year)

dt_annual_report_tbl <- make.dt.annual.report.tbl(dt_period_report_tbl,
                                                  annual_dt_tbl,
                                                  dt_annual_datacap_tbl)

contin_site_start_end_tbl <- make.contin.site.start.end.tbl(all_sites_tbl)

contin_period_report_tbl <- make.contin.period.report.tbl(all_sites_labelled_tbl,
                                                          contin_site_start_end_tbl,
                                                          monthly_contin_tbl)

contin_annual_report_tbl <- 
    make.contin.annual.report.tbl(all_sites_labelled_tbl,
                                  contin_site_start_end_tbl,
                                  annual_contin_summary_tbl)
