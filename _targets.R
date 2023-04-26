# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline # nolint

# Load packages required to define the pipeline:
# library(targets)
library("targets")

      
# library(tarchetypes) # Load other packages as needed. # nolint

# Set target options:
tar_option_set(
  packages = c( "tidyverse",
                "glue",
                "janitor",
                "labelled",
                "lubridate",
                "sf",
                "readxl"), # packages that your targets need to run
  format = "rds" # default storage format
  # Set other options as needed.
)

# tar_make_clustermq() configuration (okay to leave alone):
options(clustermq.scheduler = "multiprocess")

# tar_make_future() configuration (okay to leave alone):
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# source("other_functions.R") # Source other scripts as needed. # nolint

list(
  tar_target(
    name = roads_sf,
    command = st_read("data/ST_RoadLink.shp")
  ),
  tar_target(
    name = pcm_sf,
    command = st_read("data/clippedpcm.shp", crs = 27700)
  ),
  tar_target(
      name = nodes_sf,
      command = st_read("data/ST_RoadNode.shp")
  ),
  tar_target(
      name = annual_dt_tbl,
      command = get.annual.dt.tbl(connect_access, year_meas)
  ),
  tar_target(
      name = aqms_tbl,
      command = get.aqms.tbl(connect_access)
  ),
  tar_target(
      name = raw_tubes_tbl,
      command = get.raw.tubes.tbl(connect_access, datestart, dateend)
  ),
  tar_target(
      name = site_dates_tbl,
      command = get.site.dates.tbl(connect_access)
  ),
  tar_target(
      name = envista_meta_tbl,
      command = get.envista.meta.tbl()
  ),
  tar_target(
      name = no2_envista_meta_tbl,
      command = get.no2.envista.meta.tbl(envista_meta_tbl)
  ),
  tar_target(
      name = no2_continuous_tbl,
      command = make.no2.continuous.tbl(no2_envista_meta_tbl,
                                        get.no2.continuous.tbl)
  ),

  tar_target(
      name = monthly_contin_tbl,
      command = make.monthly.contin.tbl(no2_continuous_tbl, dateend)
  ),
  tar_target(
      name = annual_contin_summary_tbl,
      command = make.annual.contin.summary.tbl(no2_continuous_tbl, datestart)
  ),
  tar_target(
      name = aqms_sf,
      command =  make.aqms.sf(aqms_tbl)
  ),
  tar_target(
      name = site_lat_lon_tbl,
      command = make.lat.lon(aqms_sf)
  ),
  tar_target(
      name = measurement_minmax_tbl,
      command = make.measurement.minmax.dates.tbl(raw_tubes_tbl)
  ),
  tar_target(
      name = month_tubes_tbl,
      command = pivot.tubes.month(raw_tubes_tbl)
  ),
  tar_target(
      name = dt_annual_datacap_tbl,
      command = make.dt.annual.datacap.tbl(raw_tubes_tbl, aqms_tbl, datestart)
  ),
  tar_target(
      name = formatted_site_dates_tbl,
      command = format.site.dates_tbl(site_dates_tbl)
  ),
  tar_target(
      name = roads_aqms_sf,
      command = make.roads.aqms.sf(roads_sf, aqms_sf)
  ),
  tar_target(
      name = junctions_aqms_sf,
      command = make.junctions.aqms.sf(nodes_sf, aqms_sf)
  ),
  tar_target(
      name = pcm_aqms_sf,
      command = make.pcm.aqms.sf(pcm_sf, aqms_sf)
  ),
  tar_target(
      name = all_sites_tbl,
      command = make.all.sites.tbl(aqms_tbl,
                                   site_lat_lon_tbl,
                                   roads_aqms_sf,
                                   pcm_aqms_sf,
                                   junctions_aqms_sf)
  ),
  tar_target(
      name = all_sites_labelled_tbl,
      command = make.all.sites.labelled(all_sites_tbl,
                                        make.anualisation.text,
                                        annsites,
                                        annual_tubes)
  ),
  tar_target(
      name = dt_period_report_tbl,
      command = make.dt.period.report.tbl(all_sites_labelled_tbl,
                                          measurement_minmax_tbl,
                                          formatted_site_dates_tbl,
                                          month_tubes_tbl)
  ),
  tar_target(
      name = dt_annual_report_tbl,
      command = make.dt.annual.report.tbl(dt_period_report_tbl,
                                annual_dt_tbl,
                                dt_annual_datacap_tbl)
  ),
  tar_target(
      name = contin_site_start_end_tbl,
      command =  make.contin.site.start.end.tbl(all_sites_tbl,
                                                annual_contin_summary_tbl)
  ),
  tar_target(
      name = contin_period_report_tbl,
      command = make.contin.period.report.tbl(all_sites_labelled_tbl,
                                              contin_site_start_end_tbl,
                                              monthly_contin_tbl)
  ),
  tar_target(
      name = contin_annual_report_tbl,
      command = make.contin.annual.report.tbl(all_sites_labelled_tbl,
                                        contin_site_start_end_tbl,
                                        annual_contin_summary_tbl,
                                        monthly_contin_tbl)
  ),
   tar_target(
      name = write_period_dt_report,
      command = if(!is_annual){write.output.report.csv(
          dt_period_report_tbl,
          datestart,
          dateend, 
          is_annual)},
      format = "file"
  ),
  tar_target(
      name = write_dt_annual_report,
      command = if(is_annual){write.output.report.csv(
          dt_annual_report_tbl,
          datestart,
          dateend,
          is_annual)},
      format = "file"
  ),  tar_target(
      name = write_contin_period_report,
      command = if(!is_annual){write.output.report.csv(
          contin_period_report_tbl,
          datestart,
          dateend,
          is_annual)},
      format = "file"
  ),
  tar_target(
      name = write_contin_annual_report,
      command = if(is_annual){write.output.report.csv(
          contin_annual_report_tbl,
          datestart,
          dateend,
          is_annual)},
      format = "file"
  )
  
)
