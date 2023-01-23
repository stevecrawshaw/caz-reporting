# ALL IN FUNCTIONS>R NOW

p <- c("tidyverse", "sf", "mapview", "mapsf", "glue", "janitor", "labelled")
library("pacman")
p_load(char = p)



pcm %>% glimpse()



# NODES ----

# filter for junctions


# PCM ----

near_pcm_index <- st_nearest_feature(monitors, pcm)

nearest_pcm <- pcm[near_pcm_index, ]

pcm_sites <- nearest_pcm %>% 
    add_column(siteid = monitors$siteid)

roads_junction_pcm_list <- list(
    roads_aq_sites,
    junction_aq_sites,
    pcm_sites
)

write_rds(roads_junction_pcm_list, "data/roads_junction_pcm.rds")
