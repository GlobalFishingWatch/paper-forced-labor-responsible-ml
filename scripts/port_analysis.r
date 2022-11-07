# This file generates port analysis figures and statistics for the paper

# Load packages
library(bigrquery)
library(readr)
library(dplyr)
library(tidyr)
library(rworldmap)
library(ggplot2)
library(sf)
library(countrycode)
library(circlize)
# Deal with BQ data download error
# See for reference: https://github.com/r-dbi/bigrquery/issues/395
options(scipen = 20)

# Set up BQ project
gfw_project <- "world-fishing-827"
# Gavin is currently using the emlab-gcp project, since I'm not set up to bill to world-fishing-827
# but you may eventually want to set up everything to bill world-fishing-827
emlab_project <- "emlab-gcp"

# Define directory where SQL queries live
query_path <- "./queries/"

# Load query to generate voyage/port visit info, by vessel and voyage
all_voyages_by_ssvid_query <- read_file(paste0(query_path,"all_voyages_by_ssvid.sql"))

# Run this query and store on BQ
bq_project_query(x = emlab_project, query = all_voyages_by_ssvid_query,
                 destination_table = bq_table(project = gfw_project,
                                              table = "all_voyages_by_ssvid",
                                              dataset = "prj_forced_labor"),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")

# Load query to generate voyage summary by from and to port/anchorage and by forced labor class predictions
# We use `world-fishing-827.prj_forced_labor.pred_stats_per_vessel_year_dev_2021` to get class predictions
# But you may eventually want to changet it to ensure the best prediction table is being used
voyages_with_predictions_query <- read_file(paste0(query_path,"voyages_with_predictions.sql"))

# Run this query and store on BQ
bq_project_query(x = emlab_project, query = voyages_with_predictions_query,
                 destination_table = bq_table(project = gfw_project,
                                              table = "voyages_with_predictions",
                                              dataset = "prj_forced_labor"),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")


# Cache anchorage data to repo for plotting
bq_project_query(emlab_project, "SELECT * FROM `world-fishing-827.prj_forced_labor.voyages_with_predictions`") %>%
  bq_table_download(n_max = Inf) %>%
  write_csv(file=here::here("data","voyages_with_predictions.csv"))

# Load cached data
voyage_summary_with_predictions <- read_csv(here::here("data","voyages_with_predictions.csv")) %>%
  # Only want last year of predictions, 2020
  filter(year == 2020) %>%
  # Recode class
  mutate(class_mode = ifelse(class_mode == 1, "Positive","Negative"))


# How many countries did positive vessels have port visits in?
# Add up distinct number of from-countries and to-countries
number_countries_visited_by_positive <- voyage_summary_with_predictions %>%
  filter(class_mode == "Positive") %>%
  dplyr::select(country = from_country) %>%
  bind_rows(voyage_summary_with_predictions %>%
              filter(class_mode == "Positive") %>%
              dplyr::select(country = to_country)) %>%
  distinct(country) %>%
  nrow()

number_countries_visited_by_positive

# How many countries did any vessels have port visits in, regardless of predicted class?
number_countries_visited <- voyage_summary_with_predictions %>%
  dplyr::select(country = from_country) %>%
  bind_rows(voyage_summary_with_predictions %>%
              dplyr::select(country = to_country)) %>%
  distinct(country) %>%
  nrow()

number_countries_visited

# What fraction of visited countries had positive port visits?
scales::percent(number_countries_visited_by_positive / number_countries_visited)

# Summarize number of potyd visited by positive vessels
# Include from-and-to anchorages
# Just use main "label" classification for port - not "sublabel", which corresponds to specific anchorages within a port
distinct_positive_ports <- c((voyage_summary_with_predictions %>%
  filter(class_mode == "Positive") %>%
    .$from_anchorage_label),
  (voyage_summary_with_predictions %>%
     filter(class_mode == "Positive") %>%
     .$to_anchorage_label)) %>%
  unique() %>%
  length()

distinct_positive_ports

# Summarize number of ports visited by any vessels
# Include from-and-to ports
# Just use main "label" classification for port - not "sublabel", which corresponds to specific anchorages within a port
distinct_ports <- c((voyage_summary_with_predictions %>%
                                     .$from_anchorage_label),
                                  (voyage_summary_with_predictions %>%
                                     .$to_anchorage_label)) %>%
  unique() %>%
  length()

scales::comma(distinct_ports)

# Fraction anchorages visited by positive vessels:
scales::percent(distinct_positive_ports / distinct_ports)

# Summarize number of positive country-to-country voyages
country_summary <- voyage_summary_with_predictions %>%
  group_by(from_country,to_country,class_mode) %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  ungroup() %>%
  filter(class_mode == "Positive")

# How many voyages happened within a single country?
positive_voyages_within_country <- country_summary %>%
  filter(from_country == to_country) %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  .$number_voyages

scales::comma(positive_voyages_within_country)

# How many voyages happened in total?
total_positive_voyages <- country_summary %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  .$number_voyages

# What fraction of positive voyages occurred within a single country?
scales::percent(positive_voyages_within_country / total_positive_voyages)

# What fraction of positive voyages occurred between different country?
scales::percent(1 - positive_voyages_within_country / total_positive_voyages_within_country)

# Set mapping projection
map_projection <- "+proj=eqearth +datum=WGS84 +wktext"

# Create global land sf object for mapping
world_plotting <- getMap(resolution = "low") %>%
  st_as_sf() %>%
  st_transform(map_projection)

# Summarize number of positive voyages by from_anchorage_label
voyages_with_predictions_from <- voyage_summary_with_predictions %>%
  filter(class_mode == "Positive") %>%
  dplyr::select(anchorage_label = from_anchorage_label,
                anchorage_sublabel  = from_anchorage_sublabel,
                lat = from_lat,
                lon = from_lon,
                country = from_country,
                number_voyages)

# Summarize number of positive voyages by to_anchorage_label
voyages_with_predictions_to <- voyage_summary_with_predictions %>%
  filter(class_mode == "Positive")%>%
  dplyr::select(anchorage_label = to_anchorage_label,
                anchorage_sublabel  = to_anchorage_sublabel,
                lat = to_lat,
                lon = to_lon,
                country = to_country,
                number_voyages)

# Combine from_anchorage_label visits and to_anchorage_label visits,
# Then summarize number of voyages, regardless of whether port from from or to
ports_with_predictions_combined <- voyages_with_predictions_from %>%
  bind_rows(voyages_with_predictions_to) %>%
  group_by(anchorage_label, country,lat, lon) %>%
  summarize(number_voyages = sum(number_voyages)) %>%
  ungroup() %>%
  # Convert country code to iso3, for consistency
  mutate(country = countrycode(country,
                               origin = "iso3c",
                               destination = "country.name")) %>%
  # Give anchorage a meaningful name, combining anchorage_label and country
  mutate(anchorage_label = stringr::str_to_sentence(anchorage_label)) %>%
  mutate(anchorage_name = glue::glue("{anchorage_label} ({country})")) %>%
  dplyr::select(-c(anchorage_label,country))

ports_with_predictions_combined_summary <- ports_with_predictions_combined %>%
  group_by(anchorage_name) %>%
  summarize(number_voyages = sum(number_voyages)) %>%
  ungroup()%>%
  arrange(-number_voyages)


# Select the top 5 ports in terms of positive voyage visits
top_10_ports_with_positives <- ports_with_predictions_combined_summary %>%
  slice(1:10)

# Make a nice looking table of top 5 ports
top_10_ports_with_positives %>%
  mutate(number_voyages = prettyNum(number_voyages, big.mark = ",")) %>%
  rename(Port = anchorage_name,
         `Number port visits` = number_voyages) %>%
  knitr::kable()%>%
  kableExtra::kable_styling()

top_10_ports_with_positives$anchorage_name %>%
  paste(collapse = ", ")

# For each port, find the centroid of all lat/lon locations within the anchorage
# Some have multiple anchorages within a port
ports_with_predictions_combined_centroids <- anchorages_with_predictions_combined %>%
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) %>%
  group_by(anchorage_name) %>%
  summarise(st_union(geometry)) %>%
  st_centroid() %>%
  # Add summary info for number of positive visits per port
  left_join(anchorages_with_predictions_combined_summary, by = "anchorage_name") %>%
  st_transform(st_crs(world_plotting))

# Create map of port visits, filling each country based on number
# of port visits that were by positive vessels in 2020
# I think fraction is more informative, but including this one in case you like it
port_visits_number_by_port_map <- ggplot() +
  geom_sf(data = world_plotting,
          size = 0.1,
          color = "grey50",
          fill = "grey70") +
  geom_sf(data = anchorages_with_predictions_combined_centroids,
          aes(size = number_voyages),
          fill = NA,
          shape = 21,
          stroke = 0.5) +
  theme_minimal() +
  theme(panel.grid.major = element_blank()) +
  scale_size_continuous("Number\nof positive\nport visits",
                        range = c(0.1,7),
                        labels = scales::comma)

port_visits_number_by_port_map

# Save map as figure for paper
ggsave(here::here("figures","model_paper_port_visits_map.png"),
       port_visits_number_by_port_map,
       width=7,height=3,device="png",dpi=300, bg = 'white')


# Summarize port-to-port voyages, by country, by positively classified vessels
# Convert countries to World Bank 23 regions
# Summarize voyages by region
region_summary <- voyage_summary_with_predictions %>%
  mutate(across(c("from_country","to_country"), ~countrycode(.,"iso3c","region23"))) %>%
  group_by(from_country,to_country,class_mode) %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  ungroup() %>%
  dplyr::select(from = from_country,
                to = to_country,
                value = number_voyages,
                class_mode) %>%
  filter(class_mode == "Positive") %>%
  mutate(same_region = ifelse(from == to,"same","different"))

# Summarize total number of voyages
total_voyages <- region_summary %>%
  .$value %>%
  sum()

# Summarize total number of voyages to different regions
total_voyages_same_regions <- region_summary %>%
  filter(from ==to ) %>%
  .$value %>%
  sum()

# Fraction of voyages to different regions
scales::percent(1 - total_voyages_same_regions/total_voyages)
