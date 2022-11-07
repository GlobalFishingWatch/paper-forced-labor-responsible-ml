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

# Load query to generate voyage summary by from and to country and by forced labor class predictions
# We use `world-fishing-827.prj_forced_labor.pred_stats_per_vessel_year_dev_2021` to get class predictions
# But you may eventually want to changet it to ensure the best prediction table is being used
voyage_summary_with_predictions_query <- read_file(paste0(query_path,"voyage_summary_with_predictions.sql"))

# Run this query and store on BQ
bq_project_query(x = emlab_project, query = voyage_summary_with_predictions_query,
                 destination_table = bq_table(project = gfw_project,
                                              table = "voyage_summary_with_predictions",
                                              dataset = "prj_forced_labor"),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")

# Load query to generate voyage summary by from and to port and by forced labor class predictions
# We use `world-fishing-827.prj_forced_labor.pred_stats_per_vessel_year_dev_2021` to get class predictions
# But you may eventually want to changet it to ensure the best prediction table is being used
voyage_summary_with_predictions_by_port_query <- read_file(paste0(query_path,"voyage_summary_with_predictions_by_port.sql"))

# Run this query and store on BQ
bq_project_query(x = emlab_project, query = voyage_summary_with_predictions_by_port_query,
                 destination_table = bq_table(project = gfw_project,
                                              table = "voyage_summary_with_predictions_by_port",
                                              dataset = "prj_forced_labor"),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")



# Cache voyage data to repo for plotting
bq_project_query(emlab_project, "SELECT * FROM `world-fishing-827.prj_forced_labor.voyage_summary_with_predictions`") %>%
  bq_table_download(n_max = Inf) %>%
  write_csv(file=here::here("data","voyage_summary_with_predictions.csv"))

# Load query to summarize the number of anchorages visited by year and class prediction
anchorage_summary_with_predictions_query <-read_file(paste0(query_path,"/anchorage_summary_with_predictions.sql"))

# Cache anchorage data to repo for summary stats
bq_project_query(emlab_project, anchorage_summary_with_predictions_query) %>%
  bq_table_download(n_max = Inf) %>%
  write_csv(file=here::here("data","anchorage_summary_with_predictions.csv"))

# Load cached data
voyage_summary_with_predictions <- read_csv(here::here("data","voyage_summary_with_predictions.csv")) %>%
  # Don't include voyages that don't have a from or to country - we only will plots ones that do
  filter(!(is.na(from_country) & is.na(to_country))) %>%
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
number_countries_visited_by_positive / number_countries_visited

# What fraction of positive voyages happen within a single country?
voyage_summary_with_predictions %>%
  filter(from_country == to_country) %>%
  filter(class_mode == "Positive") %>%
  .$number_voyages %>%
  sum() / (voyage_summary_with_predictions %>%
             filter(class_mode == "Positive") %>%
             .$number_voyages %>%
             sum())

# Load in anchorage summary data
anchorage_summary_with_predictions <- read_csv(here::here("data","anchorage_summary_with_predictions.csv")) %>%
  # Again, we only focus on 2020, the latest year of predictions
  filter(year == 2020)%>%
  # Recode class
  mutate(class_mode = case_when(class_mode == 1 ~  "Positive",
                                class_mode == 0 ~ "Negative",
                                class_mode == "All visits" ~ "All visits"))

anchorage_summary_with_predictions

# What fraction of anchorages were visited by positive vessels?
anchorage_summary_with_predictions %>%
  filter(class_mode == "Positive") %>%
  .$unique_anchorages / (
    anchorage_summary_with_predictions %>%
      filter(class_mode == "All visits") %>%
      .$unique_anchorages
  )


# Summarize total number of port visits by country and predicted class
# then pivot wider and calculate number of visits by positive vessels and
# fraction of visits by positive vessels
# Include both from-countries and to-countries
port_visit_summary <- voyage_summary_with_predictions %>%
  dplyr::select(country = from_country,
                port_visits = number_voyages,
                class_mode) %>%
  bind_rows(voyage_summary_with_predictions %>%
              dplyr::select(country = to_country,
                            port_visits = number_voyages,
                            class_mode)) %>%
  group_by(country,class_mode) %>%
  summarize(total_port_visits = sum(port_visits, na.rm=TRUE)) %>%
  ungroup() %>%
  pivot_wider(names_from = "class_mode",
              values_from = "total_port_visits") %>%
  # Replace missing values with 0s
  mutate(across(c("Positive","Negative"),~replace_na(.,0))) %>%
  mutate(total_voyages = Positive + Negative,
         fraction_positive = Positive / total_voyages) %>%
  # Convert country code to iso3, for consistency
  mutate(iso3 = countrycode(country,
                            origin = "iso3c",
                            destination = "iso3c"))

# Summarize number of positive country-to-country voyages
country_summary <- voyage_summary_with_predictions %>%
  group_by(from_country,to_country,class_mode) %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  ungroup() %>%
  dplyr::select(from = from_country,
                to = to_country,
                value = number_voyages,
                class_mode) %>%
  filter(!is.na(from),
         !is.na(to)) %>%
  filter(class_mode == "Positive")

# How many voyages happened within a single country?
positive_voyages_within_country <- country_summary %>%
  filter(from == to) %>%
  summarize(value = sum(value,na.rm=TRUE)) %>%
  .$value

# How many voyages happened in total?
total_positive_voyages_within_country<- country_summary %>%
  summarize(value = sum(value,na.rm=TRUE)) %>%
  .$value

# What fraction of positive voyages occurred within a single country?
positive_voyages_within_country / total_positive_voyages_within_country


# Set mapping projection
map_projection <- "+proj=eqearth +datum=WGS84 +wktext"

# Create global land sf object for mapping
world_plotting <- getMap(resolution = "low") %>%
  st_as_sf() %>%
  st_transform(map_projection)  %>%
  # Convert country code to iso3, for consistency
  mutate(iso3 = countrycode(ISO3,
                            origin = "iso3c",
                            destination = "iso3c"))%>%
  dplyr::select(iso3)

# Create map of port visits, filling each country based on fraction
# of port visits that were by positive vessels in 2020
port_visits_percentage_map <- ggplot() +
  geom_sf(data = world_plotting %>%
            left_join(port_visit_summary,
                      by = "iso3"),
          aes(fill = fraction_positive),
          size = 0.1,
          color = "black") +
  scale_fill_viridis_c(name = "Positive\nport visits\n(%)",
                       labels = scales::percent,
                       option = "B",
                       na.value = "grey70") +
  theme_minimal() +
  theme(panel.grid.major = element_blank())

port_visits_percentage_map

# Save map as figure for paper
ggsave(here::here("figures","model_paper_port_visits_percentage_map.png"),
       port_visits_percentage_map,
       width=7,height=3,device="png",dpi=300)

# Create map of port visits, filling each country based on number
# of port visits that were by positive vessels in 2020
# I think fraction is more informative, but including this one in case you like it
port_visits_number_map <- ggplot() +
  geom_sf(data = world_plotting %>%
            left_join(port_visit_summary,
                      by = "iso3"),
          aes(fill = Positive),
          size = 0.1,
          color = "black") +
  scale_fill_viridis_c(name = "Positive\nport visits\n(number)",
                       labels = scales::comma,
                       option = "B",
                       na.value = "grey70") +
  theme_minimal() +
  theme(panel.grid.major = element_blank())

port_visits_number_map

# Save map as figure for paper
ggsave(here::here("figures","model_paper_port_visits_number_map.png"),
       port_visits_number_map,
       width=7,height=3,device="png",dpi=300)

# Next we want to make a chord diagram
# summarizing port-to-port voyages, by country, by positively classified vessels
# Convert countries to World Bank 23 regions
# Summarize voyages by region
# Convert to DF that circlize can use
circlize_df_all <- voyage_summary_with_predictions %>%
  mutate(across(c("from_country","to_country"), ~countrycode(.,"iso3c","region23"))) %>%
  group_by(from_country,to_country,class_mode) %>%
  summarize(number_voyages = sum(number_voyages,na.rm=TRUE)) %>%
  ungroup() %>%
  dplyr::select(from = from_country,
                to = to_country,
                value = number_voyages,
                class_mode) %>%
  filter(!is.na(from),
         !is.na(to))

circlize_df_all_table <- circlize_df_all %>%
  filter(class_mode == "Positive") %>%
  mutate(same_region = ifelse(from == to,"same","different")) %>%
  group_by(from,same_region) %>%
  summarize(number_voyages = sum(value)) %>%
  ungroup() %>%
  pivot_wider(names_from = "same_region",
              values_from = "number_voyages") %>%
  mutate(total = different+same) %>%
  arrange(-total) %>%
  mutate(same = glue::glue("{prettyNum(same,big.mark=',')} ({scales::percent(same/total, accuracy = 1)})"),
         different = glue::glue("{prettyNum(different,big.mark=',')} ({scales::percent(different/total, accuracy = 1)})"),
         total = prettyNum(total, big.mark=','),
  ) %>%
  dplyr::select(Region = from,
         `Total trips` = total,
         `Within-region trips` = same,
         `Outside-region trips` = different)

circlize_df_all_table %>%
  knitr::kable()%>%
  kableExtra::kable_styling()

# For chord diagram, just include positives
circlize_df <- circlize_df_all %>%
  filter(class_mode == "Positive")


# What fraction of positive voyages happen within a single region?
# First calculate the number of positive voyages that happen within a single region
positive_regional_voyages <- circlize_df_all %>%
  filter(from == to) %>%
  filter(class_mode == "Positive") %>%
  .$value %>%
  sum()

# Then calculate the total number of positive voyages
all_regional_voyages <- circlize_df_all %>%
  filter(class_mode == "Positive") %>%
  .$value %>%
  sum()

# Then we can calculate the fraction
positive_regional_voyages/all_regional_voyages

# Now we make the chord diagram
# The code was adapted from here: https://www.data-to-viz.com/graph/chord.html

# Set a seed, to keep coloring consistent
set.seed(101)
my_colors <- viridis::viridis(length(unique(circlize_df$from)),
                              alpha = 1,
                              begin = 0,
                              end = 1,
                              option = "D")[sample(1:length(unique(circlize_df$from)))]

# Open PNG where we'll save the figure
png(here::here("figures","model_paper_positive_voyage_chord_diagram.png"),
    res = 300,
    width = 8,
    height = 8,
    units = "in")

# Create the figure

# First we make the chords without any text
chordDiagram(
  x = circlize_df,
  grid.col = my_colors,
  transparency = 0.25,
  directional = 1,
  direction.type = c("arrows", "diffHeight"),
  diffHeight  = -0.04,
  annotationTrack = "grid",
  annotationTrackHeight = c(0.05, 0.1),
  link.arr.type = "big.arrow",
  link.sort = TRUE,
  link.largest.ontop = TRUE,
  preAllocateTracks = list(
    track.height = mm_h(4),
    track.margin = c(mm_h(20), 0)
  ))

# Then we add text to label the regions
circos.trackPlotRegion(track.index = 1, panel.fun = function(x, y) {
  xlim = get.cell.meta.data("xlim")
  ylim = get.cell.meta.data("ylim")
  sector.name = get.cell.meta.data("sector.index")
  circos.text(mean(xlim), ylim[1]-5, cex = 0.9,sector.name, facing = "clockwise", niceFacing = TRUE, adj = c(0, 0.5))
}, bg.border = NA)

# Now close the PNG so the figure saves
dev.off()

# Load query to generate anchorage summary by from and to port and by forced labor class predictions
# We use `world-fishing-827.prj_forced_labor.pred_stats_per_vessel_year_dev_2021` to get class predictions
# But you may eventually want to changet it to ensure the best prediction table is being used
anchorages_with_predictions_query <- read_file(paste0(query_path,"anchorages_with_predictions.sql"))

# Run this query and store on BQ
bq_project_query(x = emlab_project, query = anchorages_with_predictions_query,
                 destination_table = bq_table(project = gfw_project,
                                              table = "anchorages_with_predictions",
                                              dataset = "prj_forced_labor"),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")


# Cache anchorage data to repo for plotting
bq_project_query(emlab_project, "SELECT * FROM `world-fishing-827.prj_forced_labor.anchorages_with_predictions`") %>%
  bq_table_download(n_max = Inf) %>%
  write_csv(file=here::here("data","anchorages_with_predictions.csv"))

# Load cached data
anchorages_with_predictions <- read_csv(here::here("data","anchorages_with_predictions.csv")) %>%
  # Only want last year of predictions, 2020
  filter(year == 2020) %>%
  # Only care above positives
  filter(class_mode ==1)

# Summarize number of voyages by from_anchorage
anchorages_with_predictions_from <- anchorages_with_predictions %>%
  dplyr::select(anchorage_label = from_anchorage_label,
                anchorage_sublabel  = from_anchorage_sublabel,
                lat = from_lat,
                lon = from_lon,
                country = from_country,
                number_voyages)

# Summarize number of voyages by to_anchorage
anchorages_with_predictions_to <- anchorages_with_predictions %>%
  dplyr::select(anchorage_label = to_anchorage_label,
                anchorage_sublabel  = to_anchorage_sublabel,
                lat = to_lat,
                lon = to_lon,
                country = to_country,
                number_voyages)

# Combine from_anchorage visits and to_anchorage visits,
# Then summarize number of voyages, regardless of whether port from from or to
anchorages_with_predictions_combined <- anchorages_with_predictions_from %>%
  bind_rows(anchorages_with_predictions_to) %>%
  group_by(anchorage_label, anchorage_sublabel, country,lat, lon) %>%
  summarize(number_voyages = sum(number_voyages)) %>%
  ungroup() %>%
  # Convert country code to iso3, for consistency
  mutate(country = countrycode(country,
                               origin = "iso3c",
                               destination = "country.name")) %>%
  mutate(anchorage_label = stringr::str_to_sentence(anchorage_label)) %>%
  mutate(anchorage_name = glue::glue("{anchorage_label}, {country}")) %>%
  dplyr::select(-c(anchorage_label,anchorage_sublabel,country))

anchorages_with_predictions_combined_summary <- anchorages_with_predictions_combined %>%
  group_by(anchorage_name) %>%
  summarize(number_voyages = sum(number_voyages)) %>%
  ungroup() %>%
  arrange(-number_voyages)

top_10_ports_with_positives <- anchorages_with_predictions_combined_summary %>%
  slice(1:10)

top_10_ports_with_positives %>%
  mutate(number_voyages = prettyNum(number_voyages, big.mark = ",")) %>%
  rename(Port = anchorage_name,
         `Number port visits` = number_voyages) %>%
  knitr::kable()%>%
  kableExtra::kable_styling()

top_10_ports_with_positives$anchorage_name

anchorages_with_predictions_combined_centroids <- anchorages_with_predictions_combined %>%
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) %>%
  group_by(anchorage_name) %>%
  summarise(st_union(geometry)) %>%
  st_centroid() %>%
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
ggsave(here::here("figures","model_paper_port_visits_number_by_port_map.png"),
       port_visits_number_by_port_map,
       width=7,height=3,device="png",dpi=300, bg = 'white')

