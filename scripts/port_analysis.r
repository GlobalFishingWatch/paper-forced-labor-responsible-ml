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
