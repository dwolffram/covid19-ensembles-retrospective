library(tidyverse)

Sys.setlocale("LC_ALL", "C")

VALID_TARGETS = c(paste(1:4, "wk ahead inc death"), paste(1:4, "wk ahead cum death"), paste(1:4, "wk ahead inc case"))

MODELS_TO_EXCLUDE = c('COVIDhub-ensemble', 'COVIDhub-trained_ensemble', 'COVIDhub-4_week_ensemble',
                      'CU-nochange', 'CU-scenario_high', 'CU-scenario_low', 'CU-scenario_mid',
                      'KITmetricslab-select_ensemble')

LOCATIONS_TO_EXCLUDE = c("11", "60", "66", "69", "72", "74", "78")

START_DATE = '2021-04-19'

next_monday <- Vectorize(function(date){
  date + (0:6)[weekdays(date + (0:6)) == "Monday"]
  }
)


get_all_filepaths_and_dates <- function(models_to_exclude, start_date){
  files <- list.files(path=paste0("../covid19-forecast-hub/data-processed/"), 
                      pattern=".csv$", full.names = TRUE, recursive=TRUE)
  
  df_files <- data.frame(filename = files)
  df_files$model <- str_split(str_remove(df_files$filename, "../covid19-forecast-hub/data-processed//"),
                              "/", simplify = TRUE)[, 1]
  
  df_files$forecast_date <- as.Date(str_sub(str_split(str_remove(df_files$filename, "../covid19-forecast-hub/data-processed//"), "/", simplify = TRUE)[, 2], end = 10))
  
  df_files$timezero <- sapply(df_files$forecast_date, next_monday)
  df_files$timezero <- as.Date(df_files$timezero, origin = "1970-01-01")
  df_files <- df_files %>%
    filter(!(model %in% models_to_exclude) &
             timezero >= start_date)
}

df_files <- get_all_filepaths_and_dates(MODELS_TO_EXCLUDE, START_DATE)

load_file <- function(path, valid_targets, locations_to_exclude){
  
  df <- read_csv(path, col_types = cols(forecast_date = col_date(format = ""),
                                        target = col_character(),
                                        target_end_date = col_date(format = ""),
                                        location = col_character(),
                                        type = col_character(),
                                        quantile = col_double(),
                                        value = col_double()),
                 progress = FALSE)
  
  df <- df %>%
    filter(type == 'quantile',
           target %in% valid_targets,
           str_length(location) == 2,
           !(location %in% locations_to_exclude)) %>%
    drop_na()
  
  df <- df %>%
    group_by(target, location) %>%
    mutate(n_quantiles = n()) %>%
    group_by(target) %>%
    mutate(n_quantiles = min(n_quantiles)) %>%
    filter(n_quantiles == 23 | (str_detect(target, 'inc case') & n_quantiles == 7)) %>%
    select(-n_quantiles)
  
  df <- df %>%
    group_by(target) %>%
    filter(n_distinct(location) == 51)
  
  df <- df %>%
    mutate(base_target = str_sub(target, 12)) %>%
    group_by(location, base_target) %>%
    mutate(n_horizons = n_distinct(target)) %>%
    group_by(base_target) %>%
    filter(min(n_horizons) == 4) %>%
    ungroup() %>%
    select(-c(n_horizons, base_target))
}

# path <- "../covid19-forecast-hub/data-processed//BPagano-RtDriven/2020-10-25-BPagano-RtDriven.csv"
# 
# d <- load_file(path, VALID_TARGETS, LOCATIONS_TO_EXCLUDE)



pb <- txtProgressBar(0, nrow(df_files), style = 3)

df <- data.frame()
for (row in 1:nrow(df_files)){
  setTxtProgressBar(pb, row)
  df_temp <- load_file(df_files[row, "filename"], VALID_TARGETS, LOCATIONS_TO_EXCLUDE)
  df_temp$model <- df_files[row, "model"]
  
  if (nrow(df_temp) == 0) {
    next
  } 
  
  df <- bind_rows(df, df_temp)
}
close(pb)

write_csv(df, "data/df.csv.gz")

df_cd <- df %>%
  filter(target %in% paste(1:4, "wk ahead cum death"))

write_csv(df_cd, "data/df_cumulative_deaths.csv.gz")


