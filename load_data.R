library(tidyverse)

Sys.setlocale("LC_ALL", "C")

next_monday <- Vectorize(function(date){
  date + (0:6)[weekdays(date + (0:6)) == "Monday"]
  }
)

load_truth <- function(target="incident_cases", as_of){
  truth <- read.csv(paste0("https://raw.githubusercontent.com/dwolffram/covid19-versioned-data/main/data/", 
                           target, "/jhu_", target, "_as_of_", as_of, ".csv"), 
                    colClasses = c(location="character", date ="Date"))
}

add_truth <- function(df, as_of){
  target_dict = list("inc case" = "incident_cases",
                     "inc death" = "incident_deaths",
                     "cum death" = "cumulative_deaths")
  
  df$merge_target <- str_sub(df$target, start=12)
  
  targets <- unique(df$merge_target)
  
  truth_df <- data.frame()
  for (target in targets){
    truth <- load_truth(target_dict[[target]], as_of) %>%
      rename(truth = value) %>%
      mutate(merge_target = target)
    
    truth_df <- bind_rows(truth_df, truth)
  }
  
  df <- df %>%
    left_join(truth_df, by=c("merge_target", "target_end_date"="date", "location")) %>% 
    select(- merge_target)
  
  return(df)
}

# load data for models that require complete submissions (window size 4)
load_train_test <- function(forecast_date, national_level = FALSE){
  df_train <- read_csv(paste0("data/", forecast_date, "_train.csv"), 
                       col_types = cols(forecast_date = col_date(format = ""),
                                        target = col_character(),
                                        target_end_date = col_date(format = ""),
                                        location = col_character(),
                                        type = col_character(),
                                        quantile = col_double(),
                                        value = col_double())) %>%    
    filter(if (!national_level) location != "US" else TRUE) %>%
    as.data.frame()
  
  
  df_test <- read_csv(paste0("data/", forecast_date, "_test.csv"), 
                      col_types = cols(forecast_date = col_date(format = ""),
                                       target = col_character(),
                                       target_end_date = col_date(format = ""),
                                       location = col_character(),
                                       type = col_character(),
                                       quantile = col_double(),
                                       value = col_double())) %>%
    filter(if (!national_level) {location != "US"} else {location == "US"}) %>%
    as.data.frame()
  
  return(list(df_train=df_train, df_test=df_test))
}


load_data <- function(target = "cum death"){
  df <- read_csv("data/df.csv.gz", col_types = cols(forecast_date = col_date(format = ""),
                                                                 target = col_character(),
                                                                 target_end_date = col_date(format = ""),
                                                                 location = col_character(),
                                                                 type = col_character(),
                                                                 quantile = col_double(),
                                                                 value = col_double()))
  
  # add timezero: the next Monday after forecast_date (in case the submission was made before Monday)
  df <- df %>%
    nest_by(forecast_date) %>%
    mutate(timezero = as.Date(next_monday(forecast_date), origin = "1970-01-01")) %>%
    unnest(cols = c(data))
  
  df <- df %>%
    filter(target %in% paste(1:4, "wk ahead", !!target))
  
  return(df)
}


# select train and test data for models that use the full history (with missing submissions)
train_test_split <- function(df, test_date, intersect = TRUE){
  df_test <- df %>%
    filter(timezero == test_date) %>%    
    as.data.frame()
  
  
  df_train <- df %>%
    filter(target_end_date < test_date,
           model %in% unique(df_test$model)) %>%
    as.data.frame()
  
  if(intersect){
    df_train <- df_train %>%
      filter(model %in% unique(df_test$model))
    
    df_test <- df_test %>%
      filter(model %in% unique(df_train$model))
  }

  df_train <- add_truth(df_train, as_of=test_date)
  df_test$truth <- NA
  
  return(list(df_train=df_train, df_test=df_test))
}


df <- load_data()

dfs <- train_test_split(df, "2021-07-19")
train <- dfs$df_train
test <- dfs$df_test

sort(unique(train$model)) == sort(unique(test$model))
sort(unique(train$model)) 
sort(unique(test$model))

