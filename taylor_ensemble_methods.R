source("load_data.R")
source("scoring.R")

# options(dplyr.summarise.inform=F)

INV_INT <- function(df, params){
  df_temp <- merge(df, params, by=c('model'))
  inv <- df_temp %>%
    mutate(weighted_values = value * param) %>%
    group_by(across(any_of(c("target_end_date", "location", "target", "quantile", "truth")))) %>% 
    summarize(value = sum(weighted_values)) %>%
    as.data.frame()
  return(inv)
}

inv_int_fit <- function(df){
  train_wis <- score_forecasts(df, scores='wis')
  params <- train_wis %>%
    group_by(model) %>%
    summarize(param = mean(wis))%>%
    mutate(param = (1/param)/(sum(1/param))) %>%
    as.data.frame()
  return(params)
}


df <- load_data()

dfs <- train_test_split(df, "2021-07-19")
train <- dfs$df_train
test <- dfs$df_test

p <- inv_int_fit(train)

df_forecast <- INV_INT(test, p)

t <- test %>% select(-truth)
df_forecast2 <- INV_INT(t, p)

df_temp <- merge(test, p, by=c('model'))

inv <- df_temp %>%
  mutate(weighted_values = value * param) %>%
  group_by(target_end_date, location, target, quantile, truth) %>% 
  summarize(value = sum(weighted_values), .groups = "drop_last") %>%
  as.data.frame()

inv2 <- df_temp %>%
  mutate(weighted_values = value * param) %>%
  group_by(target_end_date, location, target, quantile, truth) %>% 
  summarize(value = sum(weighted_values)) %>%
  as.data.frame()


e <- EWA(test)

ensemble_forecasts <- function(test_dates){
  
  df_ensembles <- foreach(test_date=test_dates, .combine=rbind) %dopar% {
    #print(as.character(test_date))
    dfs <- train_test_split(df, test_date)

    df_train <- dfs$df_train
    df_test <- dfs$df_test

    p <- inv_int_fit(df_train)
    df_forecast <- INV_INT(df_test, p)
    df_forecast
  }
  
  df_ensembles <- df_ensembles %>%
    select(-truth)
  
  return(df_ensembles)
}

library(doParallel)
# no_cores <- detectCores() - 1  
no_cores <- 2
registerDoParallel(cores=no_cores)  
df_ensembles <- ensemble_forecasts(c("2021-07-12", "2021-07-19"))


