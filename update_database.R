##########################################
#### this script pulls all the csvs   ####
#### from the RMIS website, appends   ####
#### to database. Ty Garber 4/13/2023 ####
#### if a sqlite database doesn't     #### 
#### exist it will create one.        ####
##########################################


# libraries
library(RCurl)
library(purrr)
library(stringr)
library(DBI)
library(readr)
library(rvest)
library(dplyr)

# link to rmis ftp site
rmis_url <- "https://www.rmpc.org/pub/data-041/"

# fetch names, last updated times from website
filenames <- getURL(rmis_url) 

# read html table into R
files <- filenames %>% read_html() %>% html_table()

files_df <- files[[1]] %>%
  janitor::clean_names() %>%
  select(name, last_modified) %>%
  filter(
    str_detect(tolower(name), '.csv'),
    substr(tolower(name), 1,2 ) == tolower('RC') | # recoveries
      tolower(name) == tolower('RL041_ALL_FULLSET.csv') | # releases
      tolower(name) == tolower('LC041_ALL_FULLSET.csv') | # locations
      substr(tolower(name), 1,2 ) == tolower('CS')
  ) %>% 
  mutate(
    last_modified = as.character(as.Date(last_modified))
  )

con <- dbConnect(RSQLite::SQLite(), "rmis.db")

file_log <- dbReadTable(con, 'file_log') %>%
  as_tibble()

# check if files have been updated
update_files <- files_df %>%
  inner_join(file_log, by='name', suffix=c('_rmis', '_db')) %>%
  filter(
    as.Date(last_modified_rmis) > (as.Date(last_modified_db))
  )


updater <- function (file_name, uuid, last_modified){
  
  if(length(file_name) == 0){
    return(TRUE)
  }
  
  if(file_name == 'RL041_ALL_FULLSET.csv') {
    
    print(glue::glue('Updating releases dataset {file_name} - {uuid} - {last_modified}'))
    
    # replace releases table
     dbWriteTable(
       con,
       'releases',
       read_csv(paste0(rmis_url, file_name), col_types = cols(.default = "c"))
       ,
       overwrite = TRUE
     )

     update <- DBI::dbSendQuery(con, glue::glue(
       "UPDATE file_log
        SET
           last_modified = '{last_modified}',
           file_id = '{uuid::UUIDgenerate()}'
        WHERE
           file_id = '{uuid}'
       "
     ))
     DBI::dbClearResult(update)


    
    
  } else if (file_name == 'LC041_ALL_FULLSET.csv'){
    print(glue::glue('Updating locations dataset {file_name} - {uuid} - {last_modified}'))
    # replace releases table
    dbWriteTable(
      con,
      'locations',
      read_csv(paste0(rmis_url, file_name), col_types = cols(.default = "c"))
      ,
      overwrite = TRUE
    )
    
    update <- DBI::dbSendQuery(con, glue::glue(
      "UPDATE file_log
        SET
           last_modified = '{last_modified}',
           file_id = '{uuid::UUIDgenerate()}'
        WHERE
           file_id = '{uuid}'
       "
    ))
    DBI::dbClearResult(update)
    
    
  } else if (substr(tolower(file_name), 1,2 ) == tolower('RC')) {
    print(glue::glue('Updating recovery dataset {file_name} - {uuid} - {last_modified}'))
    
    new_uuid = uuid::UUIDgenerate()
    # insert new data
    dbWriteTable(
      con,
      'recoveries',
      read_csv(paste0(rmis_url, file_name), col_types = cols(.default = "c")) %>%
        mutate(file_id = new_uuid)
      ,
      append = TRUE
    )
    
    print('Updating ')
    update <- DBI::dbSendQuery(con, glue::glue(
      "UPDATE file_log
        SET
           last_modified = '{last_modified}',
           file_id = '{new_uuid}'
        WHERE
           file_id = '{uuid}';
       "
    ))
    DBI::dbClearResult(update)
    
    delete <-
      DBI::dbSendQuery(con, glue::glue(
        "DELETE FROM recoveries WHERE file_id = '{uuid}';"
      ))
    DBI::dbClearResult(delete)
  } else if (substr(tolower(file_name), 1,2 ) == tolower('CS')) {
    print(glue::glue('Updating catch sample dataset {file_name} - {uuid} - {last_modified}'))
    
    new_uuid = uuid::UUIDgenerate()
    # insert new data
    dbWriteTable(
      con,
      'catch_sample',
      read_csv(paste0(rmis_url, file_name), col_types = cols(.default = "c")) %>%
        mutate(file_id = new_uuid)
      ,
      append = TRUE
    )
    
    print('Updating ')
    update <- DBI::dbSendQuery(con, glue::glue(
      "UPDATE file_log
        SET
           last_modified = '{last_modified}',
           file_id = '{new_uuid}'
        WHERE
           file_id = '{uuid}';
       "
    ))
    DBI::dbClearResult(update)
    
    delete <-
      DBI::dbSendQuery(con, glue::glue(
        "DELETE FROM catch_sample WHERE file_id = '{uuid}';"
      ))
    DBI::dbClearResult(delete)
  }
  
  TRUE
}

#bc3a6698-5f02-41ae-ba3f-e16e9699d8c6

update_files %>%
  rowwise() %>%
  mutate(
    success = updater(name, file_id, last_modified_rmis)
  )

DBI::dbDisconnect(con)


# check for new files - need to wait for RMIS to get new files before
# further developing (testing this)
# new_files <- files_df %>%
#   left_join(file_log, by='name', suffix=c('_rmis', '_db')) %>%
#   filter(
#     is.na(file_id)
#   )





