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
rmis_url <- "https://www.rmpc.org/pub/data/"

# fetch names, last updated times from website
filenames <- getURL(rmis_url) 

# read html table into R
files <- filenames %>% read_html() %>% html_table()

files_df <- files[[1]] %>%
  janitor::clean_names() %>%
  select(name, last_modified) %>%
  filter(
    str_detect(name, '.csv'),
    str_detect(name, 'RC') |
      str_detect(name, 'RL041_ALL_FULLSET.csv') |
      str_detect(name, 'LC041_ALL_FULLSET.csv') 
  ) %>%
  rowwise() %>%
  mutate(
    file_id = uuid::UUIDgenerate(),
    last_modified = as.character(as.Date(last_modified))
  )

con <- dbConnect(RSQLite::SQLite(), "rmis.db")

# don't want to commit this to memory it's multiple gb
# populate the database
files_df %>%
  pull(name) %>%
  imap(~ {
    if (.x == 'LC041_ALL_FULLSET.csv') {
      dbWriteTable(
        con,
        'locations',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c"))
        ,
        overwrite = TRUE
      )
    } else if (str_detect(.x, 'RC')) {
      dbWriteTable(
        con,
        'recoveries',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c")) %>%
          mutate(file_id = files_df$file_id[.y])
        ,
        append = TRUE
      )
    } else if (.x == 'RL041_ALL_FULLSET.csv'){
      dbWriteTable(
        con,
        'releases',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c"))
        ,
        overwrite = TRUE
      )
    }
  }
)
# save a file log of updates to rmis csvs
dbWriteTable(
  con,
  'file_log',
  files_df,
  overwrite = TRUE
)

# set up indices on some tables - this will make
# queries much, much faster

DBI::dbSendStatement(con,
                     '
                      create index rel_tag
                      on releases(tag_code_or_release_id);
                      
                      create index rec_tag
                      on recoveries(tag_code);
                      
                      create index loc_location_id
                      on locations(location_code);
                     
                     create index loc_location_type
                      on locations(location_type);
                     ')

DBI::dbDisconnect(con)
