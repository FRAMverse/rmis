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
    str_detect(tolower(name), '.csv'),
    substr(tolower(name), 1,2 ) == tolower('RC') | # recoveries
    tolower(name) == tolower('RL042_ALL_FULLSET.csv') | # releases
    tolower(name) == tolower('LC042_ALL_FULLSET.csv') | # locations
      substr(tolower(name), 1,2 ) == tolower('CS')
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
    if (tolower(.x) == tolower('LC042_ALL_FULLSET.csv')) {
      dbWriteTable(
        con,
        'locations',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c"))
        ,
        overwrite = TRUE
      )
    } else if (str_detect(tolower(.x), tolower('RC'))) {
      dbWriteTable(
        con,
        'recoveries',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c")) %>%
          mutate(file_id = files_df$file_id[.y])
        ,
        append = TRUE
      )
    } else if (tolower(.x) == tolower('RL042_ALL_FULLSET.csv')){
      dbWriteTable(
        con,
        'releases',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c"))
        ,
        overwrite = TRUE
      )
    } else if (str_detect(tolower(.x), tolower('CS'))) {
      dbWriteTable(
        con,
        'catch_sample',
        read_csv(paste0(rmis_url, .x), col_types = cols(.default = "c")) %>%
          mutate(file_id = files_df$file_id[.y])
        ,
        append = TRUE
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

rel_index <- DBI::dbSendStatement(con,
                     '
                      create index rel_tag
                      on releases(tag_code_or_release_id);
                     ')
DBI::dbClearResult(rel_index)

rec_index <- DBI::dbSendStatement(con,
                     '
              create index rec_tag
              on recoveries(tag_code);
                    ')
DBI::dbClearResult(rec_index)


loc_id_index <- DBI::dbSendStatement(con,
                     '
create index loc_location_id
on locations(location_code);
                ')

DBI::dbClearResult(loc_id_index)

loc_type_index <- DBI::dbSendStatement(con,
                     '
create index loc_location_type
on locations(location_type);
')

DBI::dbClearResult(loc_type_index)


DBI::dbDisconnect(con)
