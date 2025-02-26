#### example for diego pulling all bernie gobin
#### coho releases
#### Ty Garber 2/25/2025


library(tidyverse)

# connect to db
con <- DBI::dbConnect(RSQLite::SQLite(), 'rmis.db')

# query all bernie gobin recoveries after 2020, coho
query <- DBI::dbGetQuery(con,
                         "
                         select
                                rec.*,
                                lstock.name as stock_name,
                                lrec.name as recovery_location,
                                lrel.name as hatchery
                          from
                              releases rel
                          left join recoveries rec on rel.tag_code_or_release_id = rec.tag_code
                          left join locations lrec on rec.recovery_location_code = lrec.location_code and
                                                      lrec.location_type = '1'
                          left join locations lrel on rel.hatchery_location_code = lrel.location_code and
                                                      lrel.location_type = '3'
                          left join locations lstock on rel.stock_location_code = lstock.location_code and
                                                      lstock.location_type = '5'
                          where
                              lrel.location_code = '3F10308  070001 H' and
                              rec.run_year >= '2020' and
                              rec.species = '2'
                         "
                         
                         
                         
                         )
# count recoveried by year, recovery location
query |>
  count(run_year, recovery_location)


DBI::dbDisconnect(con)
