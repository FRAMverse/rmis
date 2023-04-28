/*
 This is an example query on where/how to use joins in this database
 Ty Garber 4/28/2023
 */


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
    rel.tag_code_or_release_id in ('634281',
                    '634844',
                    '635292',
                    '635282',
                    '636168',
                    '636299',
                    '636669',
                    '636824',
                    '636954',
                    '637171',
                    '637227',
                    '637350');
