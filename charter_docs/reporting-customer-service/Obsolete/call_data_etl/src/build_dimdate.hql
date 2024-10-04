set hivevar:start_date=2000-01-01;
set hivevar:days=7300;
set hivevar:table_name=${env:ENVIRONMENT}.cs_dates;



DROP TABLE IF EXISTS ${table_name};
CREATE TABLE IF NOT EXISTS ${table_name} AS
WITH dates AS (
    SELECT date_add("${start_date}", a.pos) as c_date
    FROM (SELECT posexplode(split(repeat(",", ${days}), ","))) a
),
dates_expanded AS (
  SELECT
    c_date as calendar_date,
    year(c_date) as year,
    month(c_date) as month,
    day(c_date) as day,
    date_format(c_date, 'u') as day_of_week
    -- from_unixtime(unix_timestamp(date, "yyyy-MM-dd"), "u") as day_of_week
  FROM dates
)
SELECT
    calendar_date,
    year,
    cast(month(calendar_date)/4 + 1 AS BIGINT) as quarter,
    month,
    date_format(calendar_date, 'W') as week_of_month,
    -- from_unixtime(unix_timestamp(date, "yyyy-MM-dd"), "W") as week_of_month,
    date_format(calendar_date, 'w') as week_of_year,
    -- from_unixtime(unix_timestamp(date, "yyyy-MM-dd"), "w") as week_of_year,
    day,
    day_of_week,
    date_format(calendar_date, 'EEE') as day_of_week_s,
    -- from_unixtime(unix_timestamp(date, "yyyy-MM-dd"), "EEE") as day_of_week_s,
    date_format(calendar_date, 'D') as day_of_year,
    -- from_unixtime(unix_timestamp(date, "yyyy-MM-dd"), "D") as day_of_year,
    regexp_replace(
        if(day >= 22
          ,concat(if(day>=22 AND month = 12, year+1,year),'-',lpad(month+1,2,"0"))
          ,concat(if(day>=22 AND month = 12, year+1,year),'-',lpad(month,2,"0"))
          )
          ,"13","01") fiscal_month,
    if(day>=22 AND month = 12, year+1,year) fiscal_year,
    datediff(calendar_date, "1970-01-01") as day_of_epoch,
    if(day_of_week BETWEEN 6 AND 7, true, false) as weekend,
    if(
      ((month = 1 AND day = 1 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 1 AND day BETWEEN 1 AND 3)) -- New Year's Day
      OR (month = 1 AND day_of_week = 1 AND day BETWEEN 15 AND 21) -- MLK Jr
      OR (month = 2 AND day_of_week = 1 AND day BETWEEN 15 AND 21) -- President's Day
      OR (month = 5 AND day_of_week = 1 AND day BETWEEN 25 AND 31) -- Memorial Day
      OR ((month = 7 AND day = 4 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 7 AND day BETWEEN 4 AND 6)) -- Independence Day
      OR (month = 9 AND day_of_week = 1 AND day BETWEEN 1 AND 7) -- Labor Day
      OR ((month = 11 AND day = 11 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 11 AND day BETWEEN 11 AND 13)) -- Veteran's D$
      OR (month = 11 AND day_of_week = 4 AND day BETWEEN 22 AND 28) -- Thanksgiving
      OR ((month = 12 AND day = 25 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 12 AND day BETWEEN 25 AND 27)) -- Christmas
     ,true, false) as us_holiday
     FROM dates_expanded
     SORT BY calendar_date
     ;
