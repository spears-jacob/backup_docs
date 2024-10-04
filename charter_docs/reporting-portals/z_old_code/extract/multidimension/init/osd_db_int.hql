USE ${env:ENVIRONMENT};

--create tables needed for OS Report
drop table if exists asp_metrics_detail_os_daily   purge;
drop table if exists asp_metrics_detail_os_monthly purge;
drop table if exists asp_metrics_detail_os_weekly  purge;

CREATE TABLE if not exists asp_metrics_detail_os_daily
( value       bigint,
  detail      string,
  metric      string,
  platform    string)
PARTITIONED BY (
  unit        string,
  domain      string,
  company     string,
  date_denver string,
  source_table string);

CREATE TABLE if not exists asp_metrics_detail_os_monthly
( value       bigint,
  detail      string,
  metric      string,
  platform    string)
PARTITIONED BY (
  unit        string,
  domain      string,
  company     string,
  year_month_denver string,
  source_table string);

CREATE TABLE if not exists asp_metrics_detail_os_weekly
( value       bigint,
  detail      string,
  metric      string,
  platform    string)
PARTITIONED BY (
  unit        string,
  domain      string,
  company     string,
  week_ending_date_denver string,
  source_table string);

-- create views needed for OS Report
--summary OS view for OS report
--DROP VIEW asp_v_summary_os;


CREATE VIEW if not exists asp_v_summary_os AS
SELECT date_denver           as date_label
     , case split(metric,'[\|]') [0]
        when 'os_android'    then 'Android'
        when 'os_ios'        then 'Apple'
        else 'Other'
        end                  as os_type
     , 'day'                 as date_grain
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'devices'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , date_denver           as date_denver
  FROM prod.asp_venona_counts_daily
 WHERE metric                like 'os%'
   AND source_table          not like '%staging%'
   AND (date_denver          >= DATE_SUB(CURRENT_DATE,180))
 GROUP BY date_denver, split(metric,'[\|]') [0], source_table

UNION ALL
SELECT year_month_denver     as date_label
     , case split(metric,'[\|]') [0]
        when 'os_android'    then 'Android'
        when 'os_ios'        then 'Apple'
        else 'Other'
        end                  as os_type
     , 'month'               as date_grain
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'devices'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , CONCAT(year_month_denver, '-01')     as date_denver
  FROM prod.asp_venona_counts_monthly
 WHERE metric                like 'os%'
   AND source_table          not like '%staging%'
   AND year_month_denver     >= '2018-07'
 GROUP BY year_month_denver, split(metric,'[\|]') [0], source_table

UNION ALL
SELECT week_ending_date_denver          as date_label
     , case split(metric,'[\|]') [0]
        when 'os_android'    then 'Android'
        when 'os_ios'        then 'Apple'
        else 'Other'
        end                  as os_type
     , 'week'                as date_grain
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'devices'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , week_ending_date_denver          as date_denver
  FROM prod.asp_venona_counts_weekly
 WHERE metric                like 'os%'
   AND source_table          not like '%staging%'
   AND week_ending_date_denver IN (select distinct week_ending_date_denver
                                     from asp_metrics_detail_os_weekly)
 GROUP BY week_ending_date_denver, split(metric,'[\|]') [0], source_table
;

--detailed OS view for OS report
--DROP VIEW asp_v_detailed_os;


CREATE VIEW if not exists asp_v_detailed_os AS
SELECT date_denver           as date_label
     , 'day'                 as date_grain
     , detail                as os_name
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'visitors'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , date_denver           as date_denver
  FROM prod.asp_metrics_detail_os_daily
  WHERE (date_denver         >= DATE_SUB(CURRENT_DATE,180))
 GROUP BY date_denver, 'day', detail, source_table

UNION ALL
SELECT year_month_denver     as date_label
     , 'month'               as date_grain
     , detail                as os_name
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'visitors'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , CONCAT(year_month_denver, '-01')     as date_denver
  FROM prod.asp_metrics_detail_os_monthly
 GROUP BY year_month_denver, 'month', detail, source_table, CONCAT(year_month_denver, '-01')

UNION ALL
SELECT week_ending_date_denver          as date_label
     , 'week'                as date_grain
     , detail                as os_name
     , case source_table
        when 'asp_v_venona_events_portals_msa'     then 'myspectrum'
        when 'asp_v_venona_events_portals_smb'     then 'smb'
        when 'asp_v_venona_events_portals_specnet' then 'specnet'
        when 'asp_v_venona_events_specmobile'      then 'specmobile'
        else 'other'
        end                  as app_name
     , SUM(case when unit = 'visits'
            then value       else 0
            end)             as visits
     , SUM(case when unit = 'visitors'
            then value       else 0
            end)             as visitors
     , SUM(case when unit = 'households'
            then value       else 0
            end)             as households
     , SUM(case when unit = 'instances'
            then value       else 0
            end)             as instances
     , week_ending_date_denver          as date_denver
  FROM prod.asp_metrics_detail_os_weekly
 GROUP BY week_ending_date_denver, 'week', detail, source_table
;
