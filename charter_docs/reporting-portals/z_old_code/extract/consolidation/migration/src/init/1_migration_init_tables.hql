USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_extract_migration_process_run_date(run_date STRING)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

drop table IF EXISTS asp_migration_metrics;
create table IF NOT EXISTS asp_migration_metrics (
  platform string,
  footprint string,
  wid string,
  metric_name string,
  metric_value double
)
PARTITIONED BY (denver_date string);

drop VIEW IF EXISTS asp_v_migration_metrics;
create VIEW IF NOT EXISTS asp_v_migration_metrics AS
select distinct
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date,
       wave_status,
       CASE WHEN metric_name like '%instances' then 'instance'
            WHEN (metric_name = 'page_views_visits_with_call' OR metric_name = 'page_views_visits')
                 then 'visit'
            WHEN (metric_name = 'optin_account_daily_hh' OR metric_name = 'optout_account_daily_hh'
                  OR metric_name = 'migrated_account_with_visit' OR metric_name='migrated_account_with_visit_daily'
                  OR metric_name = 'bubble_account')
                 then 'household'
            ELSE 'NA'
       END as grain
  from
        (select *, -2 as wave_status
           from asp_migration_metrics
          where metric_name not like 'eligible_acct%'
          union all
         select a.*, b.metric_value as wave_status
           from
               (select *
                  from asp_migration_metrics
                 where metric_name like 'eligible_acct%') a
                  join
              ( select *
                  from asp_migration_metrics
                 where metric_name = 'wave_status') b
             on a.wid = b.wid
            and a.denver_date=b.denver_date) c;

create table IF NOT EXISTS asp_migration_employee
           (state string,
            account string,
            sys string,
            prin string,
            agnt string)
row format delimited fields terminated by ','
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/asp_migration_employee.csv' OVERWRITE INTO TABLE asp_migration_employee;
