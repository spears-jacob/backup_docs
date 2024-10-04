USE ${env:DASP_db};
set hive.strict.checks.type.safety=false;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

set cqe_acct_pct=0.04;
set cqe_mso_pct=0.03;

--get mean and std dev for the metrics that have weekly pattern
INSERT overwrite table asp_digital_adoption_monitor_range PARTITION(run_date)
select concat(source_table, '_null') as source_table,
       date_type,
       from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u') as day_of_week,
       '' as application_name,
       metric_name,
       min(metric_count) as min_value,
       max(metric_count) as max_value,
       percentile(cast(metric_count as BIGINT), 0.5) as median,
       avg(metric_count) as mean,
       variance(metric_count) as vari,
       stddev(metric_count) as std_dev,
       avg(metric_count)-2*stddev(metric_count) as mean_minus_2std,
       avg(metric_count)+2*stddev(metric_count) as mean_plus_2std,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null
 where source_table in ('atom','btm','call')
   and date_value < '${hiveconf:START_DATE_DENVER}'
 group by source_table,
          date_type,
          from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u'),
          metric_name
 order by day_of_week, source_table
;

INSERT INTO table asp_digital_adoption_monitor_range PARTITION(run_date)
select concat(source_table, '_null') as source_table,
       date_type,
       from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u') as day_of_week,
       application_name,
       metric_name,
       min(metric_count_distinct) as min_value,
       max(metric_count_distinct) as max_value,
       percentile(cast(metric_count_distinct as BIGINT), 0.5) as median,
       avg(metric_count_distinct) as mean,
       variance(metric_count_distinct) as vari,
       stddev(metric_count_distinct) as std_dev,
       avg(metric_count_distinct)-2*stddev(metric_count_distinct) as mean_minus_2std,
       avg(metric_count_distinct)+2*stddev(metric_count_distinct) as mean_plus_2std,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null
 where source_table in ('cqe_sspp')
   and date_value < '${hiveconf:START_DATE_DENVER}'
   and metric_name in ('count_account_number','count_visit')
  group by source_table,
           date_type,
           from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u'),
           application_name,
           metric_name
  order by day_of_week, source_table
;

INSERT INTO table asp_digital_adoption_monitor_range PARTITION(run_date)
select concat(source_table, '_null') as source_table,
       date_type,
       from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u') as day_of_week,
       application_name,
       metric_name,
       min(metric_count_pct) as min_value,
       max(metric_count_pct) as max_value,
       (percentile(cast(metric_count_pct*100 as BIGINT), 0.5))/100 as median,
       avg(metric_count_pct) as mean,
       variance(metric_count_pct) as vari,
       stddev(metric_count_pct) as std_dev,
       avg(metric_count_pct)-2*stddev(metric_count_pct) as mean_minus_2std,
       avg(metric_count_pct)+2*stddev(metric_count_pct) as mean_plus_2std,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null
 where source_table in ('cqe_sspp')
   and date_value < '${hiveconf:START_DATE_DENVER}'
   and metric_name in ('count_mso')
 group by source_table,
          date_type,
          from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u'),
          application_name,
          metric_name
 order by day_of_week, source_table
;

INSERT INTO table asp_digital_adoption_monitor_range PARTITION(run_date)
select concat(source_table, '_null') as source_table,
       date_type,
       from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u') as day_of_week,
       application_name,
       metric_name,
       min(metric_count) as min_value,
       max(metric_count) as max_value,
       percentile(cast(metric_count as BIGINT), 0.5) as median,
       avg(metric_count) as mean,
       variance(metric_count) as vari,
       stddev(metric_count) as std_dev,
       avg(metric_count)-2*stddev(metric_count) as mean_minus_2std,
       avg(metric_count)+2*stddev(metric_count) as mean_plus_2std,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null
 where source_table in ('cqe_sspp')
   and date_value < '${hiveconf:START_DATE_DENVER}'
   and metric_name in ('count_user_journey', 'count_user_sub_journey')
 group by source_table,
          date_type,
          from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u'),
          application_name,
          metric_name
 order by day_of_week, source_table
;

INSERT INTO table asp_digital_adoption_monitor_range PARTITION(run_date)
select concat(source_table, '_cqe') as source_table,
       date_type,
       from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u') as day_of_week,
       application_name,
       metric_name,
       min(metric_count) as min_value,
       max(metric_count) as max_value,
       percentile(cast(metric_count as BIGINT), 0.5) as median,
       avg(metric_count) as mean,
       variance(metric_count) as vari,
       stddev(metric_count) as std_dev,
       avg(metric_count)-2*stddev(metric_count) as mean_minus_2std,
       avg(metric_count)+2*stddev(metric_count) as mean_plus_2std,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from
       (select source_table,
               date_type,
               date_value,
               application_name,
               metric_name,
               sum(metric_count_distinct) as metric_count
          from asp_digital_adoption_monitor_cqe
         where source_table='cqe_sspp'
           and date_value < '${hiveconf:START_DATE_DENVER}'
           and grouping_id=3
           and metric_name in ('count_account_number','count_visit')
           and visit_mso in ('CHR','BHN','TWC')
         group by source_table,date_type,date_value,application_name,metric_name) a
  group by source_table,
           date_type,
           application_name,
           metric_name,
           from_unixtime(unix_timestamp(date_value,'yyyy-MM-dd'),'u')
;

--checking table asp_digital_adoption_monitor_null
INSERT overwrite table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       b.source_table,
       a.date_type,
       a.date_value,
       '' as customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_count,
       a.metric_count_pct,
       b.min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null a
  join asp_digital_adoption_monitor_range b
    on concat(a.source_table, '_null') = b.source_table
   and a.metric_name = b.metric_name
   AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
 where a.source_table='atom'
   AND date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and (a.metric_count < b.min_value OR a.metric_count_pct < 1);

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       b.source_table,
       a.date_type,
       a.date_value,
       '' as customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_count,
       a.metric_count_pct,
       b.min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null a
  join asp_digital_adoption_monitor_range b
    on concat(a.source_table, '_null') = b.source_table
   and a.metric_name = b.metric_name
   AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
 where a.source_table='btm'
   and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and (a.metric_count < b.min_value OR a.metric_count_pct < 1);

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       b.source_table,
       a.date_type,
       a.date_value,
       '' as customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_count,
       a.metric_count_pct,
       b.min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null a
  join asp_digital_adoption_monitor_range b
    on concat(a.source_table, '_null') = b.source_table
   and a.metric_name = b.metric_name
   AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
 where a.source_table='call'
   and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and a.metric_name in ('count_call_id','count_segmenet_id','count_inbound_key')
   and (a.metric_count < b.min_value OR a.metric_count_pct < 1.0);

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       b.source_table,
       a.date_type,
       a.date_value,
       '' as customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_count,
       a.metric_count_pct,
       b.min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null a
  join asp_digital_adoption_monitor_range b
    on concat(a.source_table, '_null') = b.source_table
   and a.metric_name = b.metric_name
   AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
 where a.source_table='call'
   and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and a.metric_name in ('count_account_number','count_account_key')
   and (a.metric_count < b.min_value or a.metric_count_pct < 0.941);

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       b.source_table,
       a.date_type,
       a.date_value,
       '' as customer_type,
       a.application_name,
       a.metric_name,
       a.metric_count,
       a.metric_count_pct,
       b.min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null a
  join asp_digital_adoption_monitor_range b
    on concat(a.source_table, '_null') = b.source_table
   and a.metric_name = b.metric_name
   AND a.application_name = b.application_name
   AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
 where a.source_table='cqe_sspp'
   and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and a.metric_name in ('count_visit')
   and a.application_name in ('SpecNet','SMB')
   and (a.metric_count < b.min_value or a.metric_count_pct < 1.0);

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       concat(source_table, '_null'),
       a.date_type,
       a.date_value,
       '' as customer_type,
       a.application_name,
       'count_account_number vs count_mso' as metric_name,
       abs(metric_count1-metric_count2) as metric_count,
       count_diff as metric_count_pct,
       '' as min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
  (select
          a.source_table,
          a.date_type,
          a.date_value,
          a.application_name,
          collect_set(a.metric_name)[0] as metric_name1,
          collect_set(a.metric_name)[1] as metric_name2,
          collect_set(a.metric_count)[0] as metric_count1,
          collect_set(a.metric_count)[1] as metric_count2,
          collect_set(a.metric_count_pct)[0] as metric_count_pct1,
          collect_set(a.metric_count_pct)[1] as metric_count_pct2,
          abs(collect_set(a.metric_count)[0]-collect_set(a.metric_count)[1])/(collect_set(a.metric_count)[0]+collect_set(a.metric_count)[1]) as count_diff,
          abs(collect_set(a.metric_count_pct)[0]-collect_set(a.metric_count_pct)[1])/(collect_set(a.metric_count_pct)[0]+collect_set(a.metric_count_pct)[1]) as pct_diff
     from asp_digital_adoption_monitor_null a
    where a.source_table='cqe_sspp'
      and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
      and a.metric_name in ('count_account_number','count_mso')
      and a.application_name in ('SpecNet','SMB')
      and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
    group by a.source_table, a.date_type, a.date_value, a.application_name) a
where count_diff > ${hiveconf:cqe_acct_pct}
;

--checking mso which is not in ('BHN','CHR','TWC')
INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       concat(source_table, '_null'),
       date_type,
       date_value,
       '' as customer_type,
       application_name,
       'mso_set' as metric_name,
       mso_set as metric_count,
       '' as metric_count_pct,
       '' as min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
  from asp_digital_adoption_monitor_null
 where source_table='cqe_sspp'
   and date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and metric_name in ('count_account_number','count_mso','count_visit')
   and application_name in ('SpecNet','SMB')
   and (mso_set is not null and mso_set !='' and mso_set !='BHN,CHR,TWC')
;

----checking table asp_digital_adoption_monitor_cqe (daily)
INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       concat(source_table, '_cqe'),
       date_type,
       date_value,
       '' as customer_type,
       application_name,
       metric_name,
       metric_count,
       metric_count_pct,
       ${hiveconf:cqe_mso_pct} as min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
from asp_digital_adoption_monitor_cqe
where source_table='cqe_sspp'
AND date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
and metric_name='number of visit with more than 1 mso'
and metric_count_pct > ${hiveconf:cqe_mso_pct}
order by date_value;

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
       concat(source_table, '_cqe'),
       date_type,
       date_value,
       '' as customer_type,
       application_name,
       metric_name,
       metric_count,
       metric_count_pct,
       ${hiveconf:cqe_acct_pct} as min_value,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
from asp_digital_adoption_monitor_cqe
where source_table='cqe_sspp'
and grouping_id=3
AND date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
and metric_name='count_account_number'
and visit_mso is null
and metric_count_pct > ${hiveconf:cqe_acct_pct}
order by date_value;

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select distinct
        a.source_table,
        a.date_type,
        a.date_value,
        '' as customer_type,
        a.application_name,
        a.metric_name,
        a.metric_count,
        b.min_value,
        b.max_value,
        '${env:CURRENT_TIME}' as run_time,
        SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
(select source_table,
        date_type,
        date_value,
        application_name,
        metric_name,
        sum(metric_count_distinct) as metric_count
   from asp_digital_adoption_monitor_cqe
  where source_table='cqe_sspp'
    and grouping_id=3
    and metric_name in ('count_account_number','count_visit')
    and visit_mso in ('CHR','BHN','TWC')
    AND date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
    AND application_name='SpecNet'
  group by source_table,date_type,date_value,application_name,metric_name) a
   join asp_digital_adoption_monitor_range b
     on concat(a.source_table, '_cqe') = b.source_table
    and a.metric_name = b.metric_name
    AND a.application_name = b.application_name
    AND from_unixtime(unix_timestamp(a.date_value,'yyyy-MM-dd'),'u') = day_of_week
  where (a.metric_count < b.min_value or a.metric_count > b.max_value);

--checking table asp_digital_adoption_monitor_da (daily)
INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select 'daily atom_call vs da_daily' as source_table,
       a.date_value,
       a.customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_value as da_metric_value,
       b.metric_value as call_metric_value,
       a.metric_value-b.metric_value as diff,
       (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 as diff_pct,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='atom_call'
    and metric_name='call_counts') a
FULL Join
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='da_daily'
    and metric_name='call_counts') b
   ON a.date_value = b.date_value
  and a.metric_name = b.metric_name
  and a.customer_type=b.customer_type
where a.date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
  AND ((a.metric_value-b.metric_value) is null
    or (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 > 0.01)
order by a.customer_type,a.date_value;

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select 'daily atom_call vs da_daily' as source_table,
       a.date_value,
       a.customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_value as da_metric_value,
       b.metric_value as call_metric_value,
       a.metric_value-b.metric_value as diff,
       (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 as diff_pct,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='atom_cqe_btm'
    and metric_name='digital_adoption') a
FULL Join
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='da_daily'
    and metric_name='digital_adoption') b
     ON a.date_value = b.date_value
    and a.metric_name = b.metric_name
    and a.customer_type=b.customer_type
  where a.date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
    aND ((a.metric_value-b.metric_value) is null
     or (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 > 0.01)
order by a.customer_type,a.date_value;

--checking table asp_digital_adoption_monitor_da (monthly)
INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select 'monthly atom_call vs da_daily' as source_table,
       a.date_value,
       a.customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_value as da_metric_value,
       b.metric_value as call_metric_value,
       a.metric_value-b.metric_value as diff,
       (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 as diff_pct,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='atom_call'
    and metric_name='call_counts') a
FULL Join
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='da_daily'
    and metric_name='call_counts') b
   ON a.date_value = b.date_value
  and a.metric_name = b.metric_name
  and a.customer_type=b.customer_type
where SUBSTRING(a.date_value, 1, 7)=substring(add_months('${env:CURRENT_DAY}',-1),1,7)
  AND ((a.metric_value-b.metric_value) is null
    or (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 > 0.01)
order by a.customer_type,a.date_value;

INSERT INTO table asp_digital_adoption_monitor_outlier PARTITION(run_date)
select 'monthly atom_call vs da_daily' as source_table,
       a.date_value,
       a.customer_type,
       '' as application_name,
       a.metric_name,
       a.metric_value as da_metric_value,
       b.metric_value as call_metric_value,
       a.metric_value-b.metric_value as diff,
       (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 as diff_pct,
       '${env:CURRENT_TIME}' as run_time,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
FROM
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='atom_cqe_btm'
    and metric_name='digital_adoption') a
FULL Join
(SELECT date_value,
        customer_type,
        metric_name,
        metric_value
   FROM asp_digital_adoption_monitor_da
  where source_table='da_daily'
    and metric_name='digital_adoption') b
     ON a.date_value = b.date_value
    and a.metric_name = b.metric_name
    and a.customer_type=b.customer_type
  where SUBSTRING(a.date_value, 1, 7)=substring(add_months('${env:CURRENT_DAY}',-1),1,7)
    aND ((a.metric_value-b.metric_value) is null
     or (a.metric_value-b.metric_value)/(a.metric_value+b.metric_value)/2 > 0.01)
order by a.customer_type,a.date_value;
