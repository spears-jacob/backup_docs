use prod;

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

---------------------------------------------
--OS Detail portals instances
---------------------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   count(1)           AS value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'asp'              AS platform,
   'instances'        as unit,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end           AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end           as source_table
FROM prod.asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND LOWER(visit__application_details__application_name) IN ('myspectrum','specnet','smb'))
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end,
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')),
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end ;

---------------------------------------------
--OS Detail portals visits
---------------------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   SIZE(COLLECT_SET(visit__visit_id)) as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'asp'              AS platform,
   'visits'           as unit,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end           AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end           as source_table
  FROM prod.asp_v_venona_events_portals
  WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc < '${env:END_DATE_TZ}'
     AND LOWER(visit__application_details__application_name) IN ('myspectrum','specnet','smb'))
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end,
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')),
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end ;

---------------------------------------------
--OS Detail portals visitors
---------------------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   COUNT(DISTINCT visit__device__enc_uuid)  as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'asp'              AS platform,
   'visitors'         as unit,
    CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
         THEN 'app'
         WHEN LOWER(visit__application_details__application_name) = 'specnet'
         THEN 'resi'
         WHEN LOWER(visit__application_details__application_name) = 'smb'
         THEN 'smb'
         else 'unknown'
         end           AS domain,
    COALESCE (visit__account__details__mso,'Unknown') AS company,
    date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
    CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
         THEN 'asp_v_venona_events_portals_msa'
         WHEN LOWER(visit__application_details__application_name) = 'specnet'
         THEN 'asp_v_venona_events_portals_specnet'
         WHEN LOWER(visit__application_details__application_name) = 'smb'
         THEN 'asp_v_venona_events_portals_smb'
         else 'unknown'
         end           as source_table
  FROM prod.asp_v_venona_events_portals
  WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc < '${env:END_DATE_TZ}'
     AND LOWER(visit__application_details__application_name) IN ('myspectrum','specnet','smb'))
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end,
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')),
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end ;

---------------------------------------------
--OS Detail portals households
---------------------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   SIZE(COLLECT_SET(visit__account__enc_account_number)) as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'asp'              AS platform,
   'households'       as unit,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end           AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end           as source_table
  FROM prod.asp_v_venona_events_portals
  WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc < '${env:END_DATE_TZ}'
     AND LOWER(visit__application_details__application_name) IN ('myspectrum','specnet','smb'))
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'app'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'resi'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'smb'
        else 'unknown'
        end,
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')),
   CASE WHEN LOWER(visit__application_details__application_name) = 'myspectrum'
        THEN 'asp_v_venona_events_portals_msa'
        WHEN LOWER(visit__application_details__application_name) = 'specnet'
        THEN 'asp_v_venona_events_portals_specnet'
        WHEN LOWER(visit__application_details__application_name) = 'smb'
        THEN 'asp_v_venona_events_portals_smb'
        else 'unknown'
        end ;

----------------------------------
--OS Detail SpecMobile instances
----------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   count(1)           AS value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'specmobile'       AS platform,
   'instances'        as unit,
   'specmobile'       AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   'asp_v_venona_events_specmobile'         AS source_table
FROM prod.asp_v_venona_events_specmobile
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
GROUP BY
  SPLIT(visit__device__operating_system, '[\.]') [0],
  COALESCE (visit__account__details__mso,'Unknown'),
  date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver'));

----------------------------------
--OS Detail SpecMobile visits
----------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   SIZE(COLLECT_SET(visit__visit_id)) as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'specmobile'       AS platform,
   'visits'           as unit,
   'specmobile'       AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   'asp_v_venona_events_specmobile'         AS source_table
FROM prod.asp_v_venona_events_specmobile
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver'));

----------------------------------
--OS Detail SpecMobile visitors
----------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   SIZE(COLLECT_SET(visit__device__enc_uuid)) as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'specmobile'       AS platform,
   'visitors'         as unit,
   'specmobile'       AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   'asp_v_venona_events_specmobile'         AS source_table
FROM prod.asp_v_venona_events_specmobile
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver'));

----------------------------------
--OS Detail SpecMobile households
----------------------------------
INSERT OVERWRITE TABLE asp_metrics_detail_os_monthly
PARTITION(unit,domain,company,year_month_denver,source_table)
SELECT
   SIZE(COLLECT_SET(visit__account__enc_account_number)) as value,
   SPLIT(visit__device__operating_system, '[\.]') [0] as detail,
   'OS Name'          as metric,
   'specmobile'       AS platform,
   'households'       as unit,
   'specmobile'       AS domain,
   COALESCE (visit__account__details__mso,'Unknown') AS company,
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver,
   'asp_v_venona_events_specmobile'         AS source_table
FROM prod.asp_v_venona_events_specmobile
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
GROUP BY
   SPLIT(visit__device__operating_system, '[\.]') [0],
   COALESCE (visit__account__details__mso,'Unknown'),
   date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver'));
