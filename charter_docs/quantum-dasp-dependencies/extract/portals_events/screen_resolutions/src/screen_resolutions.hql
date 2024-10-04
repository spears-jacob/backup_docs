USE ${env:ENVIRONMENT};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;

SELECT '***** inserting data into venona_screen_resolutions_visits table ******'
;

INSERT OVERWRITE TABLE venona_screen_resolutions_visits PARTITION (date_denver)
SELECT  
screen_resolution,
application_name,
application_type,
operating_system,
device_type,
width,
height,
COUNT(visit__visit_id) as visits,
date_denver
FROM (SELECT  
    epoch_converter(received__timestamp,'America/Denver') as date_denver,
    visit__visit_id,
    visit__screen_resolution AS screen_resolution,
    UPPER(visit__application_details__application_name) AS application_name,
    visit__application_details__application_type AS application_type,
    visit__device__operating_system AS operating_system,
    UPPER(visit__device__device_type) AS device_type,
    split(visit__screen_resolution, 'x')[0] AS width,
    split(visit__screen_resolution, 'x')[1] AS height 
    FROM core_v_venona_events 
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc < '${env:END_DATE_TZ}'
    AND (split(visit__screen_resolution, 'x')[0] > 0 OR split(visit__screen_resolution, 'x')[1] > 0)
    GROUP BY 
    epoch_converter(received__timestamp,'America/Denver'),
    visit__visit_id,
    visit__screen_resolution,
    UPPER(visit__application_details__application_name),
    visit__application_details__application_type,
    visit__device__operating_system,
    UPPER(visit__device__device_type)) a
GROUP BY 
screen_resolution,
application_name,
application_type,
operating_system,
device_type,
width,
height, 
date_denver;
        


SELECT '***** data insert into venona_screen_resolutions complete ******'
        ;
