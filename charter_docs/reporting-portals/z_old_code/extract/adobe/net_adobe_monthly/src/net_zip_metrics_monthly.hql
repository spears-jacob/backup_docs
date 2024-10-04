SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-- Insertion into net_zip_metrics of aggregated data, grouped by year_month, zip_code, dma, region.

INSERT INTO TABLE net_zip_metrics_monthly 
SELECT 
    concat(year(partition_date),'-',lpad(month(partition_date),2,"0")) as year_month,
    visit__location__enc_zip_code as zip_code,
    visit__account__details ["0"] as dma,
    visit__location__region as location,
    COUNT(distinct visit__account__enc_account_number) as unique_hh,  -- unique households
    COUNT(distinct visit__visit_id) as unique_visits,  -- unique visits
    COUNT(distinct visit__device__enc_uuid) as unique_visitors,  -- unique visitors
    size(collect_set(if(message__name ='login', visit__device__enc_uuid, NULL))) as unique_logged_in_visitors,  -- unique vizitors logged in
    sum(if(message__category ='Page View',1,0 )) as total_page_views  -- total page views
FROM 
    net_events
WHERE 
    partition_date BETWEEN '${hiveconf:START_DATE}' AND '${hiveconf:END_DATE}'
GROUP BY 
    concat(year(partition_date),'-',lpad(month(partition_date),2,"0")),
    visit__location__enc_zip_code, 
    visit__account__details ["0"], 
    visit__location__region;
