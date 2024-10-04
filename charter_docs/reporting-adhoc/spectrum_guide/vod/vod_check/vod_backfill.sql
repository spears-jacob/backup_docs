USE ${env:ENVIRONMENT};

CONTEXT:
  1) Rollout was down for a week in the middle of August.
    During this time, the sgr_DMA_Final_Report table was not refreshed but the vod_stream_session_usage_agg_daily table continued to pull from the non-refreshed sgr_DMA_Final_Report rollout table
  2) You pull two dates from sgr_DMA_Final_Report rollout table: nc_date and nc_install_date
  3) nc_date should be populated for everyone every day in the sgr_DMA_Final_Report table as long as the customer is a customer (i.e. not disconnected)
  4) nc_install_date is only populated for spectrum guide customers and not legacy customers
  5) We have two guide types: Spectrum Guide (new) vs Legacy (i-guide - old)
  6) The sgr_DMA_Final_Report table is dropped and recreated every day - no partitions
  7) Tableau report shows drastic drop in VOD usage: http://22.85.195.11/#/views/SpectrumGuideMonthlyDashboard-Updated-VOD/Sheet12?:iid=1

THEORIES:
  1) nc_date is taken from sgr_DMA_Final_Report. The nc_date is used to calculate customer tenure. vod_stream_session_usage_agg_daily table’s partition date minus a customer’s inception is equal to tenure
    If you could not pull the nc_date field then we might have customers that do not get the nc_date populated.
  2) Records are being incorrectly dropped from the SGR_DMA_ACCOUNT_AGG table due to filters and causing downstream issues

IMPACTS:
  1) If new customers were on boarded while rollout was down, then the nc_date would be null and those accounts would be dropped from the monthly table b/c the nc_date would not be populated for an entire month.
    But b/c rollout went down in the middle of august, this should not have an impact b/c accounts on boarded in the middle of the month would not be counted in the monthly anyway.
  2) The nc_date would explain why you see a big discrepancy in the VOD metrics when bucketed before and after 90 days.
    Caution: Keep in mind that the story did not improve much when you removed this from your view in tableau.

CONCLUSIONS:
  1) We recreated the nc_date logic from the sgr_DMA_Final_Report table using the LKP logic below. Then we reran the vod_stream_session_usage_agg_daily table for all of august to account for the potential missing nc_date. We did not observe a significant impact.
    DAILY AZKBAN BACKFILL: https://pi-datamart-proxy.corp.chartercom.com/azkaban_dev/executor?execid=58776#jobslist
    MONTHLY AZKBAN BACKFILL: https://pi-datamart-proxy.corp.chartercom.com/azkaban_dev/executor?execid=58792#jobslist

  2) We have not changed the rollout account and equipment filters used to calcaulte customers.

CODE:

VOD BASE TABLE:
  1) LINE 38: Uses NC DATE from the sgr_DMA_Final_Report table and whatever the run date for the vod_stream_session_usage_agg_daily table is to calculate customer tenure on a given day
  2) LINES 45 to 60: provide NC DATE
  https://gitlab.spectrumxg.com/product-intelligence/reporting-accounts-specguide/blob/master/accounts_flow/vod_concurrent_stream_session/src/hv_load_vod_stream_session_usage_agg_daily.hql

VOD MONTHLY TABLE:
  1) LINES 6 to 23: A Customer must be active for the entire month
  2) LINE 15:
    -If a Customer has a legacy guide type, then pull that record in
    -If a Customer has a SG guide type, then it must also have an sg_new_connect_date_on_account populated for an entire month
    -An sg_new_connect_date_on_account populated for an entire month means that every day the vod_stream_session_usage_agg_daily table ran, the customer was found in the rollout tables.
    -As a result, we could get an nc_date from the rollout tables and calculate a sg_new_connect_date_on_account.
    https://gitlab.spectrumxg.com/product-intelligence/reporting-accounts-specguide/blob/master/accounts_flow/vod_concurrent_stream_session/src/hv_load_vod_stream_session_usage_agg_monthly.hql

ROLLOUT NC DATE LOGIC BUILD:
  1) Lines 217 to 251
    nc_date is populated for a customer that:
      * non-charter customers before and signed up for video service for the first time
      * were charter customers but did not have video service before
    https://gitlab.spectrumxg.com/product-intelligence/reporting-accounts-specguide/blob/master/accounts_flow/sg_rollout_final_report_prod/src/SGR_DMA/Final_Report_table.hql

BASE ROLLOUT TABLE:
  1) Lines 186 to 193: are the key filters that can eliminate customers from the SGR_DMA_ACCOUNT_AGG table. These have not changed so I do not impact it to impact records
  https://gitlab.spectrumxg.com/product-intelligence/reporting-accounts-specguide/blob/master/accounts_flow/sg_rollout_final_report_prod/src/SGR_DMA/SGR_DMA_account_agg.hql




  -- Tell me the NC date for everyone
  DROP TABLE IF EXISTS TEST_TMP.acct_video_partition_dates;
  CREATE TABLE TEST_TMP.acct_video_partition_dates as
  SELECT
         a.account__number_aes256
        ,a.partition_date_time as partition_date
        ,max(a.product__is_video_package) as is_video
  FROM
        prod.account_history a

  GROUP BY
        a.account__number_aes256
        ,a.partition_date_time;

  DROP TABLE IF EXISTS TEST_TMP.acct_video_history;
  CREATE TABLE TEST_TMP.acct_video_history as
  SELECT
    c.account__number_aes256,
    c.partition_date,
    c.is_video,
    c.prev_is_video
  FROM
        (select a.account__number_aes256
               ,a.partition_date
               ,a.is_video
               ,b.is_video prev_is_video
          from TEST_TMP.acct_video_partition_dates a
          left join TEST_TMP.acct_video_partition_dates b
                 on (a.account__number_aes256 = b.account__number_aes256
                and  b.partition_date = date_add(a.partition_date,-1))) c
  WHERE
    c.is_video = TRUE AND (c.prev_is_video = FALSE OR c.prev_is_video is NULL)
  ;


  --NC INSTALL DATE - TELL ME WHEN SERVICE WAS ACTUALLY INSTALLED
  DROP TABLE IF EXISTS TEST_TMP.sgr_new_connects_lkp;
  CREATE TABLE TEST_TMP.sgr_new_connects_lkp as

  SELECT account_num,
  job_scheduled_dt
  FROM prod_lkp.sgr_new_connects_accts

  GROUP BY account_num,
  job_scheduled_dt
  ;

  --LKP REFERENCE: FOR EVERY SG NEW CONNECT VIDEO ACCOUNT, TELL ME WHEN THEY GOT VIDEO SERVICE FOR THE FIRST TIME AND TELL ME WHEN THEY ACTUALLY GOT SERVICE INSTALLED IN THEIR HOMES
  --ACCOUNT HISTORY SHOULD BE YOUR DRIVER BECAUSE THAT HAS NC DATES FOR LEGACY AND NEW CONNECTS

  DROP TABLE IF EXISTS TEST_TMP.sgr_dma_final_report_adjusted_for_backfill;
  CREATE TABLE TEST_TMP.sgr_dma_final_report_adjusted_for_backfill as

  SELECT
    avh.account__number_aes256 AS account_num,
    MAX(avh.partition_date) nc_date,
    MAX(lkp.job_scheduled_dt) nc_install_date


  FROM test_tmp.acct_video_history avh
  LEFT JOIN test_tmp.sgr_new_connects_lkp lkp
  ON lkp.account_num = avh.account__number_aes256
  GROUP BY avh.account__number_aes256
  ;

  ---Tell me if people who have null nc date -- there should be zero

  SELECT
  CASE WHEN nc_date IS NULL THEN 'Y' ELSE 'N' END nc_date,
  CASE WHEN nc_install_date IS NULL THEN 'Y' ELSE 'N' END nc_install_date_is_null,
  count(*)
  FROM TEST_TMP.sgr_dma_final_report_adjusted_for_backfill
  GROUP BY
  CASE WHEN nc_date IS NULL THEN 'Y' ELSE 'N' END ,
  CASE WHEN nc_install_date IS NULL THEN 'Y' ELSE 'N' END
  ;
  -- SIZE(COLLECT_SET(account_num)) FROM TEST_TMP.sgr_dma_final_report_adjusted_for_backfill ;
=================VOD CODE

DROP TABLE IF EXISTS ${env:TMP_db}.EQUIPMENT_DATA_VOD;
CREATE TABLE ${env:TMP_db}.EQUIPMENT_DATA_VOD AS
SELECT
    partition_date_time,
  	account__number_aes256,
  	collect(named_struct('equipment__derived_mac_address_aes256',equipment__derived_mac_address_aes256)) as equipment_data,
  	SUM(CASE WHEN equipment__category_name like '%DVR%' THEN 1 ELSE 0 END) AS dvr_box_count
FROM account_equipment_history equip
WHERE equip.partition_date_time in (${hiveconf:DATE_LIST})
	  AND equipment__derived_mac_address_aes256 is not NULL
    AND equipment__category_name IN ('HD/DVR Converters','Standard Digital Converters','HD Converters')
GROUP BY
    account__number_aes256,
    partition_date_time;

DROP TABLE IF EXISTS ${env:TMP_db}.ACCOUNT_LEVEL_DATA;
CREATE TABLE ${env:TMP_db}.ACCOUNT_LEVEL_DATA AS
SELECT
    DISTINCT
    accts.partition_date_time,
    accts.account__type,
    accts.account__number_aes256,
    CASE     -- kma aliasing
      WHEN system__kma_desc IN ('Michigan (KMA)','Michigan') THEN 'Michigan'
      WHEN system__kma_desc IN ('Central States (KMA)','Central States') THEN 'Central States'
      WHEN system__kma_desc IN ('Minnesota/Nebraska (KMA)','Minnesota') THEN 'Minnesota'
      WHEN system__kma_desc IN ('Northwest (KMA)','Pacific Northwest','Sierra Nevada') THEN 'Pacific Northwest/ Sierra Nevada'
      WHEN system__kma_desc IN ('Wisconsin  (KMA)','Wisconsin') THEN 'Wisconsin'
      ELSE system__kma_desc
    END AS system__kma_desc,
    IF(catg.dvr_box_count>0, 'DVR','NON-DVR') as account_category,			-- account_category from equipment history
    CASE 																	-- Guide type using spectrum_guide flag + is video condition at the end of script
      WHEN accts.product__is_spectrum_guide = TRUE THEN 'SG'
      ELSE  'LEGACY'
    END AS GUIDE_TYPE,
    DATEDIFF(accts.PARTITION_DATE_TIME,NC_DATE)	as days_after_customer_connect,
    sgr.nc_date as CUSTOMER_CONNECT_DATE,	                                    -- Storing these dates as well for raw data reference as sgr_dma reloads each day
    sgr.nc_install_date as sg_new_connect_date_on_account,
    catg.mac_address as mac_address                    -- mac address from equipment history
FROM
  	account_history accts
LEFT JOIN 																	--Picking up the max sg push dt AND nc_date as there can instances of multiple push dates
  	(
    SELECT
  		account_num,
  		max(nc_install_date) nc_install_date,
  		max(nc_date) nc_date
  	-- FROM
  	-- 	sgr_dma_final_report
    FROM
      ${env:TMP_db}.sgr_dma_final_report_adjusted_for_backfill
  	GROUP BY
        account_num) sgr
ON accts.account__number_aes256=sgr.account_num



LEFT JOIN 																	 --Exploding the array of equipment here along with dvr counts
	  (
	  SELECT
      partition_date_time,
  	  equipment_mac_data.equipment__derived_mac_address_aes256 as mac_address,
  	  account__number_aes256,
  	  dvr_box_count
	  FROM
	    ${env:TMP_db}.EQUIPMENT_DATA_VOD  lateral view explode(equipment_data) temp as equipment_mac_data)catg
ON accts.account__number_aes256 = catg.account__number_aes256
  AND accts.partition_date_time = catg.partition_date_time
WHERE
  accts.partition_date_time in (${hiveconf:DATE_LIST})
  AND accts.product__is_video_package = TRUE
  AND accts.customer__disconnect_date is NULL
  AND accts.customer__type = 'Residential'
  AND accts.system__kma_desc in ('Central States (KMA)','Central States','Michigan (KMA)','Michigan','Northwest (KMA)','Pacific Northwest','Sierra Nevada','Wisconsin  (KMA)','Wisconsin','Minnesota/Nebraska (KMA)','Minnesota','Kansas City / Lincoln');

SET hive.execution.engine=mr;
SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS ${env:TMP_db}.VOD_VIEW_AGG;
CREATE TABLE ${env:TMP_db}.VOD_VIEW_AGG AS
SELECT
  acct.partition_date_time ,
  acct.account__type ,
  acct.account__number_aes256,
  acct.system__kma_desc ,
  acct.account_category ,
  acct.guide_type ,
  acct.days_after_customer_connect,
  acct.customer_connect_date,
  CASE
    WHEN vod.title__service_category_name in ('FHD','FOD') THEN 'Free'
    WHEN vod.title__service_category_name in ('AOD','EHD','MOD','MHD','EOD','AHD') THEN 'Transactional'
    WHEN vod.title__service_category_name in ('SVOD','POD','PHD') THEN 'Premium'
    WHEN vod.title__service_category_name in ('Undefined') THEN 'Undefined'
    ELSE 'Undefined'
  END as title__service_category_name_group,
  SUM(vod.session__viewing_in_s) as total_view_duration_in_s,
  CASE
    WHEN count(distinct vod.session__vod_lease_sid) != 0 THEN count(distinct vod.session__vod_lease_sid)
    ELSE count(*)
  END as total_number_of_views
FROM
  vod_concurrent_stream_session_event vod
JOIN
  ${env:TMP_db}.ACCOUNT_LEVEL_DATA acct
ON vod.session__equipment_mac_id_aes256 = acct.mac_address AND vod.partition_date_time=acct.partition_date_time
WHERE vod.title__service_category_name != 'ADS'
AND vod.partition_date_time in (${hiveconf:DATE_LIST})
AND vod.session__is_error = FALSE
AND vod.session__viewing_in_s < 14400
AND vod.session__viewing_in_s >0
AND vod.asset__class_code = 'MOVIE'
AND vod.session__vod_lease_sid IS NOT NULL
GROUP BY
  acct.partition_date_time ,
  acct.account__type ,
  acct.account__number_aes256,
  acct.system__kma_desc ,
  acct.account_category ,
  acct.guide_type ,
  acct.days_after_customer_connect,
  acct.customer_connect_date,
  CASE
    WHEN vod.title__service_category_name in ('FHD','FOD') THEN 'Free'
    WHEN vod.title__service_category_name in ('AOD','EHD','MOD','MHD','EOD','AHD') THEN 'Transactional'
    WHEN vod.title__service_category_name in ('SVOD','POD','PHD') THEN 'Premium'
    WHEN vod.title__service_category_name in ('Undefined') THEN 'Undefined'
    ELSE 'Undefined'
  END ;


--Vod daily usage per account is stored in this table

INSERT OVERWRITE TABLE VOD_ACCT_LEVEL_USAGE_DAILY_AGG PARTITION(partition_date_time)
SELECT
  acct.account__type ,
  acct.account__number_aes256,
  acct.system__kma_desc ,
  acct.account_category ,
  acct.guide_type ,
  acct.days_after_customer_connect,
  acct.customer_connect_date,
  acct.sg_new_connect_date_on_account,
  SUM(total_view_duration_in_s) as total_view_duration_in_s,
  SUM(IF(title__service_category_name_group='Free',total_view_duration_in_s,0)) as free_view_duration_in_s,
  SUM(IF(title__service_category_name_group='Transactional',total_view_duration_in_s,0)) as trans_view_duration_in_s,
  SUM(IF(title__service_category_name_group='Premium',total_view_duration_in_s,0)) as prem_view_duration_in_s,
  SUM(total_number_of_views) as total_number_of_views,
  SUM(IF(title__service_category_name_group='Free',total_number_of_views,0)) as free_views,
  SUM(IF(title__service_category_name_group='Transactional',total_number_of_views,0)) as trans_views,
  SUM(IF(title__service_category_name_group='Premium',total_number_of_views,0)) as prem_views,
  acct.partition_date_time
FROM
  (SELECT DISTINCT
    acct.partition_date_time ,
    acct.account__type ,
    acct.account__number_aes256,
    acct.system__kma_desc ,
    acct.account_category ,
    acct.guide_type ,
    acct.days_after_customer_connect,
    acct.customer_connect_date,
    acct.sg_new_connect_date_on_account
   FROM
    ${env:TMP_db}.ACCOUNT_LEVEL_DATA acct)acct
LEFT JOIN
  ${env:TMP_db}.VOD_VIEW_AGG vod
ON acct.account__number_aes256=vod.account__number_aes256
  AND acct.partition_date_time=vod.partition_date_time
  AND acct.account__type=vod.account__type
  AND acct.system__kma_desc=vod.system__kma_desc
  AND acct.account_category=vod.account_category
  AND acct.guide_type=vod.guide_type
GROUP BY
  acct.account__type ,
  acct.account__number_aes256,
  acct.system__kma_desc ,
  acct.account_category ,
  acct.guide_type ,
  acct.days_after_customer_connect,
  acct.customer_connect_date,
  acct.sg_new_connect_date_on_account,
  acct.partition_date_time ;
