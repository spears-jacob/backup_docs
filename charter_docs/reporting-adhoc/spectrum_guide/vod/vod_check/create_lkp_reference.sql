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

---Tell me if people have null nc date

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
