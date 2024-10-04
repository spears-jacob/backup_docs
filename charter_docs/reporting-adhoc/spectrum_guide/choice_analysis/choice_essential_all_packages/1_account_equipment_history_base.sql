SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

SET BEGIN_DATE = '2017-09-01';
SET END_DATE = '2017-09-30';

-- ============
-- == ACCOUNT HISTORY: accounts present on the first and last day of the month
-- ============

DROP TABLE IF EXISTS test_tmp.account_history_base_1;
CREATE TABLE test_tmp.account_history_base_1 AS
SELECT
        -- ah.partition_date_time,
        ah.account__number_aes256,
        ah.system__kma_desc,
        ah.product__package_category,
        ah.product__video_package_type
FROM
        prod.account_history ah
WHERE
        -- ah.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
        ah.partition_date_time IN (CONCAT(SUBSTRING(partition_date_time,0,7), '-', '01'), LAST_DAY(partition_date_time))
   --  AND ac.account__number_aes256 IN ('QF0ENBrWzKsZ/g7/As0WQn4yqF9FWG8Vw3ERG/xcFtY=', '+/oiOsUEbVvis+88fuJhQzk7CzoDGlZyUNZWNcEpObM=', 'lHQLC5g1xQmmLG09AGKO/TKszA8Pu4e3srcXc+YMEH8=')
    AND ah.account__type IN ('SUBSCRIBER')
    AND ah.customer__type IN ('Residential')
    AND ah.meta__file_type IN ('Residential')
    ;

-- ============
-- == EQUIPMENT HISTORY: search for accounts present on first and last day of the month
-- ============
DROP TABLE IF EXISTS test_tmp.account_equipment_base_1;
CREATE TABLE test_tmp.account_equipment_base_1 AS

SELECT
  partition_date_time,
  account__number_aes256,
  IF(array_contains(collect_set(equipment__category_name),'HD/DVR Converters'),'DVR', 'NON-DVR') AS account__category
FROM
  prod.account_equipment_history eqp
WHERE
      partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
  AND partition_date_time IN (CONCAT(SUBSTRING(partition_date_time,0,7), '-', '01'), LAST_DAY(partition_date_time))
  AND account__number_aes256 IN (SELECT account__number_aes256 FROM test_tmp.account_history_base_1) --> only pull equipment for records in question
  AND equipment__category_name IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
  AND equipment__derived_mac_address_aes256 IS NOT NULL --> Stanard STBs should have a populated mac id
 --  AND account__number_aes256 IN ('lHQLC5g1xQmmLG09AGKO/TKszA8Pu4e3srcXc+YMEH8=')
GROUP BY
  partition_date_time,
  account__number_aes256
;
