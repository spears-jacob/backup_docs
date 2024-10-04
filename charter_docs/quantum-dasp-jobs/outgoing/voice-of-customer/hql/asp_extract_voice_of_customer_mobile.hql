USE ${env:DASP_db};

set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.exec.max.dynamic.partitions.pernode=800;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR  ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

SELECT '***** getting voice of customer - mobile data ******'
;

-- daily insert of last login data for account number
WITH CTE_summary AS
(
SELECT epoch_timestamp(received__timestamp,'America/Denver') as datetime_denver
      ,partition_date_hour_utc
      ,visit__account__enc_mobile_account_number AS mobile_account_number_enc
      ,MAX(b.phone_line_number_aes256) AS mobile_device_number_enc --captures only one phone number per account
      ,visit__visit_id AS visit_id
      ,concat_ws(' ', state__view__current_page__components__standardized_name) AS support_page
      ,REVERSE(SPLIT(REVERSE(concat_ws(' ',state__view__current_page__components__standardized_name)),' ')[1]) AS final_supportPage
      ,epoch_converter(received__timestamp,'America/Denver') as denver_date
FROM `${env:CQE}` a
LEFT JOIN `${env:MVNO_A}` b ON a.visit__account__enc_mobile_account_number = b.mobile_account_number_aes256
WHERE ((partition_date_hour_utc >= '${env:START_DATE_TZ}'
     and partition_date_hour_utc <  '${env:END_DATE_TZ}')
    AND visit__application_details__application_name IN('SpecNet'))
AND state__view__current_page__page_name = 'supportArticle'
AND visit__account__details__service_subscriptions['mobile'] = 'ACTIVE'
AND visit__account__enc_account_number IS NOT NULL
and visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
AND state__view__current_page__enc_page_title IS NOT NULL
AND visit__account__enc_mobile_account_number IS NOT NULL
AND message__name = 'pageView'
GROUP BY epoch_converter(received__timestamp,'America/Denver')
        ,epoch_timestamp(received__timestamp,'America/Denver')
        ,partition_date_hour_utc
        ,visit__account__enc_account_number
        ,visit__account__enc_mobile_account_number
        ,visit__visit_id
        ,state__view__current_page__components__standardized_name
)

INSERT OVERWRITE TABLE asp_extract_voice_of_customer_mobile PARTITION (denver_date)

SELECT datetime_denver
      ,mobile_account_number_enc
      ,mobile_device_number_enc
      ,final_supportPage
      ,denver_date
FROM CTE_summary
WHERE support_page LIKE ('%/support/mobile/%')
AND mobile_device_number_enc IS NOT NULL
;
