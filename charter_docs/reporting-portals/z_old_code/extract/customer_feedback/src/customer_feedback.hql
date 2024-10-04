USE ${env:ENVIRONMENT};

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.support.concurrency=false;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT '***** inserting data into quantum_customer_feedback table *****'
;

INSERT OVERWRITE TABLE quantum_customer_feedback PARTITION (DateDenver)
SELECT   UserFeedback,
    FeedbackCategory,
    FeedbackForm,
    footprint,
    OS,
    VisitId,
    ThirdPartySessionID,
    ApplicationName,
    ApplicationType,
    FeedbackCount,
    DateDenver
FROM(SELECT operation__user_entry__enc_feedback AS UserFeedback,
      operation__user_entry__category AS FeedbackCategory,
      operation__user_entry__entry_type as FeedbackForm,
      visit__account__details__mso AS footprint,
      visit__device__operating_system AS OS,
      visit__visit_id AS VisitId,
      operation__third_party__session_id AS ThirdPartySessionID,
      visit__application_details__application_name AS ApplicationName,
      visit__application_details__application_type AS ApplicationType,
      COUNT(*) AS FeedbackCount,
      epoch_converter(received__timestamp,'America/Denver') as DateDenver
   FROM asp_v_venona_events_portals
   WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND (visit__visit_id != ''
   OR visit__visit_id IS NOT NULL)
   AND visit__application_details__application_name IN ('MySpectrum', 'SMB', 'SpecNet')
   AND (operation__user_entry__enc_feedback != ''
   OR operation__user_entry__enc_feedback IS NOT NULL)
   GROUP BY epoch_converter(received__timestamp,'America/Denver'),
         visit__visit_id,
        operation__third_party__session_id,
        visit__account__details__mso,
        visit__device__operating_system,
        visit__application_details__application_name,
        visit__application_details__application_type,
        operation__user_entry__category,
        operation__user_entry__entry_type,
        operation__user_entry__enc_feedback
   ) AS a
UNION
SELECT   UserFeedback,
    FeedbackCategory,
    FeedbackForm,
    footprint,
    OS,
    VisitId,
    ThirdPartySessionID,
    ApplicationName,
    ApplicationType,
    FeedbackCount,
    DateDenver
FROM(SELECT epoch_converter(received__timestamp,'America/Denver') as DateDenver,
      operation__user_entry__enc_feedback AS UserFeedback,
      operation__user_entry__category AS FeedbackCategory,
      operation__user_entry__entry_type as FeedbackForm,
      visit__account__details__mso AS footprint,
      visit__device__operating_system AS OS,
      visit__visit_id AS VisitId,
      operation__third_party__session_id AS ThirdPartySessionID,
      visit__application_details__application_name AS ApplicationName,
      visit__application_details__application_type AS ApplicationType,
      COUNT(*) AS FeedbackCount
   FROM asp_v_venona_events
   WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND (visit__visit_id != ''
   OR visit__visit_id IS NOT NULL)
   AND visit__application_details__application_name in ('OneApp','SpecU')
   AND (operation__user_entry__enc_feedback != ''
   OR operation__user_entry__enc_feedback IS NOT NULL)
   GROUP BY epoch_converter(received__timestamp,'America/Denver'),
         visit__visit_id,
        operation__third_party__session_id,
        visit__account__details__mso,
        visit__device__operating_system,
        visit__application_details__application_name,
        visit__application_details__application_type,
        operation__user_entry__category,
        operation__user_entry__entry_type,
        operation__user_entry__enc_feedback
   ) AS b
;


SELECT '***** data insert into quantum_customer_feedback complete *****'
;
