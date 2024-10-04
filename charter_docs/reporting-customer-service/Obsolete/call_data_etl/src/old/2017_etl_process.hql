--Debugging echo
SELECT "*Begin ETL script";

USE ${env:ENVIRONMENT};

SELECT "**Begin loading temporary table ${env:TMP_db}.steve_call_data_2017_tmp";

TRUNCATE TABLE ${env:ENVIRONMENT}.steve_call_data_2017_preload;

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;

set hive.auto.convert.join=false;

DROP TABLE IF EXISTS ${env:TMP_db}.steve_call_data_2017_callstartendtimes;
CREATE TABLE ${env:TMP_db}.steve_call_data_2017_callstartendtimes AS
SELECT CallInbKey
,min(CallStartDatetime_NewYork) CallStartTime_new
,max(CallEndDatetime_NewYork) CallEndTime_new
FROM ${env:TMP_db}.steve_call_data_2017_managed
GROUP BY CallInbKey
;

DROP TABLE IF EXISTS ${env:TMP_db}.steve_call_data_2017_accounts;
CREATE TABLE ${env:TMP_db}.steve_call_data_2017_accounts AS
SELECT *
FROM
(
  SELECT DISTINCT CallInbKey, AcctNum
  FROM
  (
    SELECT CallInbKey, AcctNum, ROW_NUMBER() OVER (PARTITION BY CallInbKey ORDER BY lasthndlsegfl, CASE WHEN lower(dev.aes_decrypt256(acctnum)) = 'unknown' THEN 9999 ELSE 1 END
    ,CallEndDatetime_NewYork DESC,SegNum) RANKING
    FROM ${env:TMP_db}.steve_call_data_2017_managed
    WHERE lower(${env:ENVIRONMENT}.aes_decrypt256(AcctNum)) <> 'unknown'
    --AND lasthndlsegfl = 1
    GROUP BY CallInbKey, AcctNum, CallEndDatetime_NewYork, SegNum
  ) dt
  WHERE RANKING = 1
) accounts
;




--CREATE cs_chr_account_ids
DROP TABLE IF EXISTS ${env:ENVIRONMENT}.steve_chr_account_ids;
CREATE TABLE ${env:ENVIRONMENT}.steve_chr_account_ids
(
  spa string
  ,account_number string
  ,account_id string
  ,customer_type string
  ,customer_subtype string
  ,company_code string
)
PARTITIONED BY
(
  partition_date string
)
;

INSERT INTO ${env:ENVIRONMENT}.steve_chr_account_ids PARTITION (partition_date)
SELECT distinct spa, account_number, account_id, upper(customer__type), upper(customer__subtype), company_code, partition_date
FROM
(
  SELECT spa, account_number, account_id, customer__type, customer__subtype, company_code, partition_date
  ,RANK() OVER (PARTITION BY SPA, ACCOUNT_NUMBER ORDER BY CASE WHEN META__FILE_TYPE = CUSTOMER__TYPE THEN 1 ELSE 2 END, CUSTOMER__TYPE) AS RANKING
  FROM
    (
      SELECT DISTINCT CONCAT(SUBSTRING(CONCAT('0000',SYSTEM__SYS),LENGTH(CONCAT('0000',SYSTEM__SYS))-3)
      ,SUBSTRING(CONCAT('0000',SYSTEM__PRIN),LENGTH(CONCAT('0000',SYSTEM__PRIN))-3)
      ,SUBSTRING(CONCAT('0000',SYSTEM__AGENT),LENGTH(CONCAT('0000',SYSTEM__AGENT))-3)) SPA
      ,prod.aes_decrypt256(account__number_aes256) account_number
      ,prod.aes_decrypt256(account__id_aes256) account_id
      ,customer__type
      ,customer__subtype
      ,'CHR' as company_code
      ,to_DATE(partition_date_time) partition_date
      ,CASE WHEN META__FILE_TYPE = 'Commercial Business' THEN 'Commercial' ELSE META__FILE_TYPE END meta__file_type
      FROM PROD.ACCOUNT_history
        WHERE TO_DATE(PARTITION_DATE_TIME) >= '2017-01-01' AND TO_DATE(PARTITION_DATE_TIME) <= '2017-12-31'
    ) DT
) DT
WHERE RANKING = 1
;


CREATE TABLE ${env:ENVIRONMENT}.steve_account_current AS
SELECT spa, account_number, account_id, customer__type as customer_type, customer__subtype as customer_subtype, company_code, partition_date
FROM
(
  SELECT spa, account_number, account_id, customer__type, customer__subtype, company_code, partition_date
  ,RANK() OVER (PARTITION BY SPA, ACCOUNT_NUMBER ORDER BY CASE WHEN META__FILE_TYPE = CUSTOMER__TYPE THEN 1 ELSE 2 END, CUSTOMER__TYPE) RANKING
  FROM
    (
      SELECT DISTINCT CONCAT(SUBSTRING(CONCAT('0000',SYSTEM__SYS),LENGTH(CONCAT('0000',SYSTEM__SYS))-3)
      ,SUBSTRING(CONCAT('0000',SYSTEM__PRIN),LENGTH(CONCAT('0000',SYSTEM__PRIN))-3)
      ,SUBSTRING(CONCAT('0000',SYSTEM__AGENT),LENGTH(CONCAT('0000',SYSTEM__AGENT))-3)) SPA
      ,prod.aes_decrypt256(account__number_aes256) account_number
      ,prod.aes_decrypt256(account__id_aes256) account_id
      ,customer__type
      ,customer__subtype
      ,'CHR' as company_code
      ,to_DATE(partition_date_time) partition_date
      ,CASE WHEN META__FILE_TYPE = 'Commercial Business' THEN 'Commercial' ELSE META__FILE_TYPE END meta__file_type
      FROM PROD.account_current
    ) DT
) DT
WHERE RANKING = 1
;


DROP TABLE IF EXISTS ${env:TMP_db}.steve_call_data_2017_tmp;
--Initial query pulling from call data files in CTE
CREATE TABLE ${env:TMP_db}.steve_call_data_2017_tmp AS
SELECT
  a.CallId call_id
  ,to_utc_timestamp(CallStartTime_new,'America/New_York') call_start_datetime_utc
  ,to_utc_timestamp(CallEndTime_new,'America/New_York') call_end_datetime_utc

  ,cast('' as bigint) previous_call_time

  ,SegNum segment_number
  ,AnsDispoDesc segment_status_disposition

  ,to_utc_timestamp(a.CallStartDatetime_NewYork,'America/New_York') segment_start_datetime_utc



  ,CASE WHEN a.CallStartDatetime_NewYork = coalesce(endtimes.segment_end_datetime_new,a.CallEndDatetime_NewYork)
  THEN to_utc_timestamp(a.CallEndDatetime_NewYork,'America/New_York')
  ELSE
  cast(from_unixtime(unix_timestamp(to_utc_timestamp(coalesce(endtimes.segment_end_datetime_new,a.callenddatetime_newyork),'America/New_York'))-1) as timestamp)
  END
    AS segment_end_datetime_utc

  ,Product
  ,AcctId
  ,COALESCE(acct.AcctNum,a.acctnum) account_number
  ,CustAccountNo customer_account_number

  --Transform 1 and 0 to TRUE and FALSE booleans
  ,CASE WHEN TruckRollFl = "1" THEN TRUE ELSE FALSE END as truck_roll_fl

  ,NotesTxt notes_txt
  ,CallResDesc resolution_description
  ,CallCauseDesc cause_description
  ,CallIssDesc issue_description

  ,CompanyCode company_code
  ,ServiceCallTrackerID service_call_tracker_id
  ,CreatedOn created_on
  ,CreatedBy created_by
  ,PhoneNumberFromTracker phone_number_from_tracker
  ,CallType call_type
  ,SplitSumDesc split_sum_desc
  ,LocNm location_name
  ,carectrmgmtnm care_center_management_name
  ,JobRoleNm agent_job_role_name
  ,EffectiveHireDt agent_effective_hire_date
  ,MSOAgent agent_mso
  ,CustCallCntctInd customer_call_count_indicator
  ,CallOwner call_owner
  ,EDUID eduid
  ,a.CallInbKey call_inbound_key
  ,UniCallId unique_call_id
  ,CallsHndlFl call_handled_flag
  ,LastHndlSegFl last_handled_segment_flag
  --Assign call record_source from length of CallResDesc.
  --If length of CallResDesc is greater than 1, then Call Tracker, Else Switch
  ,CASE WHEN LENGTH(CallResDesc)>1
      THEN "Call Tracker"
      ELSE "Switch"
    END as record_source
  ,Updatetimestamp record_update_timestamp
FROM ${env:TMP_db}.steve_call_data_2017_managed a
LEFT JOIN ${env:TMP_db}.steve_call_data_2017_callstartendtimes b
  ON a.CallInbKey = b.CallInbKey
LEFT JOIN ${env:TMP_db}.steve_call_data_2017_accounts acct
  ON a.CallInbKey = acct.CallInbKey
LEFT JOIN
(
  SELECT callinbkey, callstartdatetime_newyork
    ,lead(callstartdatetime_newyork) OVER (PARTITION BY callinbkey ORDER BY callstartdatetime_newyork) AS segment_end_datetime_new
    FROM
    (
      SELECT DISTINCT callinbkey, callstartdatetime_newyork
      FROM ${env:TMP_db}.steve_call_data_2017_managed
    ) endtimes
) endtimes
  ON a.callinbkey = endtimes.CallInbKey
  AND a.callstartdatetime_newyork = endtimes.CallStartDatetime_NewYork
--  AND a.CallId = endtimes.CallId
  --AND unix_timestamp(a.CallStopDate_NewYork,'MM/dd/yyyy') = unix_timestamp(b.CallStopDate_NewYork,'MM/dd/yyyy')
;

SELECT "**End loading temporary table ${env:TMP_db}.steve_call_data_2017_tmp";

SELECT "**Begin loading table ${env:ENVIRONMENT}.steve_call_data_2017_preload";
--Combine data sets and perform date functions on call and segment start/end dates times
--Purposely storing multiple formats of datetime/timestamp to support downstream processing
INSERT INTO ${env:ENVIRONMENT}.steve_call_data_2017_preload PARTITION (call_end_date_utc)
SELECT
  call_inbound_key
  ,call_id
  ,substr(call_start_datetime_utc,0,10) call_start_date_utc
  ,substr(call_start_datetime_utc,12) call_start_time_utc
  ,substr(call_end_datetime_utc,12) call_end_time_utc
  ,call_start_datetime_utc
  ,call_end_datetime_utc
  ,prod.datetime_converter(cast(call_start_datetime_utc AS STRING),'UTC','UTC') call_start_timestamp_utc
  ,prod.datetime_converter(cast(call_end_datetime_utc AS STRING),'UTC','UTC') call_end_timestamp_utc
  ,cast('' as bigint) as previous_call_time

  ,concat(call_inbound_key,'-',call_id,'-',segment_number) segment_id
  ,segment_number
  ,segment_status_disposition
  ,substr(segment_start_datetime_utc,12) segment_start_time_utc
  ,substr(segment_end_datetime_utc,12) segment_end_time_utc
  ,segment_start_datetime_utc
  ,segment_end_datetime_utc
  ,prod.datetime_converter(cast(segment_start_datetime_utc AS STRING),'UTC','UTC') segment_start_timestamp_utc
  ,prod.datetime_converter(cast(segment_end_datetime_utc AS STRING),'UTC','UTC') segment_end_timestamp_utc
  ,abs(unix_timestamp(segment_start_datetime_utc)
  - unix_timestamp(segment_end_datetime_utc)) segment_duration_seconds
  ,abs(unix_timestamp(segment_start_datetime_utc)
  - unix_timestamp(segment_end_datetime_utc))/60 segment_duration_minutes

  -- Filter helps identify only handled call
  ,CASE WHEN customer_call_count_indicator = 1
           AND call_handled_flag = 1
           AND LOWER(call_owner) in ('customer operations')
           AND LOWER(segment_status_disposition) in ('answered by an agent')
           THEN 1 ELSE 0 END
  ,customer_call_count_indicator
  ,call_handled_flag
  ,call_owner
  ,product
  ,acctid
  ,cd.account_number
  ,customer_account_number
  ,CASE WHEN TRIM(UPPER(coalesce(ids.customer_type, ac.customer_type))) is null
          OR TRIM(UPPER(coalesce(ids.customer_type, ac.customer_type))) = 'N/A'
            THEN 'UNMAPPED'
          ELSE TRIM(UPPER(coalesce(ids.customer_type, ac.customer_type))) END as customer_type

  ,CASE WHEN TRIM(UPPER(coalesce(ids.customer_subtype, ac.customer_subtype))) is null
          OR TRIM(UPPER(coalesce(ids.customer_subtype, ac.customer_subtype))) = 'N/A'
            THEN 'UNMAPPED'
          ELSE TRIM(UPPER(coalesce(ids.customer_subtype, ac.customer_subtype))) END as customer_subtype
  ,truck_roll_fl
  ,notes_txt
  ,CASE WHEN resolution_description = '?' THEN NULL ELSE resolution_description END
  ,CASE WHEN cause_description = '?' THEN NULL ELSE cause_description END
  ,CASE WHEN issue_description = '?' THEN NULL ELSE issue_description END
  ,cd.company_code
  ,CASE WHEN service_call_tracker_id = '?' THEN NULL ELSE service_call_tracker_id END
  ,CASE WHEN created_on = '?' THEN NULL ELSE created_on END
  ,CASE WHEN created_by = '?' THEN NULL ELSE created_by END
  ,phone_number_from_tracker
  ,CASE WHEN call_type = '?' THEN NULL ELSE call_type END
  ,CASE WHEN split_sum_desc = '?' THEN NULL ELSE split_sum_desc END
  ,location_name
  ,care_center_management_name
  ,agent_job_role_name
  ,CASE WHEN agent_effective_hire_date = '1/1/1900' OR agent_effective_hire_date = '?'
    THEN NULL
    ELSE to_date(from_unixtime(unix_timestamp(agent_effective_hire_date,'MM/dd/yyyy')))
    END agent_effective_hire_date
  ,agent_mso
  ,eduid
  ,case when row_number() over (partition by call_inbound_key order by CASE WHEN customer_call_count_indicator = 1
           AND call_handled_flag = 1
           AND LOWER(call_owner) in ('customer operations')
           AND LOWER(segment_status_disposition) in ('answered by an agent')
           THEN 0 ELSE 1 END,segment_end_datetime_utc desc) = 1 then 1 else 0 end
  ,record_update_timestamp
  ,to_date(call_end_datetime_utc)
FROM ${env:TMP_db}.steve_call_data_2017_tmp cd
LEFT JOIN ${env:ENVIRONMENT}.steve_CHR_ACCOUNT_IDS IDS
  ON ${env:ENVIRONMENT}.aes_decrypt256(CD.account_number) = ids.account_number
  AND to_date(cd.call_end_datetime_utc) = ids.partition_date
  LEFT JOIN ${env:ENVIRONMENT}.steve_account_current AC
    ON ${env:ENVIRONMENT}.AES_DECRYPT256(cd.account_number) = ac.account_number
;



SELECT "**End preload table load";

SELECT "*End ETL script";
