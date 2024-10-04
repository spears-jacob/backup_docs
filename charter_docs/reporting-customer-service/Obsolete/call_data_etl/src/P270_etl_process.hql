--Debugging echo
SELECT "*Begin ETL script";

select '${hiveconf:processdate}';


USE ${env:ENVIRONMENT};

SELECT "**Begin loading temporary table ${env:TMP_db}.cs_call_data_P270_tmp";

--Set hive parameters for efficiently processing the dataset
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;
--set hive.auto.convert.join=false;

--Get min and max of segment start/end times for call level data
DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_p270_callstartendtimes;
CREATE TABLE ${env:TMP_db}.cs_call_data_P270_callstartendtimes AS
SELECT CallInbKey
,min(CallStartDatetime_NewYork) CallStartTime_new
,max(CallEndDatetime_NewYork) CallEndTime_new
FROM ${env:TMP_db}.cs_call_data_P270_managed
GROUP BY CallInbKey
;

--Cleanse accounts by assigning the account number on the last handled segment and other criteria to all segments in a CALL_INBOUND_KEY
--This makes call_inbound_key to account_number a 1-to-1 relationship instead of 1-to-many as it is coming from the feed
DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_p270_accounts;
CREATE TABLE ${env:TMP_db}.cs_call_data_p270_accounts AS
SELECT *
FROM
(
  SELECT DISTINCT CallInbKey, AcctNum
  FROM
  (
    SELECT CallInbKey, AcctNum
    ,ROW_NUMBER() OVER (PARTITION BY CallInbKey ORDER BY lasthndlsegfl, CASE WHEN lower(dev.aes_decrypt256(acctnum)) = 'unknown' THEN 9999 ELSE 1 END
    ,CallEndDatetime_NewYork DESC,SegNum) RANKING
    --MAX(CallEndDatetime_NewYork) OVER (PARTITION BY CallInbKey) latest_segment
    FROM DEV_TMP.cs_call_data_p270_managed
    GROUP BY CallInbKey, AcctNum, CallEndDatetime_NewYork, SegNum, lasthndlsegfl
  ) dt
  WHERE RANKING = 1
) accounts
;

--Loading the call data tmp table with transformations and field name changes
DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_P270_tmp;
--Initial query pulling from call data files in CTE
CREATE TABLE ${env:TMP_db}.cs_call_data_P270_tmp AS
SELECT
  a.CallId call_id
  ,to_utc_timestamp(CallStartTime_new,'America/New_York') call_start_datetime_utc
  ,to_utc_timestamp(CallEndTime_new,'America/New_York') call_end_datetime_utc

  ,cast('' as bigint) previous_call_time

  ,SegNum segment_number
  ,AnsDispoDesc segment_status_disposition

  ,to_utc_timestamp(a.CallStartDatetime_NewYork,'America/New_York') segment_start_datetime_utc


  --Logic to back into the segment_end_datetime. Data set joined below and then logic performed here to use
  --    the next segment start time -1 second if it is not the last segment in the call.
  ,CASE WHEN a.CallStartDatetime_NewYork = coalesce(endtimes.segment_end_datetime_new,a.CallEndDatetime_NewYork)
  THEN to_utc_timestamp(a.CallEndDatetime_NewYork,'America/New_York')
  ELSE
  cast(from_unixtime(unix_timestamp(to_utc_timestamp(coalesce(endtimes.segment_end_datetime_new,a.callenddatetime_newyork),'America/New_York'))-1) as timestamp)
  END
    AS segment_end_datetime_utc

  ,Product

  --Use new account number from cleansing or the original account number from the feed if no cleansed account number is found
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
FROM ${env:TMP_db}.cs_call_data_P270_managed a
LEFT JOIN ${env:TMP_db}.cs_call_data_p270_callstartendtimes b
  ON a.CallInbKey = b.CallInbKey
--Join cleansed accounts
LEFT JOIN ${env:TMP_db}.cs_call_data_p270_accounts acct
  ON a.CallInbKey = acct.CallInbKey
--Join to get the next segment end time to use as calculation to back into the real segment end time
LEFT JOIN
(
  SELECT callinbkey, callstartdatetime_newyork
    ,lead(callstartdatetime_newyork) OVER (PARTITION BY callinbkey ORDER BY callstartdatetime_newyork) AS segment_end_datetime_new
    FROM
    (
      SELECT DISTINCT callinbkey, callstartdatetime_newyork
      FROM dev_tmp.cs_call_data_p270_managed
    ) endtimes
) endtimes
  ON a.callinbkey = endtimes.CallInbKey
  AND a.callstartdatetime_newyork = endtimes.CallStartDatetime_NewYork
WHERE to_date(to_utc_timestamp(CallEndTime_new,'America/New_York')) >= to_date('2018-08-01')
--to_date('${hiveconf:processdate}')
--"${hiveconf:processdate}"
;

SELECT "**End loading temporary table ${env:TMP_db}.cs_call_data_P270_tmp";


SELECT "*End ETL script";
