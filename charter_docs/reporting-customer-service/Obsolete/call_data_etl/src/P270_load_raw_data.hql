USE ${env:TMP_db};

SELECT "**Begin raw data load into table";
LOAD DATA LOCAL INPATH "/data/tmp/CARE_P270_09222018_10192018.txt" OVERWRITE INTO TABLE ${env:TMP_db}.cs_call_data_P270_raw;
SELECT "**Complete raw data load into table";

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;

INSERT OVERWRITE TABLE ${env:TMP_db}.cs_call_data_p270_managed partition (call_end_date_newyork)
SELECT
  to_date(from_unixtime(unix_timestamp(CallStartDt_NewYork,'MM/dd/yyyy')))
  ,to_date(from_unixtime(unix_timestamp(CallStopDt_NewYork,'MM/dd/yyyy')))
  ,concat(to_date(from_unixtime(unix_timestamp(CallStartDt_NewYork,'MM/dd/yyyy'))),' ',CallStartTime_NewYork) CallStartDatetime_NewYork
  ,concat(to_date(from_unixtime(unix_timestamp(CallStopDt_NewYork,'MM/dd/yyyy'))),' ',CallStopTime_NewYork) CallEndDatetime_NewYork
  --,CallStartTime_NewYork
  --,CallStopTime_NewYork

  ,CallStartTime_NewYork
  ,CallStopTime_NewYork
  ,Product
  --,${env:ENVIRONMENT}.aes_encrypt256(AcctId)
  ,${env:ENVIRONMENT}.aes_encrypt256(AcctNum)
  ,${env:ENVIRONMENT}.aes_encrypt256(CustAccountNo)
  ,TruckRollFl
  ,${env:ENVIRONMENT}.aes_encrypt256(NotesTxt)
  ,CallResDesc
  ,CallCauseDesc
  ,CallIssDesc
  ,SegNum
  ,TRIM(CallId)
  ,CompanyCode
  ,ServiceCallTrackerID
  ,CreatedOn
  ,CreatedBy
  ,PhoneNumberFromTracker
  ,CallType
  ,SplitSumDesc
  ,LocNm
  ,CareCtrMgmtNm
  ,JobRoleNm
  ,EffectiveHireDt
  ,EDUId
  ,MSOAGENT
  ,AnsDispoDesc
  ,CustCallCntctInd
  ,CallOwner
  ,CallInbKey
  ,UniCallId
  ,CallsHndlFl
  ,LastHndlSegFl
  ,UPDATETIMESTAMP
  ,to_date(from_unixtime(unix_timestamp(CallStopDt_NewYork,'MM/dd/yyyy')))
FROM ${env:TMP_db}.cs_call_data_p270_raw
;


ANALYZE TABLE ${env:TMP_db}.cs_call_data_P270_managed PARTITION (call_end_date_newyork) COMPUTE STATISTICS;
ANALYZE TABLE ${env:TMP_db}.cs_call_data_P270_managed PARTITION (call_end_date_newyork) COMPUTE STATISTICS FOR COLUMNS;
