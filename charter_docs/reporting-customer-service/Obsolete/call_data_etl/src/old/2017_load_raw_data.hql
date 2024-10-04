USE ${env:TMP_db};

SELECT "**Begin raw data load into table";
LOAD DATA LOCAL INPATH "/data/tmp/CALL_LCHR_*.txt" OVERWRITE INTO TABLE ${env:TMP_db}.steve_call_data_2017_raw;
SELECT "**Complete raw data load into table";

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;

INSERT OVERWRITE TABLE ${env:TMP_db}.steve_call_data_2017_managed
SELECT
  to_date(from_unixtime(unix_timestamp(CallStartDt_NewYork,'MM/dd/yyyy')))
  ,to_date(from_unixtime(unix_timestamp(CallStopDt_NewYork,'MM/dd/yyyy')))
  ,concat(to_date(from_unixtime(unix_timestamp(CallStartDt_NewYork,'MM/dd/yyyy'))),' ',CallStartTime_NewYork) CallStartDatetime_NewYork
  ,concat(to_date(from_unixtime(unix_timestamp(CallStopDt_NewYork,'MM/dd/yyyy'))),' ',CallStopTime_NewYork) CallEndDatetime_NewYork
  ,CallStartTime_NewYork
  ,CallStopTime_NewYork
  ,Product
  ,${env:ENVIRONMENT}.aes_encrypt256(AcctId)
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
FROM ${env:TMP_db}.steve_call_data_2017_raw
;


ANALYZE TABLE ${env:TMP_db}.steve_call_data_2017_managed COMPUTE STATISTICS;
ANALYZE TABLE ${env:TMP_db}.steve_call_data_2017_managed COMPUTE STATISTICS FOR COLUMNS;
