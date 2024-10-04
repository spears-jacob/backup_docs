#!/bin/bash

#SEC_db=prod_sec_repo_sspp
USE ${SEC_db};

CREATE EXTERNAL TABLE asp_sentmsgs_hp_raw (
  header struct<deliveryplatform:string>,
  payload struct <accountnumber:string,
                  businessgroupname:string,
                  campaignconsumerid:string,
                  comm:struct<attemptnum:string,
                              callduration:string,
                              channel:string,
                              commid:string,
                              commreceiveddatetime:string,
                              commstatus:string,
                              communicationtype:string,
                              contactvalue:string,
                              contentlanguage:string,
                              customerresponse:string,
                              deliverystatus:string,
                              emaillink:string,
                              enddatetime:string,
                              errordescription:string,
                              extractedatcommlevel:string,
                              isspanishfirst:boolean,
                              jasid:int,
                              jcrid:int,
                              jobid:int,
                              jpdid:int,
                              startdatetime:string,
                              urlclicks:array<struct<clicktime:string, url:string>>>,
                  creator:string,
                  customertype:string,
                  jobdescription:string,
                  jobparameters:array<struct<name:string, value:string>>,
                  sessionid:string,
                  spcdivisionid:string,
                  templateid:string,
                  transactionid:string,
                  userpreflanguage:string>)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
'separatorChar' = '\n'
)
stored as textfile
LOCATION 's3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/processed/hp/';

aws glue create-partition-index \
 --database-name ${SEC_db} \
 --table-name asp_sentmsgs_hp_raw \
 --partition-index Keys=partition_date,IndexName=pd_index

aws glue get-partition-indexes  --database-name ${SEC_db} --table-name asp_sentmsgs_hp_raw

CREATE EXTERNAL TABLE asp_sentmsgs_epid_raw
( accountnumber string,
  campaigncategory string,
  campaignconsumerid string,
  campaignversion string,
  companycd string,
  contactstrategy string,
  customertype string,
  eventdetail struct <attemptnum:string,
                      channel:string,
                      commid:string,
                      commmsgid:string,
                      contactid:string,
                      eventdatetime:string,
                      eventtype:string,
                      startdatetime:string,
                      templatedesc:string,
                      templateid:string,
                      templateversion:string,
                      timezone:string>,
  eventpostid string,
  ordernumber string,
  params array<struct<name:string, value:string>>,
  serviceeventtype string,
  soloaccountid string,
  spcdivisionid string,
  tokenid string,
  transactionid string,
  triggersource string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
'separatorChar' = '\n'
)
stored as textfile
LOCATION 's3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/processed/epid/'

aws glue create-partition-index \
 --database-name ${SEC_db} \
 --table-name asp_sentmsgs_epid_raw \
 --partition-index Keys=partition_date,IndexName=pd_index

aws glue get-partition-indexes  --database-name ${SEC_db} --table-name asp_sentmsgs_epid_raw

## ------------- binary / orc tables below
#SEC_db=prod_sec_repo_sspp
USE ${SEC_db};

CREATE EXTERNAL TABLE asp_sentmsgs_hp (
  header struct<deliveryplatform:string>,
  payload struct <accountnumber:string,
                  businessgroupname:string,
                  campaignconsumerid:string,
                  comm:struct<attemptnum:string,
                              callduration:string,
                              channel:string,
                              commid:string,
                              commreceiveddatetime:string,
                              commstatus:string,
                              communicationtype:string,
                              contactvalue:string,
                              contentlanguage:string,
                              customerresponse:string,
                              deliverystatus:string,
                              emaillink:string,
                              enddatetime:string,
                              errordescription:string,
                              extractedatcommlevel:string,
                              isspanishfirst:boolean,
                              jasid:int,
                              jcrid:int,
                              jobid:int,
                              jpdid:int,
                              startdatetime:string,
                              urlclicks:array<struct<clicktime:string, url:string>>>,
                  creator:string,
                  customertype:string,
                  jobdescription:string,
                  jobparameters:array<struct<name:string, value:string>>,
                  sessionid:string,
                  spcdivisionid:string,
                  templateid:string,
                  transactionid:string,
                  userpreflanguage:string>)
PARTITIONED BY (partition_date_utc string)
STORED AS PARQUET
LOCATION 's3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/collated/hp/'
TBLPROPERTIES ('parquet.compression'='SNAPPY')

aws glue create-partition-index \
 --database-name ${SEC_db} \
 --table-name asp_sentmsgs_hp \
 --partition-index Keys=partition_date_utc,IndexName=pd_index

aws glue get-partition-indexes  --database-name ${SEC_db} --table-name asp_sentmsgs_hp

CREATE EXTERNAL TABLE asp_sentmsgs_epid
( accountnumber string,
  campaigncategory string,
  campaignconsumerid string,
  campaignversion string,
  companycd string,
  contactstrategy string,
  customertype string,
  eventdetail struct <attemptnum:string,
                      channel:string,
                      commid:string,
                      commmsgid:string,
                      contactid:string,
                      eventdatetime:string,
                      eventtype:string,
                      startdatetime:string,
                      templatedesc:string,
                      templateid:string,
                      templateversion:string,
                      timezone:string>,
  eventpostid string,
  ordernumber string,
  params array<struct<name:string, value:string>>,
  serviceeventtype string,
  soloaccountid string,
  spcdivisionid string,
  tokenid string,
  transactionid string,
  triggersource string)
PARTITIONED BY (partition_date_utc string)
STORED AS PARQUET
LOCATION 's3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/collated/epid/'
TBLPROPERTIES ('parquet.compression'='SNAPPY')


aws glue create-partition-index \
 --database-name ${SEC_db} \
 --table-name asp_sentmsgs_epid \
 --partition-index Keys=partition_date_utc,IndexName=pd_index

aws glue get-partition-indexes  --database-name ${SEC_db} --table-name asp_sentmsgs_epid
