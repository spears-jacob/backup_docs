USE ${env:SEC_db};

SET hive.merge.size.per.task=4096000000;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.tezfiles=true;
SET hive.optimize.sort.dynamic.partition.threshold=-1;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT "Inserting PII Data For asp_sentmsgs_epid";
INSERT OVERWRITE TABLE asp_sentmsgs_epid  PARTITION (partition_date_utc)
SELECT  accountnumber,
        campaigncategory,
        campaignconsumerid,
        campaignversion,
        companycd,
        contactstrategy,
        customertype,
        eventdetail,
        eventpostid,
        ordernumber,
        params,
        serviceeventtype,
        soloaccountid,
        spcdivisionid,
        tokenid,
        transactionid,
        triggersource,
        substr(eventdetail.eventdatetime,1,10) as partition_date_utc
FROM asp_sentmsgs_epid_raw
WHERE partition_date >= '${env:ROLLING_03_START_DATE}'
  AND partition_date <  '${env:END_DATE}'
;
