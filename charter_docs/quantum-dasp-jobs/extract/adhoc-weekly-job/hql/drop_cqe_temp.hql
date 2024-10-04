USE ${env:DASP_db};

SELECT "\n\n Dropping CQE TEMPORARY Table \n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER} PURGE;
