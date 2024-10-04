USE ${env:ENVIRONMENT};

ALTER TABLE ${hiveconf:TABLE_NAME} DROP IF EXISTS PARTITION (partition_year_month='${env:YEAR_MONTH}');
