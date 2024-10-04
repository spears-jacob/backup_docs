USE ${env:ENVIRONMENT};

ALTER TABLE ${hiveconf:TABLE_NAME} DROP IF EXISTS PARTITION (partition_date_denver='${env:YESTERDAY_DEN}');
