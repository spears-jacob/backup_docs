USE ${env:ENVIRONMENT};

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TABLE_NAME};
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TABLE_NAME} like ${hiveconf:TABLE_NAME};

INSERT INTO TABLE ${env:TMP_db}.${hiveconf:TABLE_NAME}
SELECT * FROM ${hiveconf:TABLE_NAME} WHERE ${hiveconf:COLUMN_DATE1} != '${hiveconf:RUN_YEAR}' AND ${hiveconf:COLUMN_DATE2} != '${hiveconf:RUN_MONTH}';

INSERT overwrite TABLE ${hiveconf:TABLE_NAME}
SELECT * FROM ${env:TMP_db}.${hiveconf:TABLE_NAME};

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TABLE_NAME};
