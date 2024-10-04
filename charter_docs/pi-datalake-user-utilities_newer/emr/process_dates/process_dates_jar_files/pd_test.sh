#!/bin/bash

#echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
#hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

#echo "### Dates process - Download shell script"
#aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar

#echo "### Dates process - Extract shell script and timezone lookup from jar file"
#jar -xfM process_dates.jar

echo "### Running process_dates with new variables for testing"
export RUN_DATE="2021-01-15"
export TIME_ZONE="UTC"
export CADENCE="daily"

echo "### running process_dates for daily ###"
source process_dates.sh

export "###Start of steps running..."
export DAILY_EXPECTED_RUN_DATE="2021-01-15"
export DAILY_RUN_DATE=$RUN_DATE
export DAILY_EXPECTED_TIME_ZONE="UTC";
export DAILY_TIME_ZONE="$TZ";
export DAILY_EXPECTED_START_DATE="2021-01-14";
export DAILY_START_DATE=$START_DATE;
export DAILY_EXPECTED_END_DATE="2021-01-15";
export DAILY_END_DATE=$END_DATE;
export DAILY_EXPECTED_START_DATE_TZ="2021-01-14_00";
export DAILY_START_DATE_TZ=$START_DATE_TZ;
export DAILY_EXPECTED_END_DATE_TZ="2021-01-15_00";
export DAILY_END_DATE_TZ=$END_DATE_TZ;
export DAILY_EXPECTED_CADENCE="daily";
export DAILY_CADENCE=$CADENCE;
export DAILY_EXPECTED_label_date_denver="2021-01-14";
export DAILY_label_date_denver=$LABEL_DATE_DENVER;
export DAILY_EXPECTED_LAG_DAYS=1;
export DAILY_LAG_DAYS=$LAGDAYS;
export DAILY_EXPECTED_LEAD_DAYS=1;
export DAILY_LEAD_DAYS=$LEADDAYS;

echo "### Un-setting variables for subsequent run ###"
unset pwd RUN_DATE TIME_ZONE CADENCE LAG_DAYS LEAD_DAYS

echo "### Re-running process_dates with new variables for testing"
export RUN_DATE="2021-01-15"
export TIME_ZONE="America/Denver"
export CADENCE="daily"
export LAG_DAYS=5
export LEAD_DAYS=2

echo "### Re-running process_dates for daily with lag-lead ###"
source process_dates.sh

echo "###Start of steps running..."
export DAILY_LAGLEAD_EXPECTED_RUN_DATE="2021-01-15"
export DAILY_LAGLEAD_RUN_DATE=$RUN_DATE
export DAILY_LAGLEAD_EXPECTED_TIME_ZONE="America/Denver";
export DAILY_LAGLEAD_TIME_ZONE=$TZ;
export DAILY_LAGLEAD_EXPECTED_START_DATE="2021-01-10";
export DAILY_LAGLEAD_START_DATE=$START_DATE;
export DAILY_LAGLEAD_EXPECTED_END_DATE="2021-01-12";
export DAILY_LAGLEAD_END_DATE=$END_DATE;
export DAILY_LAGLEAD_EXPECTED_START_DATE_TZ="2021-01-10_07";
export DAILY_LAGLEAD_START_DATE_TZ=$START_DATE_TZ;
export DAILY_LAGLEAD_EXPECTED_END_DATE_TZ="2021-01-12_07";
export DAILY_LAGLEAD_END_DATE_TZ=$END_DATE_TZ;
export DAILY_LAGLEAD_EXPECTED_CADENCE="daily";
export DAILY_LAGLEAD_CADENCE=$CADENCE;
export DAILY_LAGLEAD_EXPECTED_label_date_denver="2021-01-10";
export DAILY_LAGLEAD_label_date_denver=$LABEL_DATE_DENVER;
export DAILY_LAGLEAD_EXPECTED_LAG_DAYS=5;
export DAILY_LAGLEAD_LAG_DAYS=$LAGDAYS;
export DAILY_LAGLEAD_EXPECTED_LEAD_DAYS=2;
export DAILY_LAGLEAD_LEAD_DAYS=$LEADDAYS;

echo "### Un-setting variables for subsequent run ###"
unset pwd RUN_DATE TIME_ZONE CADENCE LAG_DAYS LEAD_DAYS

echo "### Re-running process_dates with new variables for testing"
export RUN_DATE="2021-06-02"
export TIME_ZONE="America/New_York"
export CADENCE="weekly"
export LAG_DAYS=1
export LEAD_DAYS=2

echo "### Re-running process_dates for weekly ###"
source process_dates.sh

echo "###Start of steps running..."
export WEEKLY_EXPECTED_RUN_DATE="2021-06-02"
export WEEKLY_RUN_DATE=$RUN_DATE
export WEEKLY_EXPECTED_TIME_ZONE="America/New_York";
export WEEKLY_TIME_ZONE=$TZ;
export WEEKLY_EXPECTED_START_DATE="2021-05-26";
export WEEKLY_START_DATE=$START_DATE;
export WEEKLY_EXPECTED_END_DATE="2021-06-02";
export WEEKLY_END_DATE=$END_DATE;
export WEEKLY_EXPECTED_START_DATE_TZ="2021-05-26_04";
export WEEKLY_START_DATE_TZ=$START_DATE_TZ;
export WEEKLY_EXPECTED_END_DATE_TZ="2021-06-02_04";
export WEEKLY_END_DATE_TZ=$END_DATE_TZ;
export WEEKLY_EXPECTED_label_date_denver="2021-06-01";
export WEEKLY_label_date_denver=$LABEL_DATE_DENVER;
export WEEKLY_EXPECTED_LAG_DAYS=1;
export WEEKLY_LAG_DAYS=$LAGDAYS;
export WEEKLY_EXPECTED_LEAD_DAYS=2;
export WEEKLY_LEAD_DAYS=$LEADDAYS;

echo "### Un-setting variables for subsequent run ###"
unset pwd RUN_DATE TIME_ZONE CADENCE LAG_DAYS LEAD_DAYS

echo "### Re-running process_dates with new variables for testing"
export RUN_DATE="2021-06-09"
export TIME_ZONE=""
export CADENCE="fiscal_monthly"
export LAG_DAYS=""
export LEAD_DAYS=""

echo "### Re-running process_datesfor fiscal monthly ###"
source process_dates.sh

echo "###Start of steps running..."
export FISCAL_MONTHLY_EXPECTED_RUN_DATE="2021-06-09"
export FISCAL_MONTHLY_RUN_DATE=$RUN_DATE
export FISCAL_MONTHLY_EXPECTED_TIME_ZONE="America/Denver";
export FISCAL_MONTHLY_TIME_ZONE=$TZ;
export FISCAL_MONTHLY_EXPECTED_START_DATE="2021-05-29";
export FISCAL_MONTHLY_START_DATE=$START_DATE;
export FISCAL_MONTHLY_EXPECTED_END_DATE="2021-06-29";
export FISCAL_MONTHLY_END_DATE=$END_DATE;
export FISCAL_MONTHLY_EXPECTED_START_DATE_TZ="2021-05-29_06";
export FISCAL_MONTHLY_START_DATE_TZ=$START_DATE_TZ;
export FISCAL_MONTHLY_EXPECTED_END_DATE_TZ="2021-06-29_06";
export FISCAL_MONTHLY_END_DATE_TZ=$END_DATE_TZ;
export FISCAL_MONTHLY_EXPECTED_label_date_denver="2021-06-28";
export FISCAL_MONTHLY_label_date_denver=$LABEL_DATE_DENVER;
export FISCAL_MONTHLY_EXPECTED_LAG_DAYS=1;
export FISCAL_MONTHLY_LAG_DAYS=$LAGDAYS;
export FISCAL_MONTHLY_EXPECTED_LEAD_DAYS=1;
export FISCAL_MONTHLY_LEAD_DAYS=$LEADDAYS;

echo "### Un-setting variables for subsequent run ###"
unset pwd RUN_DATE TIME_ZONE CADENCE LAG_DAYS LEAD_DAYS

echo "### Re-running process_dates with new variables for testing"
export RUN_DATE="2021-06-09"
export TIME_ZONE="America/Denver"
export CADENCE=monthly

echo "### Re-running process_dates for monthly ###"
source process_dates.sh

echo "###Start of steps running..."
export MONTHLY_EXPECTED_RUN_DATE="2021-06-09"
export MONTHLY_RUN_DATE=$RUN_DATE
export MONTHLY_EXPECTED_TIME_ZONE="America/Denver";
export MONTHLY_TIME_ZONE=$TZ;
export MONTHLY_EXPECTED_START_DATE="2021-06-01";
export MONTHLY_START_DATE=$START_DATE;
export MONTHLY_EXPECTED_END_DATE="2021-07-01";
export MONTHLY_END_DATE=$END_DATE;
export MONTHLY_EXPECTED_START_DATE_TZ="2021-06-01_06";
export MONTHLY_START_DATE_TZ=$START_DATE_TZ;
export MONTHLY_EXPECTED_END_DATE_TZ="2021-07-01_06";
export MONTHLY_END_DATE_TZ=$END_DATE_TZ;
export MONTHLY_EXPECTED_label_date_denver="2021-06-30";
export MONTHLY_label_date_denver=$LABEL_DATE_DENVER;
export MONTHLY_EXPECTED_LAG_DAYS=1;
export MONTHLY_LAG_DAYS=$LAGDAYS;
export MONTHLY_EXPECTED_LEAD_DAYS=1;
export MONTHLY_LEAD_DAYS=$LEADDAYS;

echo "### running test results output ###"
source process_dates_test_output.sh
