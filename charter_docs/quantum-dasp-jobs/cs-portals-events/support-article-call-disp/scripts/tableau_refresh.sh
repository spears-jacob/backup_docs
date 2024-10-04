#!/bin/bash
echo "### Download jar file from s3 udf-jars for Tableau Refresh ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
shopt -s expand_aliases
source ./scripts/aws_common_utilities.sh

export ENVIRONMENT=$1
STG_REFRESH="no"

echo ".................... Running Tableau Refresh ...................."
REPORT_NAME="SupportArticleandCustomerCallJourney-XG27616"
tableau_refresh $ENVIRONMENT $REPORT_NAME $STG_REFRESH
