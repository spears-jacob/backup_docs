#!/bin/bash

. ./scripts/util-${SCRIPT_VERSION}.sh

drop_hive_view_if_exists "${DASP_db}.asp_m2dot0_metric_visit_lookup_5day"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_m2dot0_metric_visit_lookup_5day
    AS

    SELECT
    visit__application_details__application_name,
    visit__application_details__application_type,
    visit__application_details__app_version,
    agg_custom_visit__account__details__service_subscriptions,
    agg_custom_customer_group,
    agg_visit__account__configuration_factors,
    visit_id,
    metric_name,
    metric_value,
    partition_date_utc
    FROM ${DASP_db}.asp_m2dot0_metric_visit_lookup
    WHERE partition_date_utc >= '$VIEW_LAG_DATE'
" || { exit 1;}
