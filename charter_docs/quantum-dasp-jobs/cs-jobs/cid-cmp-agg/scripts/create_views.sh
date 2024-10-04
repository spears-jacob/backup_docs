#!/bin/bash

# TODO: make select form prod environment

. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_cs_cid_cmp_aggregate
    AS

    SELECT      application_name,
                message_name,
                count_of_events,
                count_of_unique_users,
                count_of_users,
                count_of_visits,
                campaign_id,
                page_title,
                page_id,
                page_name,
                grain,
                label_date_denver as day
    FROM ${DASP_db}.cs_cid_cmp_aggregate
" || { exit 1;}
