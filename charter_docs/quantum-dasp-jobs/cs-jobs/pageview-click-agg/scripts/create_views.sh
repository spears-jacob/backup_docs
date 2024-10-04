#!/bin/bash

# TODO: make select form prod environment

. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
drop_hive_view_if_exists "${DASP_db}.cs_quantum_pageview_selectaction_aggregates"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.cs_quantum_pageview_selectaction_aggregates AS

        SELECT  application_name,
                current_page,
                element_name,
                count_of_buttonclicks,
                count_of_unique_visitors,
                count_of_visitors,
                count_of_distinct_visits,
                date(partition_date_utc) as day,
                'daily' as grain
        FROM ${DASP_db}.cs_daily_pageview_selectaction_aggregate

        UNION ALL

        SELECT  application_name,
                current_page,
                element_name,
                count_of_buttonclicks,
                count_of_unique_visitors,
                count_of_visitors,
                count_of_distinct_visits,
                date(week_starting) as day,
                'weekly' as grain
        FROM ${DASP_db}.cs_weekly_pageview_selectaction_aggregate

        UNION ALL

        SELECT  application_name,
                current_page,
                element_name,
                count_of_buttonclicks,
                count_of_unique_visitors,
                count_of_visitors,
                count_of_distinct_visits,
                date(CONCAT(fiscal_month,'-01')) as day,
                'fiscal monthly' as grain
        FROM ${DASP_db}.cs_monthly_pageview_selectaction_aggregate

        UNION ALL

        SELECT  application_name,
                current_page,
                element_name,
                count_of_buttonclicks,
                count_of_unique_visitors,
                count_of_visitors,
                count_of_distinct_visits,
                date(CONCAT(calendar_month,'-01')) as day,
                'calendar monthly' as grain
        FROM ${DASP_db}.cs_calendar_monthly_pageview_selectaction_aggregate
" || { exit 1;}
