#!/bin/bash

# TODO: make select form prod environment

. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
drop_hive_view_if_exists "${DASP_db}.asp_vat_support_content_agg"

execute_script_in_athena "
CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_support_content_agg AS

SELECT *,

  CAST(denver_date AS date) AS denver_date_dt

FROM
  ${DASP_db}.asp_support_content_agg
WHERE

(
  CAST(denver_date AS date) >= (date_add('DAY', -90, current_date))
  AND
  CAST(denver_date AS date) <= (date_add('DAY', -2, current_date))
)
;
"
#execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.cs_v_call_care_data as SELECT * FROM ${DASP_db}.cs_call_care_data;"
