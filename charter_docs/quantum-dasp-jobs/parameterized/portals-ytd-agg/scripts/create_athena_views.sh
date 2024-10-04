#!/bin/bash

. ./scripts/util-${SCRIPT_VERSION}.sh

drop_hive_view_if_exists "${DASP_db}.asp_vat_portals_ytd_agg"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_portals_ytd_agg AS
SELECT *,
       CAST(label_date_denver as date)  AS label_date_denver_dt
FROM ${DASP_db}.asp_portals_ytd_agg
WHERE CAST(label_date_denver as date)   >= DATE_ADD('day', -370, CURRENT_DATE)
;
"
