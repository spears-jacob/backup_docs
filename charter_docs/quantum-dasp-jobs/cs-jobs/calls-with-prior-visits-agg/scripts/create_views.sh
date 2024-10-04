#!/bin/bash

. ./scripts/util-${SCRIPT_VERSION}.sh

drop_hive_view_if_exists "${DASP_db}.cs_calls_with_prior_visits_research_v"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.cs_calls_with_prior_visits_research_v as
select
cwpva.*
,ig.issue_group
,cg.cause_group
,rg.resolution_group
from ${DASP_db}.cs_calls_with_prior_visits_agg cwpva
left join ${DASP_db}.cs_dispositions_issue_groups ig on cwpva.issue_description = ig.issue_description
  and ig.version = 'current'
left join ${DASP_db}.cs_dispositions_cause_groups cg on cwpva.cause_description = cg.cause_description
  and cg.version = 'current'
left join ${DASP_db}.cs_dispositions_resolution_groups rg on cwpva.resolution_description = rg.resolution_description
  and rg.version = 'current'
;"