#!/bin/bash

. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
drop_hive_view_if_exists "${DASP_db}.cs_call_care_data_research_v"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.cs_call_care_data_research_v as
select
cca.*,
ig.issue_group,
cg.cause_group,
rg.resolution_group
from ${DASP_db}.cs_call_care_data_agg cca
left join ${DASP_db}.cs_dispositions_issue_groups ig on cca.issue_description = ig.issue_description
  and ig.version = 'current'
left join ${DASP_db}.cs_dispositions_cause_groups cg on cca.cause_description = cg.cause_description
  and cg.version = 'current'
left join ${DASP_db}.cs_dispositions_resolution_groups rg on cca.resolution_description = rg.resolution_description
  and rg.version = 'current'
;"