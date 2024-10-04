#!/bin/bash
# TODO: change enviroment to prod
hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_quantum_metric_agg_portals AS SELECT * FROM ${DASP_db}.quantum_metric_agg_portals;"

hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_onboarding_metrics AS SELECT * FROM ${DASP_db}.asp_msa_onboarding_metrics;"

hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_onboarding_time AS SELECT * FROM ${DASP_db}.asp_msa_onboarding_time;"


# temporal solution for asp_v_quantum_events_portals_msa view
hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_quantum_events_portals_msa AS SELECT * FROM ${ENVIRONMENT}.core_quantum_events_sspp
WHERE (visit__application_details__application_name = 'MySpectrum');"