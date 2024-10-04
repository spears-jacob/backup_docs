#!/bin/bash

echo "### DROP asp_quantum_metric_agg_v VIEW..."
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_quantum_metric_agg_v;"

echo "### Create asp_quantum_metric_agg_v View"
hive -v -e "CREATE VIEW ${DASP_db}.asp_quantum_metric_agg_v AS
select *
from ${DASP_db}.quantum_metric_agg_portals;"