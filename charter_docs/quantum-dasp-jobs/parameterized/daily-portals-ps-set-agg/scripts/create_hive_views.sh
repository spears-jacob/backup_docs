#!/bin/bash

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_privacysite_set_agg_v;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_privacysite_set_agg_v AS
select *
from ${DASP_db}.asp_privacysite_set_agg;"
