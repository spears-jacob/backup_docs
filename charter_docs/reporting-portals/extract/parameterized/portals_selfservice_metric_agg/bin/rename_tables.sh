#!/bin/bash
# This script renames two tables for use in portals self service reprocessing

export FLOW_BACKFILL="backfill";
export rnts=_$(date +"%s")
export map=venona_metric_agg_portals
export sap=venona_set_agg_portals


echo "

### The two self-service related tables are being renamed for reprocessing.

    They are being retained in case restoration is needed in the near term.

    The suffix on the two table names is the current timestamp in epoch time,
    the number of seconds since 1970.

    $rnts

    So, the metric and set agg tables in the $ENVIRONMENT environment
    on $azurl are changing.

    $map --> $map$rnts
    $sap --> $sap$rnts

    The rename code looks like the following.

    USE $ENVIRONMENT;
    ALTER TABLE $map RENAME TO $map$rnts;
    ALTER TABLE $sap RENAME TO $sap$rnts;

"

hive -e "USE $ENVIRONMENT; ALTER TABLE $map RENAME TO $map$rnts; ALTER TABLE $sap RENAME TO $sap$rnts;" || { echo 'hql statement had an issue.'; exit 2112; }


echo "

### Success. The metric and set agg tables in the $ENVIRONMENT
    environment on $azurl have been altered.

    $map --> $map$rnts
    $sap --> $sap$rnts


"
