#!/bin/bash
# This script renames two tables for use in portals self service reprocessing

export FLOW_BACKFILL="backfill";
export rnts=_$(date +"%s")
export map=venona_metric_agg_portals
export sap=venona_set_agg_portals

if [ $ENVIRONMENT != "prod" ]; then
   echo $0: Usage: only from production environment.  Try again using prod.
   exit 1
fi

echo "

### The two self-service related tables are being renamed so they can be replaced
    with the newly reprocessed set of tables from the $reprocess_env environment.

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

echo "
################################################################################
### The two self-service related tables are now being moved from the
    $reprocess_env environment.

    So, the metric and set agg tables in the $reprocess_env environment are
    moving to the $ENVIRONMENT environment on $azurl.

    $reprocess_env.$map --> $ENVIRONMENT.$map
    $reprocess_env.$sap --> $ENVIRONMENT.$sap

    The rename code looks like the following.

    ALTER TABLE $reprocess_env.$map RENAME TO $ENVIRONMENT.$map;
    ALTER TABLE $reprocess_env.$sap RENAME TO $ENVIRONMENT.$sap;

"

 hive -e "USE $ENVIRONMENT; ALTER TABLE $reprocess_env.$map RENAME TO $ENVIRONMENT.$map; ALTER TABLE $reprocess_env.$sap RENAME TO $ENVIRONMENT.$sap; " || { echo 'hql statement had an issue.'; exit 2112; }


echo "

### Success. The metric and set agg tables in the $reprocess_env environment have
    been moved to the $ENVIRONMENT on $azurl.

    $reprocess_env.$map --> $ENVIRONMENT.$map
    $reprocess_env.$sap --> $ENVIRONMENT.$sap




################################################################################
    Now, please ensure that the same version of code that prepared the latest metric
    and set aggs is promoted to production -- both the metric and set aggs.

"
