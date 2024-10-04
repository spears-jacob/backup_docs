#!/bin/bash

export tmp_db=$1

count_augmentation_analytics=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_augmentation_analytics;")
count_convos_intents_ended=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_intents_ended;")
count_convos_intents=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_intents;")
count_convos_metadata_ended=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_metadata_ended;")
count_convos_metadata=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_metadata;")
count_convos_metrics_ended=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_metrics_ended;")
count_convos_metrics=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_convos_metrics;")
count_csid_containment=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_csid_containment;")
count_customer_params=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_customer_params;")
count_flow_completions=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_flow_completions;")
count_rep_augmentation=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_rep_augmentation;")
count_sdk_events=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_sdk_events;")
count_utterances=$(hive -S -e "set hive.cli.print.header=false; SELECT COUNT(1) FROM ${tmp_db}.asp_asapp_utterances;")

echo "count_augmentation_analytics is $count_augmentation_analytics"
if [ "$count_augmentation_analytics" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/augmentation_analytics.conf
fi

echo "count_convos_intents_ended is $count_convos_intents_ended"
if [ "$count_convos_intents_ended" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_intents_ended.conf
fi

echo "count_convos_intents is $count_convos_intents"
if [ "$count_convos_intents" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_intents.conf
fi

echo "count_convos_metadata_ended is $count_convos_metadata_ended"
if [ "$count_convos_metadata_ended" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_metadata_ended.conf
fi

echo "count_convos_metadata is $count_convos_metadata"
if [ "$count_convos_metadata" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_metadata.conf
fi

echo "count_convos_metrics_ended is $count_convos_metrics_ended"
if [ "$count_convos_metrics_ended" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_metrics_ended.conf
fi

echo "count_convos_metrics is $count_convos_metrics"
if [ "$count_convos_metrics" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/convos_metrics.conf
fi

echo "count_csid_containment is $count_csid_containment"
if [ "$count_csid_containment" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/csid_containment.conf
fi

echo "count_customer_params is $count_customer_params"
if [ "$count_customer_params" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/customer_params.conf
fi

echo "count_flow_completions is $count_flow_completions"
if [ "$count_flow_completions" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/flow_completions.conf
fi

echo "count_rep_augmentation is $count_rep_augmentation"
if [ "$count_rep_augmentation" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/rep_augmentation.conf
fi

echo "count_sdk_events is $count_sdk_events"
if [ "$count_sdk_events" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/sdk_events.conf
fi

echo "count_utterances is $count_utterances"
if [ "$count_utterances" -gt 0 ]; then
	/data/opt/soteria/soteria.sh ../conf/utterances.conf
fi
