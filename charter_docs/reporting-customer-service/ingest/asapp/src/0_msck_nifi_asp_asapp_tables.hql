USE nifi;

MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_customer_feedback;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_export_row_counts;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_flow_detail;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_intents;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_issue_queues;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_activity;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_attributes;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_convos;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_hierarchy;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_utilized;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_repmetrics;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_reps;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_transfers;

MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_augmentation_analytics;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_intents;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_intents_ended;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metadata;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metadata_ended;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metrics;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metrics_ended;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_csid_containment;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_customer_params;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_flow_completions;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_augmentation;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_sdk_events;
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_utterances;
