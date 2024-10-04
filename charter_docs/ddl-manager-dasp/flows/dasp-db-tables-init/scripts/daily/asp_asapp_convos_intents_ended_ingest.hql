CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_convos_intents_ended_ingest (
    first_utterance_text STRING,
    customer_id STRING,
    company_name STRING,
    is_first_intent_correct BOOLEAN,
    first_agent_id INT,
    first_rep_id INT,
    final_intent_code STRING,
    ftd_visit BOOLEAN,
    faq_id STRING,
    issue_id STRING,
    first_intent_code STRING,
    first_utterance_ts STRING,
    intent_path STRING,
    first_intent_code_alt STRING,
    final_action_destination STRING,
    conversation_id STRING,
    disambig_count INT,
    partition_date STRING,
    partition_hour STRING,
    first_utterance_hour string
)
PARTITIONED BY (first_utterance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
