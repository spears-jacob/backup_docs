CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.SG_ZODIAC_EVENTS_UTC(
    SERVER_NODE_ID STRING,
    MAC_ID STRING,
    NODE_ID STRING,
    HUB_ID STRING,
    CHANNEL_MAP_ID STRING,
    SESSION_ID STRING,
    SOURCE_TMS_ID STRING,
    DESTINATION_TMS_ID STRING,
    EVENT_START_TIME STRING,
    REASON STRING,
    APP_ID STRING,
    SESSION_ACTIVITY_ID STRING,
    REMOTE_KEY STRING,
    APP_SCREEN_ID STRING,
    MENU_OPTION_SELECTED STRING,
    TMS_ID STRING,
    SEQUENCE_NUMBER STRING,
    EVENT_TYPE STRING,
    OPERATION_RESULT STRING,
    RECORDING_ID STRING,
    TMS_PROGRAM_ID STRING,
    TMS_CHANNEL_ID STRING,
    CHANNEL_NUMBER STRING,
    SCHEDULED_START_TIME STRING,
    SCHEDULED_END_TIME STRING,
    ACTUAL_START_TIME STRING,
    ACTUAL_END_TIME STRING,
    INTERNAL_HDD_FREE_SPACE STRING,
    EXTERNAL_HDD_FREE_SPACE STRING,
    CURRENT_POSITION_OF_WATCHED_RECORDING STRING,
    INTERNAL_HDD_CAPACITY STRING,
    EXTERNAL_HDD_CAPACITY STRING,
    SCREEN_MODE STRING,
    INPUT_FILENAME STRING,
    ADDITIONAL_INFO STRING,
    TUNER_ID STRING,
    TUNER_USE STRING,
    TSB_RECORDING STRING,
    DVR_RECORDING STRING,
    DLNA_OUTPUT STRING
)
    PARTITIONED BY (PARTITION_DATE_UTC STRING,MESSAGE_TYPE STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
