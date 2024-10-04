CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_core_quantum_events
(
	received__timestamp                                        BIGINT,
	visit__visit_id                                            STRING,
	visit__account__details__mso                               STRING,
	visit__application_details__application_type               STRING,
	visit__application_details__app_version                    STRING,
	application__error__error_type                             STRING,
	application__error__error_code                             STRING,
	application__api__response_text                            STRING,
	application__api__response_time_ms                         INT,
	application__api__path                                     STRING,
	application__api__http_verb                                STRING,
	message__name                                              STRING,
	message__sequence_number                                   INT,
	message__timestamp                                         BIGINT,
	message__triggered_by                                      STRING,
	message__feature__feature_name                             STRING,
	state__view__current_page__page_name                       STRING,
	state__view__current_page__elements__standardized_name     STRING,
	state__view__modal__name                                   STRING,
	operation__success                                         BOOLEAN,
	message__context                                           STRING,
	message__feature__transaction_id                           STRING,
	message__feature__feature_step_name                        STRING,
	message__event_case_id                                     STRING,
	state__view__current_page__elements__element_string_value  STRING,
	state__view__current_page__additional_information          STRING,
	state__view__current_page__page_title                      STRING,
	operation__user_entry__numeric                             INT,
	operation__user_entry__survey_id                           STRING,
	visit__device__enc_uuid                                    STRING,
	visit__account__enc_account_number                         STRING,
	visit__account__enc_account_billing_division               STRING,
	visit__account__enc_account_billing_division_id            STRING,
	message__time_since_last_event_ms                          INT
)
	PARTITIONED BY (partition_date_utc STRING, partition_date_hour_utc STRING, visit__application_details__application_name STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Event Level - With PII (1 Year)")
