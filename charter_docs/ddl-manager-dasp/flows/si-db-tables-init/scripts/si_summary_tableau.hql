CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_summary_tableau
(
	visit_id                                 STRING,
	device_id                                STRING,
	acct_id                                  STRING,
	application_group                        STRING,
	application_type                         STRING,
	mso                                      STRING,
	user_package                             STRING,
	initiated_self_installs                  INT,
	initiated_self_installs_new              INT,
	initiated_self_installs_existing         INT,
	account_status_past_due                  INT,
	account_status_current                   INT,
	terms_conditions_accept                  INT,
	terms_conditions_disagree                INT,
	inactivity                               INT,
	abandonment_exiting_via_overflow_menu    INT,
	activation_no_touch                      INT,
	activation_user_call                     INT,
	activation_self_install_call             INT,
	activation_truck_roll_call               INT,
	activation_success_full                  INT,
	activation_success_partial               INT,
	activation_failure                       INT,
	activation_failure_full                  INT,
	activation_failure_partial               INT,
	activation_failure_calls                 INT,
	activation_failure_truck_rolls           INT,
	repair_call                              INT,
	repair_truck_roll_call                   INT,
	repair_no_touch                          INT,
	repair_rescue_no_touch                   INT,
	repair_rescue_call                       INT,
	repair_rescue_truck                      INT,
	job_class_category_code_t                INT,	
	job_class_category_code_all              INT,
	re_entry                                 INT,
	retry                                    INT,
	errors_connection_issue                  INT,
	errors_equipment_setup                   INT,
	customer_satisfaction                    INT,
	customer_satisfaction_submitted          INT,
	customer_satisfaction_submission_score   DOUBLE,
	customer_satisfaction_completed_survey   DOUBLE,
	home_page_view                           INT,
	equipment_setup_page_view                INT,
	terms_conditions_page_view               INT,
	service_terms_conditions_page_view       INT,
	equipment_checklist_page_view            INT,
	modem_checklist_page_view                INT,
	modem_activation_page_view               INT,
	router_checklist_page_view               INT,
	network_setup_page_view                  INT,
	router_activation_page_view              INT,
	rescue_no_touch                          INT,
	rescue_call                              INT,
	rescue_truck                             INT,
	date_level                               STRING,
	metric_type                              STRING,
	errors_authentication_setup              INT,
	errors_device_signal_setup               INT,
	errors_other_setup                       INT,
	tv_activation_page_view                  INT,
	voice_activation_page_view               INT,
	download_MSA                             INT,
	avg_transaction_duration                 DOUBLE,
	avg_api_response_time                    DOUBLE,
	abandonment_via_exit_setup               INT,
	coam_device                              INT,
	whitelisted_coam_device                  INT,
	blacklisted_coam_device                  INT,
	minimally_qualified_coam_device          INT,
	distinct_filter                          STRING,
	equipment_type_scp                       INT,
	equipment_type_monolithic                INT,
	visit_eligible                           INT,
	modem_activating                         INT,
	modem_connecting                         INT,
	router_activating                        INT,
	router_connecting                        INT,
	full_screen_takeover_page_view           INT,
	activation_complete                      INT,
	modem_errors_preActivation               INT,
	modem_errors_connection                  INT,
	modem_errors_activation                  INT,
	router_errors_preActivation              INT,
	router_errors_connection                 INT,
	router_errors_activation                 INT,
	tv_errors_preActivation                  INT,
	tv_errors_connection                     INT,
	tv_errors_activation                     INT,
	errors_authentication                    INT,
	errors_preActivation                     INT,
	errors_activation                        INT,
	other_errors                             INT,
	interaction                              INT,
	modem_setup_success                      INT,
	router_setup_success                     INT,
	tv_setup_success                         INT,
	voice_setup_success                      INT,
	total_devices                            INT,
	tv_checklist_page_view                   INT,
	tv_activating                            INT,
	tv_connecting                            INT,
	pending_modem_count                      INT,
	pending_router_count                     INT,
	pending_tv_count                         INT,
	pending_voice_count                      INT
)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Aggregate - With PII (3 Years)")
