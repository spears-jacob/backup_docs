USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cbo.enable=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.tez.auto.reducer.parallelism=true;
set tez.grouping.max-size=78643200;
set tez.grouping.min-size=52428800;
set hive.exec.reducers.bytes.per.reducer=26214400;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

set hive.groupby.position.alias=true;


--Calculate dates for each date level based on START_DATE: 90 days, 12 weeks, and 3 months
set current_dt = '${hiveconf:START_DATE}';
set current_day_of_week = from_unixtime(unix_timestamp(${hiveconf:current_dt},'yyyy-MM-dd'), 'u');
set weeks_back = 12;
set week_days_back = ${hiveconf:weeks_back} * 7;
set week_days_back_dt = CAST(DATE_SUB(${hiveconf:current_dt}, CAST(${hiveconf:week_days_back} + IF(${hiveconf:current_day_of_week} = 7, 0, ${hiveconf:current_day_of_week}) AS INT)) AS STRING);
set months_back = 3;
set month_days_back = ${hiveconf:months_back} * 30;
set month_days_back_dt = CAST(DATE_SUB(${hiveconf:current_dt}, ${hiveconf:month_days_back}) AS STRING);
set months_back_dt = CAST(TRUNC(DATE_SUB(${hiveconf:current_dt}, ${hiveconf:month_days_back}), 'MM') AS STRING);


--Filtered data to feed Tableau Dashboard
SELECT "\n\nFor 1a: ${env:TMP_db}.si_summary_tableau_filtered\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_tableau_filtered PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_tableau_filtered AS
SELECT *
FROM si_summary_agg_master
WHERE date_level = 'daily'
	AND application_group != 'Self-Install'
	AND denver_date >= ${hiveconf:month_days_back_dt}
;

INSERT INTO TABLE ${env:TMP_db}.si_summary_tableau_filtered
SELECT *
FROM si_summary_agg_master
WHERE date_level = 'weekly'
	AND application_group != 'Self-Install'
	AND denver_date >= ${hiveconf:week_days_back_dt}
;

INSERT INTO TABLE ${env:TMP_db}.si_summary_tableau_filtered
SELECT *
FROM si_summary_agg_master
WHERE date_level = 'monthly'
	AND application_group != 'Self-Install'
	AND denver_date >= ${hiveconf:months_back_dt}
;




--Populate Tableau source
SELECT "\n\nFor 1b: si_summary_tableau\n\n";

ALTER TABLE si_summary_tableau DROP IF EXISTS PARTITION (denver_date >= '2020-09-01');

MSCK REPAIR TABLE si_summary_tableau;

INSERT INTO TABLE si_summary_tableau PARTITION (denver_date)
   SELECT c.visit  as visit_id, c.device as device_id, d.*
   from(
	  SELECT  
			   (acct_id) as acct_id
			 , application_group
			 , application_type
			 , mso
			 , user_package
			 , MAX(initiated_self_installs) AS initiated_self_installs
			 , MAX(initiated_self_installs_new) AS initiated_self_installs_new
			 , MAX(initiated_self_installs_existing) AS initiated_self_installs_existing
			 , MAX(account_status_past_due) AS account_status_past_due
			 , MAX(account_status_current) AS account_status_current
			 , MAX(terms_conditions_accept) AS terms_conditions_accept
			 , MAX(terms_conditions_disagree) AS terms_conditions_disagree
			 , CAST(SUM(inactivity) AS INT)      AS inactivity
			 , CAST(SUM(abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu
			 , MAX(activation_no_touch) AS activation_no_touch
			 , MAX(activation_user_call) AS activation_user_call
			 , MAX(activation_self_install_call) AS activation_self_install_call
			 , MAX(activation_truck_roll_call) AS activation_truck_roll_call
			 , MAX(activation_success_full) AS activation_success_full
			 , MAX(activation_success_partial) AS activation_success_partial
			 , MAX(activation_failure) AS activation_failure
			 , MAX(activation_failure_full) AS activation_failure_full
			 , MAX(activation_failure_partial) AS activation_failure_partial
			 , MAX(activation_failure_calls) AS activation_failure_calls
			 , MAX(activation_failure_truck_rolls) AS activation_failure_truck_rolls
			 ,
				-- adding repair call logic (14Dec)
			   MAX(repair_call) AS repair_call
			 , MAX(repair_truck_roll_call) AS repair_truck_roll_call
			 , MAX(repair_no_touch) AS repair_no_touch
			 , MAX(repair_rescue_no_touch) AS repair_rescue_no_touch
			 , MAX(repair_rescue_call) AS repair_rescue_call
			 , MAX(repair_rescue_truck) AS repair_rescue_truck
			 , MAX(job_class_category_code_t) AS job_class_category_code_t
			 , MAX(job_class_category_code_all) AS job_class_category_code_all
			 ,
				-- end of repair call addition
			   CAST(SUM(re_entry) AS INT) AS re_entry
			 , CAST(SUM(retry) AS INT) AS retry
			 , MAX(errors_connection_issue) AS errors_connection_issue
			 , MAX(errors_equipment_setup) AS errors_equipment_setup
			 , MAX(customer_satisfaction) AS customer_satisfaction
			 , MAX(customer_satisfaction_submitted) AS customer_satisfaction_submitted
			 , MAX(customer_satisfaction_submission_score) AS customer_satisfaction_submission_score
			 , MAX(customer_satisfaction_completed_survey) as customer_satisfaction_completed_survey
			 , CAST(SUM(home_page_view) AS INT) AS home_page_view
			 , CAST(SUM(equipment_setup_page_view) AS INT) AS equipment_setup_page_view
			 , CAST(SUM(terms_conditions_page_view) AS INT) AS terms_conditions_page_view
			 , CAST(SUM(service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view
			 , CAST(SUM(equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view
			 , CAST(SUM(modem_checklist_page_view) AS INT) AS modem_checklist_page_view
			 , CAST(SUM(modem_activation_page_view) AS INT) AS modem_activation_page_view
			 , CAST(SUM(router_checklist_page_view) AS INT) AS router_checklist_page_view
			 , CAST(SUM(network_setup_page_view) AS INT) AS network_setup_page_view
			 , CAST(SUM(router_activation_page_view) AS INT) AS router_activation_page_view
			 , 0 AS rescue_no_touch
			 , 0 AS rescue_call
			 , 0 AS rescue_truck
			 , CASE
						WHEN date_level LIKE '%aily%'
								 THEN 'Daily'
						WHEN date_level LIKE '%eekly%'
								 THEN 'Weekly'
						WHEN date_level LIKE '%onthly%'
								 THEN 'Monthly'
			   END as date_level
			 , metric_type
			 , MAX(errors_authentication_setup) AS errors_authentication_setup
			 , MAX(errors_device_signal_setup) AS errors_device_signal_setup
			 , MAX(errors_other_setup) AS errors_other_setup
			 , CAST(SUM(tv_activation_page_view) AS INT) AS tv_activation_page_view
			 , CAST(SUM(voice_activation_page_view) AS INT) AS voice_activation_page_view
			 , CAST(SUM(download_MSA) AS INT) AS download_MSA
			 ,
				--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
			   AVG(avg_transaction_duration) AS avg_transaction_duration
			 , AVG(avg_api_response_time) AS avg_api_response_time
			 , MAX(abandonment_via_exit_setup) AS abandonment_via_exit_setup
			 , MAX(coam_device) AS coam_device
			 , MAX(whitelisted_coam_device) AS whitelisted_coam_device
			 , MAX(blacklisted_coam_device) AS blacklisted_coam_device
			 , MAX(minimally_qualified_coam_device) AS minimally_qualified_coam_device
			 , 'Distinct Households' as distinct_filter
			 
			 , MAX(equipment_type_scp) AS equipment_type_scp
			 , MAX(equipment_type_monolithic) AS equipment_type_monolithic
			 , MAX(visit_eligible) AS visit_eligible
			 , CAST(SUM(modem_activating) AS INT) AS modem_activating
			 , CAST(SUM(modem_connecting) AS INT) AS modem_connecting
			 , CAST(SUM(router_activating) AS INT) AS router_activating
			 , CAST(SUM(router_connecting) AS INT) AS router_connecting
			 , CAST(SUM(full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view
			 , MAX(activation_complete) AS activation_complete
			 , CAST(SUM(modem_errors_preActivation) AS INT) AS modem_errors_preActivation
			 , CAST(SUM(modem_errors_connection) AS INT) AS modem_errors_connection
			 , CAST(SUM(modem_errors_activation) AS INT) AS modem_errors_activation
			 , CAST(SUM(router_errors_preActivation) AS INT) AS router_errors_preActivation
			 , CAST(SUM(router_errors_connection) AS INT) AS router_errors_connection
			 , CAST(SUM(router_errors_activation) AS INT) AS router_errors_activation
			 , CAST(SUM(tv_errors_preActivation) AS INT) AS tv_errors_preActivation
			 , CAST(SUM(tv_errors_connection) AS INT) AS tv_errors_connection
			 , CAST(SUM(tv_errors_activation) AS INT) AS tv_errors_activation
			 , CAST(SUM(errors_authentication) AS INT) AS errors_authentication
			 , CAST(SUM(errors_preActivation) AS INT) AS errors_preActivation
			 , CAST(SUM(errors_activation) AS INT) AS errors_activation
			 , CAST(SUM(other_errors) AS INT) AS other_errors
			 , CAST(SUM(interaction) AS INT) AS interaction
			 , CAST(MAX(modem_setup_success) AS INT) AS modem_setup_success
			 , CAST(MAX(router_setup_success) AS INT) AS router_setup_success
			 , CAST(MAX(tv_setup_success) AS INT) AS tv_setup_success
			 , CAST(MAX(voice_setup_success) AS INT) AS voice_setup_success
			 , CAST(MAX(total_devices) AS INT) AS total_devices
			 , CAST(SUM(tv_checklist_page_view) AS INT) AS tv_checklist_page_view
			 , CAST(SUM(tv_activating) AS INT) AS tv_activating
			 , CAST(SUM(tv_connecting) AS INT) AS tv_connecting
			 , MAX(pending_modem_count) AS pending_modem_count
			 , MAX(pending_router_count) AS pending_router_count
			 , MAX(pending_tv_count) AS pending_tv_count
			 , MAX(pending_voice_count) AS pending_voice_count
			 , CAST(SUM(modem_errors_postActivation) AS INT) AS modem_errors_postActivation
			 , CAST(SUM(router_errors_postActivation) AS INT) AS router_errors_postActivation
			 , CAST(SUM(tv_errors_postActivation) AS INT) AS tv_errors_postActivation
			 , MAX(size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))) AS package_combo_count
			 , MAX(CASE size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))
				 WHEN 1 THEN 'Single Play'
				 WHEN 2 THEN 'Double Play'
				 WHEN 3 THEN 'Triple Play'
			   END) AS package_combo_text
			 , CAST(SUM(voice_checklist_page_view) AS INT) AS voice_checklist_page_view
			 , CAST(SUM(voice_activating) AS INT) AS voice_activating
			 , denver_date
	  FROM ${env:TMP_db}.si_summary_tableau_filtered
	  where metric_type = 'Activation'
	  GROUP BY
			   acct_id
			 , application_group
			 , application_type
			 , mso
			 , user_package
			 , date_level
			 , denver_date
			 , metric_type
	 ) d
	 LEFT JOIN
	   (
			SELECT 
					 (acct_id) as acc
				   , MIN(visit_id) as visit
				   , MIN(device_id) as device
				   , denver_date
				   , CASE
						  WHEN date_level LIKE '%aily%' THEN 'Daily'
						  WHEN date_level LIKE '%eekly%' THEN 'Weekly'
						  WHEN date_level LIKE '%onthly%' THEN 'Monthly'
					 END as date_level
			FROM ${env:TMP_db}.si_summary_tableau_filtered
			GROUP by 1, 4, 5
	   )
	   c
		   on
			 d.acct_id = c.acc
			 AND D.date_level = C.date_level
			 AND d.denver_date = c.denver_date
;

--INSERT OVERWRITE TABLE si_summary_tableau PARTITION (denver_date)
--   SELECT a.visit as visit_id, b.*
--   from(
--	  SELECT
--			   (device_id) as device_id
--			 , MIN(acct_id) as acct_id
--			 , application_group
--			 , application_type
--			 , mso
--			 , user_package
--			 , MAX(initiated_self_installs) AS initiated_self_installs
--			 , MAX(initiated_self_installs_new) AS initiated_self_installs_new
--			 , MAX(initiated_self_installs_existing) AS initiated_self_installs_existing
--			 , MAX(account_status_past_due) AS account_status_past_due
--			 , MAX(account_status_current) AS account_status_current
--			 , MAX(terms_conditions_accept) AS terms_conditions_accept
--			 , MAX(terms_conditions_disagree) AS terms_conditions_disagree
--			 , CAST(SUM(inactivity) AS INT) AS inactivity
--			 , CAST(SUM(abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu
--			 , MAX(activation_no_touch) AS activation_no_touch
--			 , MAX(activation_user_call) AS activation_user_call
--			 , MAX(activation_self_install_call) AS activation_self_install_call
--			 , MAX(activation_truck_roll_call) AS activation_truck_roll_call
--			 , MAX(activation_success_full) AS activation_success_full
--			 , MAX(activation_success_partial) AS activation_success_partial
--			 , MAX(activation_failure) AS activation_failure
--			 , MAX(activation_failure_full) AS activation_failure_full
--			 , MAX(activation_failure_partial) AS activation_failure_partial
--			 , MAX(activation_failure_calls) AS activation_failure_calls
--			 , MAX(activation_failure_truck_rolls) AS activation_failure_truck_rolls
--			 ,
--				-- adding repair call logic (14Dec)
--			   MAX(repair_call) AS repair_call
--			 , MAX(repair_truck_roll_call) AS repair_truck_roll_call
--			 , MAX(repair_no_touch) AS repair_no_touch
--			 , MAX(repair_rescue_no_touch) AS repair_rescue_no_touch
--			 , MAX(repair_rescue_call) AS repair_rescue_call
--			 , MAX(repair_rescue_truck) AS repair_rescue_truck
--			 , MAX(job_class_category_code_t) AS job_class_category_code_t
--			 , MAX(job_class_category_code_all) AS job_class_category_code_all
--			 ,
--				-- end of repair call addition
--			   CAST(SUM(re_entry) AS INT) AS re_entry
--			 , CAST(SUM(retry) AS    INTEGER) AS retry
--			 , MAX(errors_connection_issue) AS errors_connection_issue
--			 , MAX(errors_equipment_setup) AS errors_equipment_setup
--			 , MAX(customer_satisfaction) AS customer_satisfaction
--			 , MAX(customer_satisfaction_submitted) AS customer_satisfaction_submitted
--			 , MAX(customer_satisfaction_submission_score) AS customer_satisfaction_submission_score
--			 , MAX(customer_satisfaction_completed_survey) as customer_satisfaction_completed_survey
--			 , CAST(SUM(home_page_view) AS INT) AS home_page_view
--			 , CAST(SUM(equipment_setup_page_view) AS INT) AS equipment_setup_page_view
--			 , CAST(SUM(terms_conditions_page_view) AS INT) AS terms_conditions_page_view
--			 , CAST(SUM(service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view
--			 , CAST(SUM(equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view
--			 , CAST(SUM(modem_checklist_page_view) AS INT) AS modem_checklist_page_view
--			 , CAST(SUM(modem_activation_page_view) AS INT) AS modem_activation_page_view
--			 , CAST(SUM(router_checklist_page_view) AS INT) AS router_checklist_page_view
--			 , CAST(SUM(network_setup_page_view) AS INT) AS network_setup_page_view
--			 , CAST(SUM(router_activation_page_view) AS INT) AS router_activation_page_view
--			 , 0 AS rescue_no_touch
--			 , 0 AS rescue_call
--			 , 0 AS rescue_truck
--			 , CASE
--					WHEN date_level LIKE '%aily%' THEN 'Daily'
--					WHEN date_level LIKE '%eekly%' THEN 'Weekly'
--					WHEN date_level LIKE '%onthly%' THEN 'Monthly'
--			   END as date_level
--			 , metric_type
--			 , MAX(errors_authentication_setup) AS errors_authentication_setup
--			 , MAX(errors_device_signal_setup) AS errors_device_signal_setup
--			 , MAX(errors_other_setup) AS errors_other_setup
--			 , CAST(SUM(tv_activation_page_view) AS INT) AS tv_activation_page_view
--			 , CAST(SUM(voice_activation_page_view) AS INT) AS voice_activation_page_view
--			 , CAST(SUM(download_MSA) AS INT) AS download_MSA
--			 ,
--				--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
--			   AVG(avg_transaction_duration) AS avg_transaction_duration
--			 , AVG(avg_api_response_time) AS avg_api_response_time
--			 , MAX(abandonment_via_exit_setup) AS abandonment_via_exit_setup
--			 , MAX(coam_device) AS coam_device
--			 , MAX(whitelisted_coam_device) AS whitelisted_coam_device
--			 , MAX(blacklisted_coam_device) AS blacklisted_coam_device
--			 , MAX(minimally_qualified_coam_device) AS minimally_qualified_coam_device
--			 , 'Distinct Devices' as distinct_filter
--			 
--			 , MAX(equipment_type_scp) AS equipment_type_scp
--			 , MAX(equipment_type_monolithic) AS equipment_type_monolithic
--			 , MAX(visit_eligible) AS visit_eligible
--			 , CAST(SUM(modem_activating) AS INT) AS modem_activating
--			 , CAST(SUM(modem_connecting) AS INT) AS modem_connecting
--			 , CAST(SUM(router_activating) AS INT) AS router_activating
--			 , CAST(SUM(router_connecting) AS INT) AS router_connecting
--			 , CAST(SUM(full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view
--			 , MAX(activation_complete) AS activation_complete
--			 , CAST(SUM(modem_errors_preActivation) AS INT) AS modem_errors_preActivation
--			 , CAST(SUM(modem_errors_connection) AS INT) AS modem_errors_connection
--			 , CAST(SUM(modem_errors_activation) AS INT) AS modem_errors_activation
--			 , CAST(SUM(router_errors_preActivation) AS INT) AS router_errors_preActivation
--			 , CAST(SUM(router_errors_connection) AS INT) AS router_errors_connection
--			 , CAST(SUM(router_errors_activation) AS INT) AS router_errors_activation
--			 , CAST(SUM(tv_errors_preActivation) AS INT) AS tv_errors_preActivation
--			 , CAST(SUM(tv_errors_connection) AS INT) AS tv_errors_connection
--			 , CAST(SUM(tv_errors_activation) AS INT) AS tv_errors_activation
--			 , CAST(SUM(errors_authentication) AS INT) AS errors_authentication
--			 , CAST(SUM(errors_preActivation) AS INT) AS errors_preActivation
--			 , CAST(SUM(errors_activation) AS INT) AS errors_activation
--			 , CAST(SUM(other_errors) AS INT) AS other_errors
--			 , CAST(SUM(interaction) AS INT) AS interaction
--			 , CAST(MAX(modem_setup_success) AS INT) AS modem_setup_success
--			 , CAST(MAX(router_setup_success) AS INT) AS router_setup_success
--			 , CAST(MAX(tv_setup_success) AS INT) AS tv_setup_success
--			 , CAST(MAX(voice_setup_success) AS INT) AS voice_setup_success
--			 , CAST(MAX(total_devices) AS INT) AS total_devices
--			 , CAST(SUM(tv_checklist_page_view) AS INT) AS tv_checklist_page_view
--			 , CAST(SUM(tv_activating) AS INT) AS tv_activating
--			 , CAST(SUM(tv_connecting) AS INT) AS tv_connecting
--			 , MAX(pending_modem_count) AS pending_modem_count
--			 , MAX(pending_router_count) AS pending_router_count
--			 , MAX(pending_tv_count) AS pending_tv_count
--			 , MAX(pending_voice_count) AS pending_voice_count
--			 , CAST(SUM(modem_errors_postActivation) AS INT) AS modem_errors_postActivation
--			 , CAST(SUM(router_errors_postActivation) AS INT) AS router_errors_postActivation
--			 , CAST(SUM(tv_errors_postActivation) AS INT) AS tv_errors_postActivation
--			 , MAX(size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))) AS package_combo_count
--			 , MAX(CASE size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))
--				 WHEN 1 THEN 'Single Play'
--				 WHEN 2 THEN 'Double Play'
--				 WHEN 3 THEN 'Triple Play'
--			   END) AS package_combo_text
--			 , CAST(SUM(voice_checklist_page_view) AS INT) AS voice_checklist_page_view
--			 , CAST(SUM(voice_activating) AS INT) AS voice_activating
--			 , denver_date
--	  FROM ${env:TMP_db}.si_summary_tableau_filtered
--	  where metric_type = 'Activation'
--	  GROUP BY
--			   device_id
--			 , application_group
--			 , application_type
--			 , mso
--			 , user_package
--			 , date_level
--			 , denver_date
--			 , metric_type
--	 ) b
--	 LEFT JOIN
--	   (
--			SELECT 
--					 (device_id)   as device
--				   , MIN(visit_id) as visit
--				   , denver_date
--				   , CASE
--						  WHEN date_level LIKE '%aily%' THEN 'Daily'
--						  WHEN date_level LIKE '%eekly%' THEN 'Weekly'
--						  WHEN date_level LIKE '%onthly%' THEN 'Monthly'
--					 END as date_level
--			FROM ${env:TMP_db}.si_summary_tableau_filtered
--			GROUP by 1, 3, 4
--	   ) a
--		   ON
--			 b.device_id = a.device
--			 AND b.denver_date = a.denver_date
--			 AND b.date_level = a.date_level
--;
----   UNION ALL
--INSERT INTO TABLE si_summary_tableau PARTITION (denver_date)
--   SELECT 
--			 (visit_id) as visit_id
--			 , device_id
--			 , acct_id
--			 , application_group
--			 , application_type
--			 , mso
--			 , user_package
--			 , MAX(initiated_self_installs) AS initiated_self_installs
--			 , MAX(initiated_self_installs_new) AS initiated_self_installs_new
--			 , MAX(initiated_self_installs_existing) AS initiated_self_installs_existing
--			 , MAX(account_status_past_due) AS account_status_past_due
--			 , MAX(account_status_current) AS account_status_current
--			 , MAX(terms_conditions_accept) AS terms_conditions_accept
--			 , MAX(terms_conditions_disagree) AS terms_conditions_disagree
--			 , CAST(SUM(inactivity) AS INT)      AS inactivity
--			 , CAST(SUM(abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu
--			 , MAX(activation_no_touch) AS activation_no_touch
--			 , MAX(activation_user_call) AS activation_user_call
--			 , MAX(activation_self_install_call) AS activation_self_install_call
--			 , MAX(activation_truck_roll_call) AS activation_truck_roll_call
--			 , MAX(activation_success_full) AS activation_success_full
--			 , MAX(activation_success_partial) AS activation_success_partial
--			 , MAX(activation_failure) AS activation_failure
--			 , MAX(activation_failure_full) AS activation_failure_full
--			 , MAX(activation_failure_partial) AS activation_failure_partial
--			 , MAX(activation_failure_calls) AS activation_failure_calls
--			 , MAX(activation_failure_truck_rolls) AS activation_failure_truck_rolls
--			 ,
--			  -- adding repair call logic (14Dec)
--			 MAX(repair_call) AS repair_call
--			 , MAX(repair_truck_roll_call) AS repair_truck_roll_call
--			 , MAX(repair_no_touch) AS repair_no_touch
--			 , MAX(repair_rescue_no_touch) AS repair_rescue_no_touch
--			 , MAX(repair_rescue_call) AS repair_rescue_call
--			 , MAX(repair_rescue_truck) AS repair_rescue_truck
--			 , MAX(job_class_category_code_t) AS job_class_category_code_t
--			 , MAX(job_class_category_code_all) AS job_class_category_code_all
--			 ,
--			  -- end of repair call addition
--			 CAST(SUM(re_entry) AS INT) AS re_entry
--			 , CAST(SUM(retry) AS INT) AS retry
--			 , MAX(errors_connection_issue) AS errors_connection_issue
--			 , MAX(errors_equipment_setup) AS errors_equipment_setup
--			 , MAX(customer_satisfaction) AS customer_satisfaction
--			 , MAX(customer_satisfaction_submitted) AS customer_satisfaction_submitted
--			 , MAX(customer_satisfaction_submission_score) AS customer_satisfaction_submission_score
--			 , MAX(customer_satisfaction_completed_survey) as customer_satisfaction_completed_survey
--			 , CAST(SUM(home_page_view) AS INT) AS home_page_view
--			 , CAST(SUM(equipment_setup_page_view) AS INT) AS equipment_setup_page_view
--			 , CAST(SUM(terms_conditions_page_view) AS INT) AS terms_conditions_page_view
--			 , CAST(SUM(service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view
--			 , CAST(SUM(equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view
--			 , CAST(SUM(modem_checklist_page_view) AS INT) AS modem_checklist_page_view
--			 , CAST(SUM(modem_activation_page_view) AS INT) AS modem_activation_page_view
--			 , CAST(SUM(router_checklist_page_view) AS INT) AS router_checklist_page_view
--			 , CAST(SUM(network_setup_page_view) AS INT) AS network_setup_page_view
--			 , CAST(SUM(router_activation_page_view) AS INT) AS router_activation_page_view
--			 , 0 AS rescue_no_touch
--			 , 0 AS rescue_call
--			 , 0 AS rescue_truck
--			 , CASE
--			 	 WHEN date_level LIKE '%aily%' THEN 'Daily'
--			 	 WHEN date_level LIKE '%eekly%' THEN 'Weekly'
--			 	 WHEN date_level LIKE '%onthly%' THEN 'Monthly'
--			 END as date_level
--			 , metric_type
--			 , MAX(errors_authentication_setup) AS errors_authentication_setup
--			 , MAX(errors_device_signal_setup) AS errors_device_signal_setup
--			 , MAX(errors_other_setup) AS errors_other_setup
--			 , CAST(SUM(tv_activation_page_view) AS INT) AS tv_activation_page_view
--			 , CAST(SUM(voice_activation_page_view) AS INT) AS voice_activation_page_view
--			 , CAST(SUM(download_MSA) AS INT) AS download_MSA
--			 ,
--			  --avg_transaction_duration and avg_api_response_time are populated in Composite Score query
--			 AVG(avg_transaction_duration) AS avg_transaction_duration
--			 , AVG(avg_api_response_time) AS avg_api_response_time
--			 , MAX(abandonment_via_exit_setup) AS abandonment_via_exit_setup
--			 , MAX(coam_device) AS coam_device
--			 , MAX(whitelisted_coam_device) AS whitelisted_coam_device
--			 , MAX(blacklisted_coam_device) AS blacklisted_coam_device
--			 , MAX(minimally_qualified_coam_device) AS minimally_qualified_coam_device
--			 , 'Distinct Visits' as distinct_filter
--			 
--			 , MAX(equipment_type_scp) AS equipment_type_scp
--			 , MAX(equipment_type_monolithic) AS equipment_type_monolithic
--			 , MAX(visit_eligible) AS visit_eligible
--			 , CAST(SUM(modem_activating) AS INT) AS modem_activating
--			 , CAST(SUM(modem_connecting) AS INT) AS modem_connecting
--			 , CAST(SUM(router_activating) AS INT) AS router_activating
--			 , CAST(SUM(router_connecting) AS INT) AS router_connecting
--			 , CAST(SUM(full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view
--			 , MAX(activation_complete) AS activation_complete
--			 , CAST(SUM(modem_errors_preActivation) AS INT) AS modem_errors_preActivation
--			 , CAST(SUM(modem_errors_connection) AS INT) AS modem_errors_connection
--			 , CAST(SUM(modem_errors_activation) AS INT) AS modem_errors_activation
--			 , CAST(SUM(router_errors_preActivation) AS INT) AS router_errors_preActivation
--			 , CAST(SUM(router_errors_connection) AS INT) AS router_errors_connection
--			 , CAST(SUM(router_errors_activation) AS INT) AS router_errors_activation
--			 , CAST(SUM(tv_errors_preActivation) AS INT) AS tv_errors_preActivation
--			 , CAST(SUM(tv_errors_connection) AS INT) AS tv_errors_connection
--			 , CAST(SUM(tv_errors_activation) AS INT) AS tv_errors_activation
--			 , CAST(SUM(errors_authentication) AS INT) AS errors_authentication
--			 , CAST(SUM(errors_preActivation) AS INT) AS errors_preActivation
--			 , CAST(SUM(errors_activation) AS INT) AS errors_activation
--			 , CAST(SUM(other_errors) AS INT) AS other_errors
--			 , CAST(SUM(interaction) AS INT) AS interaction
--			 , CAST(MAX(modem_setup_success) AS INT) AS modem_setup_success
--			 , CAST(MAX(router_setup_success) AS INT) AS router_setup_success
--			 , CAST(MAX(tv_setup_success) AS INT) AS tv_setup_success
--			 , CAST(MAX(voice_setup_success) AS INT) AS voice_setup_success
--			 , CAST(MAX(total_devices) AS INT) AS total_devices
--			 , CAST(SUM(tv_checklist_page_view) AS INT) AS tv_checklist_page_view
--			 , CAST(SUM(tv_activating) AS INT) AS tv_activating
--			 , CAST(SUM(tv_connecting) AS INT) AS tv_connecting
--			 , MAX(pending_modem_count) AS pending_modem_count
--			 , MAX(pending_router_count) AS pending_router_count
--			 , MAX(pending_tv_count) AS pending_tv_count
--			 , MAX(pending_voice_count) AS pending_voice_count
--			 , CAST(SUM(modem_errors_postActivation) AS INT) AS modem_errors_postActivation
--			 , CAST(SUM(router_errors_postActivation) AS INT) AS router_errors_postActivation
--			 , CAST(SUM(tv_errors_postActivation) AS INT) AS tv_errors_postActivation
--			 , MAX(size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))) AS package_combo_count
--			 , MAX(CASE size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))
--				 WHEN 1 THEN 'Single Play'
--				 WHEN 2 THEN 'Double Play'
--				 WHEN 3 THEN 'Triple Play'
--			   END) AS package_combo_text
--			 , CAST(SUM(voice_checklist_page_view) AS INT) AS voice_checklist_page_view
--			 , CAST(SUM(voice_activating) AS INT) AS voice_activating
--			 , denver_date
--   FROM ${env:TMP_db}.si_summary_tableau_filtered
--   where metric_type = 'Activation'
--   GROUP BY
--			visit_id
--		  , device_id
--		  , acct_id
--		  , application_group
--		  , application_type
--		  , mso
--		  , user_package
--		  , date_level
--		  , denver_date
--		  , metric_type
--;
----   UNION ALL
--INSERT INTO TABLE si_summary_tableau PARTITION (denver_date)
--   SELECT f.visit  as visit_id, f.device as device_id, e.*
--   from(
--	  SELECT 
--			   (acct_id) as acct_id
--			 , application_group
--			 , application_type
--			 , mso
--			 , user_package
--			 , MAX(initiated_self_installs) AS initiated_self_installs
--			 , MAX(initiated_self_installs_new) AS initiated_self_installs_new
--			 , MAX(initiated_self_installs_existing) AS initiated_self_installs_existing
--			 , MAX(account_status_past_due) AS account_status_past_due
--			 , MAX(account_status_current) AS account_status_current
--			 , MAX(terms_conditions_accept) AS terms_conditions_accept
--			 , MAX(terms_conditions_disagree) AS terms_conditions_disagree
--			 , CAST(SUM(inactivity) AS INT) AS inactivity
--			 , CAST(SUM(abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu
--			 , MAX(activation_no_touch) AS activation_no_touch
--			 , MAX(activation_user_call) AS activation_user_call
--			 , MAX(activation_self_install_call) AS activation_self_install_call
--			 , MAX(activation_truck_roll_call) AS activation_truck_roll_call
--			 , MAX(activation_success_full) AS activation_success_full
--			 , MAX(activation_success_partial) AS activation_success_partial
--			 , MAX(activation_failure) AS activation_failure
--			 , MAX(activation_failure_full) AS activation_failure_full
--			 , MAX(activation_failure_partial) AS activation_failure_partial
--			 , MAX(activation_failure_calls) AS activation_failure_calls
--			 , MAX(activation_failure_truck_rolls) AS activation_failure_truck_rolls
--			 ,
--				-- adding repair call logic (14Dec)
--			   MAX(repair_call) AS repair_call
--			 , MAX(repair_truck_roll_call) AS repair_truck_roll_call
--			 , MAX(repair_no_touch) AS repair_no_touch
--			 , MAX(repair_rescue_no_touch) AS repair_rescue_no_touch
--			 , MAX(repair_rescue_call) AS repair_rescue_call
--			 , MAX(repair_rescue_truck) AS repair_rescue_truck
--			 , MAX(job_class_category_code_t) AS job_class_category_code_t
--			 , MAX(job_class_category_code_all) AS job_class_category_code_all
--			 ,
--				-- end of repair call addition
--			   CAST(SUM(re_entry) AS INT) AS re_entry
--			 , CAST(SUM(retry) AS INT) AS retry
--			 , MAX(errors_connection_issue) AS errors_connection_issue
--			 , MAX(errors_equipment_setup) AS errors_equipment_setup
--			 , MAX(customer_satisfaction) AS customer_satisfaction
--			 , MAX(customer_satisfaction_submitted) AS customer_satisfaction_submitted
--			 , MAX(customer_satisfaction_submission_score) AS customer_satisfaction_submission_score
--			 , MAX(customer_satisfaction_completed_survey) as customer_satisfaction_completed_survey
--			 , CAST(SUM(home_page_view) AS INT) AS home_page_view
--			 , CAST(SUM(equipment_setup_page_view) AS INT) AS equipment_setup_page_view
--			 , CAST(SUM(terms_conditions_page_view) AS INT) AS terms_conditions_page_view
--			 , CAST(SUM(service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view
--			 , CAST(SUM(equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view
--			 , CAST(SUM(modem_checklist_page_view) AS INT) AS modem_checklist_page_view
--			 , CAST(SUM(modem_activation_page_view) AS INT) AS modem_activation_page_view
--			 , CAST(SUM(router_checklist_page_view) AS INT) AS router_checklist_page_view
--			 , CAST(SUM(network_setup_page_view) AS INT) AS network_setup_page_view
--			 , CAST(SUM(router_activation_page_view) AS INT) AS router_activation_page_view
--			 , MAX(rescue_no_touch) AS rescue_no_touch
--			 , MAX(rescue_call) AS rescue_call
--			 , MAX(rescue_truck) AS rescue_truck
--			 , CASE
--					WHEN date_level LIKE '%aily%' THEN 'Daily'
--					WHEN date_level LIKE '%eekly%' THEN 'Weekly'
--					WHEN date_level LIKE '%onthly%' THEN 'Monthly'
--			   END as date_level
--			 , metric_type
--			 , MAX(errors_authentication_setup) AS errors_authentication_setup
--			 , MAX(errors_device_signal_setup) AS errors_device_signal_setup
--			 , MAX(errors_other_setup) AS errors_other_setup
--			 , CAST(SUM(tv_activation_page_view) AS INT) AS tv_activation_page_view
--			 , CAST(SUM(voice_activation_page_view) AS INT) AS voice_activation_page_view
--			 , CAST(SUM(download_MSA) AS INT) AS download_MSA
--			 ,
--				--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
--			   AVG(avg_transaction_duration) AS avg_transaction_duration
--			 , AVG(avg_api_response_time) AS avg_api_response_time
--			 , MAX(abandonment_via_exit_setup) AS abandonment_via_exit_setup
--			 , MAX(coam_device) AS coam_device
--			 , MAX(whitelisted_coam_device) AS whitelisted_coam_device
--			 , MAX(blacklisted_coam_device) AS blacklisted_coam_device
--			 , MAX(minimally_qualified_coam_device) AS minimally_qualified_coam_device
--			 , 'Distinct Households' as distinct_filter
--			 
--			 , MAX(equipment_type_scp) AS equipment_type_scp
--			 , MAX(equipment_type_monolithic) AS equipment_type_monolithic
--			 , MAX(visit_eligible) AS visit_eligible
--			 , CAST(SUM(modem_activating) AS INT) AS modem_activating
--			 , CAST(SUM(modem_connecting) AS INT) AS modem_connecting
--			 , CAST(SUM(router_activating) AS INT) AS router_activating
--			 , CAST(SUM(router_connecting) AS INT) AS router_connecting
--			 , CAST(SUM(full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view
--			 , MAX(activation_complete) AS activation_complete
--			 , CAST(SUM(modem_errors_preActivation) AS INT) AS modem_errors_preActivation
--			 , CAST(SUM(modem_errors_connection) AS INT) AS modem_errors_connection
--			 , CAST(SUM(modem_errors_activation) AS INT) AS modem_errors_activation
--			 , CAST(SUM(router_errors_preActivation) AS INT) AS router_errors_preActivation
--			 , CAST(SUM(router_errors_connection) AS INT) AS router_errors_connection
--			 , CAST(SUM(router_errors_activation) AS INT) AS router_errors_activation
--			 , CAST(SUM(tv_errors_preActivation) AS INT) AS tv_errors_preActivation
--			 , CAST(SUM(tv_errors_connection) AS INT) AS tv_errors_connection
--			 , CAST(SUM(tv_errors_activation) AS INT) AS tv_errors_activation
--			 , CAST(SUM(errors_authentication) AS INT) AS errors_authentication
--			 , CAST(SUM(errors_preActivation) AS INT) AS errors_preActivation
--			 , CAST(SUM(errors_activation) AS INT) AS errors_activation
--			 , CAST(SUM(other_errors) AS INT) AS other_errors
--			 , CAST(SUM(interaction) AS INT) AS interaction
--			 , CAST(MAX(modem_setup_success) AS INT) AS modem_setup_success
--			 , CAST(MAX(router_setup_success) AS INT) AS router_setup_success
--			 , CAST(MAX(tv_setup_success) AS INT) AS tv_setup_success
--			 , CAST(MAX(voice_setup_success) AS INT) AS voice_setup_success
--			 , CAST(MAX(total_devices) AS INT) AS total_devices
--			 , CAST(SUM(tv_checklist_page_view) AS INT) AS tv_checklist_page_view
--			 , CAST(SUM(tv_activating) AS INT) AS tv_activating
--			 , CAST(SUM(tv_connecting) AS INT) AS tv_connecting
--			 , MAX(pending_modem_count) AS pending_modem_count
--			 , MAX(pending_router_count) AS pending_router_count
--			 , MAX(pending_tv_count) AS pending_tv_count
--			 , MAX(pending_voice_count) AS pending_voice_count
--			 , CAST(SUM(modem_errors_postActivation) AS INT) AS modem_errors_postActivation
--			 , CAST(SUM(router_errors_postActivation) AS INT) AS router_errors_postActivation
--			 , CAST(SUM(tv_errors_postActivation) AS INT) AS tv_errors_postActivation
--			 , MAX(size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))) AS package_combo_count
--			 , MAX(CASE size(split(replace(regexp_replace(user_package, '(Modem|Router)', 'Internet' ), 'Internet+Internet', 'Internet'),'[+]'))
--				 WHEN 1 THEN 'Single Play'
--				 WHEN 2 THEN 'Double Play'
--				 WHEN 3 THEN 'Triple Play'
--			   END) AS package_combo_text
--			 , CAST(SUM(voice_checklist_page_view) AS INT) AS voice_checklist_page_view
--			 , CAST(SUM(voice_activating) AS INT) AS voice_activating
--			 , denver_date
--	  FROM ${env:TMP_db}.si_summary_tableau_filtered
--	  where metric_type = 'Program'
--	  group by
--			   acct_id
--			 , application_group
--			 , application_type
--			 , mso
--			 , user_package
--			 , date_level
--			 , denver_date
--			 , metric_type
--	 ) e
--	 LEFT JOIN
--	   (
--			SELECT 
--					 (acct_id)      as acc
--				   , MIN(visit_id)  as visit
--				   , MIN(device_id) as device
--				   , denver_date
--				   , CASE
--						  WHEN date_level LIKE '%aily%' THEN 'Daily'
--						  WHEN date_level LIKE '%eekly%' THEN 'Weekly'
--						  WHEN date_level LIKE '%onthly%' THEN 'Monthly'
--					 END as date_level
--			FROM ${env:TMP_db}.si_summary_tableau_filtered
--			GROUP by 1, 4, 5
--	   ) f
--		   on
--			 e.acct_id        =f.acc
--			 AND e.date_level = f.date_level
--			 AND e.denver_date=f.denver_date
--;


DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_tableau_filtered PURGE;
