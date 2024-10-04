locals {
  db_name = "${var.env}_${var.flow_category}"
  path_to_daily_ddl_scripts = "${local.path_to_ddl_scripts_dir}/daily"


  tables_config = map(
  "aggregates-pii", [
    ### DASP PII TABLES ###


    {
      db_table_name = "${local.db_name}.asp_onlineform_errors_dashboard"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_onlineform_errors_dashboard.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_voice_of_customer_mobile_troubleshooting"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_voice_of_customer_mobile_troubleshooting.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_voice_of_customer_troubleshooting"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_voice_of_customer_troubleshooting.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_voice_of_customer_cable"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_voice_of_customer_cable.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_voice_of_customer_mobile"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_voice_of_customer_mobile.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_adhoc_sspp_events"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_adhoc_sspp_events.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_asapp_visitid_data_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_asapp_visitid_data_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_asapp_visitid_data_daily_pvt"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_asapp_visitid_data_daily_pvt.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_login_data_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_login_data_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_extract_login_data_daily_pvt"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_login_data_daily_pvt.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_usage_ccpa_daily_table_a"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_a.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_usage_ccpa_daily_table_b"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_b.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_usage_ccpa_daily_table_c"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_c.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_support_quantum_events"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_quantum_events.hql"
    },
    {
      db_table_name = "${local.db_name}.quantum_metric_agg_portals"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_metric_agg_portals.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_visit_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_visit_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.idm_quality_visit_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/idm_quality_visit_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_visit_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_visit_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_pmai_sia_device_out"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_pmai_sia_device_out.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_adhoc_outage_alerts"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_adhoc_outage_alerts.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_adhoc_cpni_verification"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_adhoc_cpni_verification.hql"
    },
    {
      db_table_name = "${local.db_name}.mvno_accounts"
      ddl_path = "${local.path_to_daily_ddl_scripts}/mvno_accounts.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_specmobile_login"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_specmobile_login.hql"
    },
    {
      db_table_name = "${local.db_name}.mvno_sspp_activity"
      ddl_path = "${local.path_to_daily_ddl_scripts}/mvno_sspp_activity.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_visit_agg_eft"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_visit_agg_eft.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_m2_metric_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_m2_metric_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_m2_activation_completion"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_m2_activation_completion.hql"
    },
    {
      db_table_name = "${local.db_name}.eft_device_catalog"
      ddl_path = "${local.path_to_daily_ddl_scripts}/eft_device_catalog.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_intents_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_intents_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_intents_ended_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_intents_ended_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_metadata_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metadata_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_metadata_ended_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metadata_ended_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_metrics_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metrics_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_metrics_ended_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metrics_ended_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_csid_containment_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_csid_containment_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_customer_params_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_customer_params_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_flow_completions_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_flow_completions_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_augmentation_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_augmentation_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_utterances_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_utterances_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_metric_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_metric_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_transaction_events_base"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_transaction_events_base.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_acct_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_acct_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_action"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_action.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_login"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_login.hql"
    }
  ],
  "aggregates-nopii", [
    ### DASP NO-PII TABLES ###
    {
      db_table_name = "${local.db_name}.asp_msaweb_troubleshoot_raw"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_msaweb_troubleshoot_raw.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_msa_ahw"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_msa_ahw.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_support_content_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_content_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.quantum_set_agg_portals"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_api_responses_raw"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_api_responses_raw.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_usage_ccpa_daily_table_c"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_c.hql"
    },
    {
      db_table_name = "${local.db_name}.quantum_customer_feedback"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_customer_feedback.hql"
    },
    {
      db_table_name = "${local.db_name}.quantum_screen_resolutions_visits"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_screen_resolutions_visits.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_privacysite_metric_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_privacysite_metric_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_privacysite_set_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_privacysite_set_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_kpi"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_app_daily_app_figures"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_daily_app_figures.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_app_daily_app_figures_reviews"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_daily_app_figures_reviews.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_app_figures_star_ratings_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_figures_star_ratings_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_time"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_time.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_metrics"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_metrics.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_metrics_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_metrics_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_time_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_time_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_last_6months"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_last_6months.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_product_monthly_last_month"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_last_month.hql"
    },
    {
      db_table_name = "${local.db_name}.app_figures_downloads"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_downloads.hql"
    },
    {
      db_table_name = "${local.db_name}.app_figures_ranks"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_ranks.hql"
    },
    {
      db_table_name = "${local.db_name}.app_figures_sentiment"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_sentiment.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_convos_metadata"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metadata.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_kpi_core"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi_core.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_kpi_mos"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi_mos.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_kpi_dist"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi_dist.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_portals_ytd_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_portals_ytd_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_quality_bucket_distribution"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_bucket_distribution.hql"
    },
    {
      db_table_name = "${local.db_name}.idm_quality_kpi_mos"
      ddl_path = "${local.path_to_daily_ddl_scripts}/idm_quality_kpi_mos.hql"
    },
    {
      db_table_name = "${local.db_name}.auth_login_duration_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/auth_login_duration_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.login_duration_base"
      ddl_path = "${local.path_to_daily_ddl_scripts}/login_duration_base.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_component_distribution"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_component_distribution.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_visit_agg_tableau"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_visit_agg_tableau.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_monthly_referring_traffic"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_monthly_referring_traffic.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_error_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_error_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_support_article_call_disposition"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_article_call_disposition.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_support_article_call_prep"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_article_call_prep.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monthly"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monthly.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monthly_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monthly_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_expected_unexpected_mappings"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_expected_unexpected_mappings.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_m2_kpis"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_m2_kpis.hql"
    },
    {
      db_table_name = "${local.db_name}.eft_success_rates_tableau"
      ddl_path = "${local.path_to_daily_ddl_scripts}/eft_success_rates_tableau.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_customer_feedback_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_customer_feedback_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_export_row_counts_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_export_row_counts_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_flow_detail_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_flow_detail_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_intents_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_intents_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_issue_queues_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_issue_queues_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_activity_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_activity_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_attributes_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_attributes_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_convos_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_convos_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_hierarchy_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_hierarchy_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_rep_utilized_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_rep_utilized_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_reps_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_reps_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_osc_metrics"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_osc_metrics.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_asapp_transfers_ingest"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_transfers_ingest.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_visitsRanked"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_visitsRanked.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_CIRagg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_CIRagg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_engagedHouseholds"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_engagedHouseholds.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_metric_visit_lookup"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_metric_visit_lookup.hql"
    },
    {
      db_table_name = "${local.db_name}.mvno_avgperlinecount_buckets_prod"
      ddl_path = "${local.path_to_daily_ddl_scripts}/mvno_avgperlinecount_buckets_prod.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_apiJoin"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_apiJoin.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_apiRaw"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_apiRaw.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_apiagg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_apiagg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_apiFinal"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_apiFinal.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_api_score"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_api_score.hql"
    },
    {
      db_table_name = "${local.db_name}.quality_app_startup_base"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quality_m2_app_startup.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_medallia_interceptsurvey"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_medallia_interceptsurvey.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_m2dot0_set_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_m2dot0_set_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_null"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_null.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_null_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_null_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_cqe"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_cqe.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_cqe_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_cqe_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_da"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_da.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_da_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_da_archive.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_range"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_range.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_outlier"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_outlier.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_monitor_log"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_monitor_log.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_msa_web_troubleshoot_all_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_msa_web_troubleshoot_all_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_digital_adoption_daily_check"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_digital_adoption_daily_check.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_de_drilldown"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_de_drilldown.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_de_visit_drilldown"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_de_visit_drilldown.hql"
    },
     {
      db_table_name = "${local.db_name}.asp_scp_portals_actions_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_actions_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_billing_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_billing_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_product"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_product.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_scp_portals_subscriber_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_scp_portals_subscriber_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.asp_service_appts"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_service_appts.hql"
    }
  ]
  )
}
