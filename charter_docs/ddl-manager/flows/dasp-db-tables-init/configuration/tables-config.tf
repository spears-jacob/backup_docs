locals {

  path_to_daily_ddl_scripts = "${local.path_to_ddl_scripts_dir}/daily"


  tables_config = map(
  "aggregates-pii", [
    ### DASP PII TABLES ###
    {
      db_table_name = "${var.env}.asp_extract_asapp_visitid_data_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_asapp_visitid_data_daily.hql"
    },
    {
      db_table_name = "${var.env}.asp_extract_asapp_visitid_data_daily_pvt"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_asapp_visitid_data_daily_pvt.hql"
    },
    {
      db_table_name = "${var.env}.asp_extract_login_data_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_login_data_daily.hql"
    },
    {
      db_table_name = "${var.env}.asp_extract_login_data_daily_pvt"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_extract_login_data_daily_pvt.hql"
    }
  ],
  "aggregates-nopii", [
    ### DASP NO-PII TABLES ###
    {
      db_table_name = "${var.env}.asp_api_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_api_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_billing_set_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_billing_set_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_msa_onboarding_metrics"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_msa_onboarding_metrics.hql"
    },
    {
      db_table_name = "${var.env}.asp_msa_onboarding_time"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_msa_onboarding_time.hql"
    },
    {
      db_table_name = "${var.env}.asp_page_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_page_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_page_set_counts_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_page_set_counts_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_page_set_pathing_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_page_set_pathing_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_support_content_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_content_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_support_quantum_events"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_support_quantum_events.hql"
    },
    {
      db_table_name = "${var.env}.quantum_metric_agg_portals"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_metric_agg_portals.hql"
    },
    {
      db_table_name = "${var.env}.quantum_set_agg_portals"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals.hql"
    },
    {
      db_table_name = "tmp_${var.env}.quantum_set_agg_portals_stage_accounts"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals_stage_accounts.hql"
    },
    {
      db_table_name = "tmp_${var.env}.quantum_set_agg_portals_stage_devices"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals_stage_devices.hql"
    },
    {
      db_table_name = "tmp_${var.env}.quantum_set_agg_portals_stage_instances"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals_stage_instances.hql"
    },
    {
      db_table_name = "tmp_${var.env}.quantum_set_agg_portals_stage_visits"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_set_agg_portals_stage_visits.hql"
    },
    {
      db_table_name = "${var.env}.asp_api_responses_raw"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_api_responses_raw.hql"
    },
    {
      db_table_name = "${var.env}.asp_usage_ccpa_daily_table_a"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_a.hql"
    },
    {
      db_table_name = "${var.env}.asp_usage_ccpa_daily_table_b"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_b.hql"
    },
    {
      db_table_name = "${var.env}.asp_usage_ccpa_daily_table_c"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_c.hql"
    },
    {
      db_table_name = "${var.env}.asp_usage_ccpa_daily_table_c"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_usage_ccpa_daily_table_c.hql"
    },
    {
      db_table_name = "${var.env}.quantum_customer_feedback"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_customer_feedback.hql"
    },
    {
      db_table_name = "${var.env}.asp_idm_paths_flow"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_idm_paths_flow.hql"
    },
    {
      db_table_name = "${var.env}.asp_idm_paths_time"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_idm_paths_time.hql"
    },
    {
      db_table_name = "${var.env}.asp_idm_paths_metrics"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_idm_paths_metrics.hql"
    },
    {
      db_table_name = "${var.env}.asp_bounces_entries"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_bounces_entries.hql"
    },
    {
      db_table_name = "${var.env}.quantum_screen_resolutions_visits"
      ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_screen_resolutions_visits.hql"
    },
    {
      db_table_name = "${var.env}.asp_agg_page_load_time_grouping_sets_quantum"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_agg_page_load_time_grouping_sets_quantum.hql"
    },
    {
      db_table_name = "${var.env}.asp_hourly_page_load_tenths_by_browser_quantum"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_hourly_page_load_tenths_by_browser_quantum.hql"
    },
    {
      db_table_name = "${var.env}.asp_hourly_page_load_tenths_quantum"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_hourly_page_load_tenths_quantum.hql"
    },
    {
      db_table_name = "${var.env}.asp_page_render_time_seconds_page_views_visits"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_page_render_time_seconds_page_views_visits.hql"
    },
    {
      db_table_name = "${var.env}.asp_top_browsers_quantum"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_top_browsers_quantum.hql"
    },
    {
      db_table_name = "${var.env}.asp_daily_report_data"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_daily_report_data.hql"
    },
    {
      db_table_name = "${var.env}.asp_daily_report_data_summary"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_daily_report_data_summary.hql"
    },
    {
      db_table_name = "${var.env}.asp_page_agg_counts"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_page_agg_counts.hql"
    },
    {
      db_table_name = "${var.env}.asp_privacysite_metric_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_privacysite_metric_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_privacysite_set_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_privacysite_set_agg.hql"
    },
    {
      db_table_name = "${var.env}.asp_quality_kpi"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi.hql"
    },
    {
      db_table_name = "${var.env}.asp_app_daily_app_figures"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_daily_app_figures.hql"
    },
    {
      db_table_name = "${var.env}.asp_app_daily_app_figures_reviews"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_daily_app_figures_reviews.hql"
    },
    {
      db_table_name = "${var.env}.asp_app_figures_star_ratings_daily"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_app_figures_star_ratings_daily.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_time"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_time.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_metrics"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_metrics.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_metrics_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_metrics_archive.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_time_archive"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_time_archive.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_last_6months"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_last_6months.hql"
    },
    {
      db_table_name = "${var.env}.asp_product_monthly_last_month"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_product_monthly_last_month.hql"
    },
    {
      db_table_name = "${var.env}.app_figures_downloads"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_downloads.hql"
    },
    {
      db_table_name = "${var.env}.app_figures_ranks"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_ranks.hql"
    },
    {
      db_table_name = "${var.env}.app_figures_sentiment"
      ddl_path = "${local.path_to_daily_ddl_scripts}/app_figures_sentiment.hql"
    },
    {
      db_table_name = "${var.env}.asp_asapp_convos_metadata"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_asapp_convos_metadata.hql"
    },
    {
      db_table_name = "${var.env}.asp_specmobile_events"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_specmobile_events.hql"
    },
    {
      db_table_name = "${var.env}.asp_quality_kpi_core"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi_core.hql"
    },
    {
      db_table_name = "${var.env}.asp_quality_kpi_mos"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_quality_kpi_mos.hql"
    },
    {
      db_table_name = "${var.env}.asp_portals_ytd_agg"
      ddl_path = "${local.path_to_daily_ddl_scripts}/asp_portals_ytd_agg.hql"
    }

  ]
  )
}
