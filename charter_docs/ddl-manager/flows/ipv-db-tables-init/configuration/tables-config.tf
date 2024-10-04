locals {

  path_to_daily_ddl_scripts = "${local.path_to_ddl_scripts_dir}/daily"
  path_to_weekly_ddl_scripts = "${local.path_to_ddl_scripts_dir}/weekly"
  path_to_monthly_ddl_scripts = "${local.path_to_ddl_scripts_dir}/monthly"

  tables_config = map(
    "general", [],
    "events-pii", [],
    "aggregates-pii", [
      {
        db_table_name = "${var.env}.quantum_acct_agg_daily"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_acct_agg_daily.hql"
      },
      {
        db_table_name = "${var.env}.quantum_acct_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_acct_agg.hql"
      },
      {
        db_table_name = "${var.env}.daily_consumption_report"
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_consumption_report.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_buy_flow_page_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_buy_flow_page_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_buy_flow_acct_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_buy_flow_acct_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_buy_flow_eligible_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_buy_flow_eligible_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_stream_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/stream-agg/quantum_stream_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_deeplinking_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_deeplinking_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_roku_ad"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_roku_ad.hql"
      },
      {
        db_table_name = "${var.env}.quantum_viewership"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_viewership.hql"
      },
      {
        db_table_name = "${var.env}.quantum_metric_agg",
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_metric_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_metric_agg",
        ddl_path = "${local.path_to_daily_ddl_scripts}/tmp_quantum_metric_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_page_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_page_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_customer_engagement_report"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_customer_engagement_report.hql"
      },
      {
        db_table_name = "${var.env}.quantum_apple_device_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_apple_device_agg.hql"
      },
       {
        db_table_name = "${var.env}.quantum_appletv_device_sales"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_appletv_device_sales.hql"
      },
      {
        db_table_name = "nifi.quantum_appletv_device_sales"
        ddl_path = "${local.path_to_daily_ddl_scripts}/nifi_quantum_appletv_device_sales.hql"
      },
      {
        db_table_name = "${var.env}.quantum_stream_mos_tableau"
        ddl_path = "${local.path_to_daily_ddl_scripts}/stream-agg/quantum_stream_mos_tableau.hql"
      },
      {
        db_table_name = "${var.env}.quantum_mos_deciles_agg_tableau"
        ddl_path = "${local.path_to_daily_ddl_scripts}/stream-agg/quantum_mos_deciles_agg_tableau.hql"
      },
      {
        db_table_name = "${var.env}.quantum_ccpa_session_base"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_ccpa_session_base_export.hql"
      },
      {
        db_table_name = "${var.env}.quantum_ccpa_events_base"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_ccpa_events_base_export.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_atoms_video_categories"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_atoms_video_categories.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_buy_flow_fallout"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_buy_flow_fallout.hql"
      },
      {
        db_table_name = "${var.env}.daily_quantum_atoms_video_categories"
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_quantum_atoms_video_categories.hql"
      },
      {
        db_table_name = "${var.env}.quantum_appletv_device_sales_completed"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_appletv_device_sales_completed.hql"
      },
      {
        db_table_name = "${var.env}.daily_quantum_visit_app_entry",
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_quantum_visit_app_entry.hql"
      }
    ],
    "aggregates-nopii", [
    ### DAILY TABLES ###

      {
        db_table_name = "${var.env}.quantum_concurrent_streams_bulkmdu_specu"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_concurrent_streams_bulkmdu_specu.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_set_agg_search_new_logic"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_set_agg_search_new_logic.hql"
      },
      {
        db_table_name = "${var.env}.quantum_search_selected_string_agg_daily"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_search_selected_string_agg_daily.hql"
      },
      {
        db_table_name = "${var.env}.daily_consumption_report_app_version"
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_consumption_report_app_version.hql"
      },
      {
        db_table_name = "${var.env}.daily_consumption_report_hist"
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_consumption_report_hist.hql"
      },
      {
        db_table_name = "${var.env}.quantum_stream_agg_last_message"
        ddl_path = "${local.path_to_daily_ddl_scripts}/stream-agg/quantum_stream_agg_last_message.hql"
      },
      {
        db_table_name = "${var.env}.quantum_video_manifest"
        ddl_path = "${local.path_to_daily_ddl_scripts}/stream-agg/quantum_video_manifest.hql"
      },
      {
        db_table_name = "${var.env}.quantum_api_set_daily_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_api_set_daily_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_unknown_api_paths"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_unknown_api_paths.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_set_agg_tvod"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_set_agg_tvod.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_feature_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_feature_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_set_agg_search_new_logic"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_monthly_set_agg_search_new_logic.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_set_agg_page"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_monthly_set_agg_page.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_set_agg_tvod"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_monthly_set_agg_tvod.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_set_agg_tvod"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_weekly_set_agg_tvod.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_set_agg_page"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_set_agg_page.hql"
      },
      {
        db_table_name = "${var.env}.quantum_mirroring_devices"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_mirroring_devices.hql"
      },
      {
        db_table_name = "${var.env}.quantum_enrichment_status"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_enrichment_status.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_tms_enrichment_report"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_tms_enrichment_report.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_set_agg_watch_next_episode"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_set_agg_watch_next_episode.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_page_pathing_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_page_pathing_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_customer_engagement_report_daily"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_customer_engagement_report_daily.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_viewership_monitor"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_viewership_monitor.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_deeplink_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_deeplink_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_mos_tableau"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_mos_tableau.hql"
      },
      {
        db_table_name = "${var.env}.quantum_concurrent_streams"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_concurrent_streams.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_sso_auth_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_sso_auth_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_apple_device_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_apple_device_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_content_analysis_daily"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_content_analysis_daily.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_set_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_daily_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/tmp_quantum_daily_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_errors_daily_set_agg"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_errors_daily_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_mos_ss_poc"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_mos_ss_poc.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_stva_update"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_stva_update.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_app_nav_quality"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_app_nav_quality.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_app_entry_quality"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_app_entry_quality.hql"
      },
      {
        db_table_name = "${var.env}.quantum_originals_tms_ids"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_originals_tms_ids.hql"
      },
      {
        db_table_name = "${var.env}.quantum_daily_alarms"
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_daily_alarms.hql"
      },
      {
        db_table_name = "${var.env}.quantum_app_entry_browsing_tableau",
        ddl_path = "${local.path_to_daily_ddl_scripts}/quantum_app_entry_browsing_tableau.hql"
      },
      {
        db_table_name = "${var.env}.daily_quantum_app_entry_metric_agg",
        ddl_path = "${local.path_to_daily_ddl_scripts}/daily_quantum_app_entry_metric_agg.hql"
      },
    ### WEEKLY TABLES ###
      {
        db_table_name = "${var.env}.quantum_api_set_weekly_agg",
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_api_set_weekly_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_api_set_weekly_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/tmp_quantum_api_set_weekly_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_content_analysis_weekly"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_content_analysis_weekly.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_feature_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_feature_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_errors_weekly_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_errors_weekly_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_deeplink_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_deeplink_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_search_selected_string_agg_weekly"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_search_selected_string_agg_weekly.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_set_agg_search_new_logic"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_set_agg_search_new_logic.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_set_agg_page"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_set_agg_page.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_sso_auth_set_agg",
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_sso_auth_set_agg.hql",
      },
      {
        db_table_name = "${var.env}.quantum_weekly_apple_device_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_apple_device_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_house_stream_perc"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_house_stream_perc.hql"
      },
      {
        db_table_name = "${var.env}.quantum_bulk_accounts_info"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_bulk_accounts_info.hql"
      },
      {
        db_table_name = "${var.env}.quantum_mos_tableau_weekly"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_mos_tableau_weekly.hql"
      },
      {
        db_table_name = "${var.env}.QUANTUM_WEEKLY_WATCH_TIME_HISTOGRAM"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_watch_time_histogram.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_weekly_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/tmp_quantum_weekly_set_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_weekly_set_agg_4w"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/tmp_quantum_weekly_set_agg_4w.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_set_agg"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_weekly_mirroring_devices"
        ddl_path = "${local.path_to_weekly_ddl_scripts}/quantum_weekly_mirroring_devices.hql"
      },
    ### MONTHLY TABLES ###
      {
        db_table_name = "${var.env}.quantum_api_set_monthly_agg",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_api_set_monthly_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_api_set_monthly_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/tmp_quantum_api_set_monthly_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_feature_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_feature_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_content_analysis_monthly"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_content_analysis_monthly.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_deeplink_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_deeplink_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_dma_analysis",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_dma_analysis.hql"
      },
      {
        db_table_name = "${var.env}.quantum_dma_master"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_dma_master.hql"
      },
      {
        db_table_name = "${var.env}.quantum_errors_monthly_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_errors_monthly_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_set_agg.hql"
      },
      {
        db_table_name = "tmp_${var.env}.quantum_monthly_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_bulk_accounts_info"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_bulk_accounts_info.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_sso_auth_set_agg",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_sso_auth_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_monthly_apple_device_set_agg"
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_monthly_apple_device_set_agg.hql"
      },
      {
        db_table_name = "${var.env}.quantum_programmer_analysis_monthly",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_programmer_analysis_monthly.hql"
      },
      {
        db_table_name = "${var.env}.quantum_programmer_analysis_all_days_monthly",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/quantum_programmer_analysis_all_days_monthly.hql"
      },
      {
        db_table_name = "${var.env}.programmer_monthly_daily",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/programmer_monthly_daily.hql"
      },
      {
        db_table_name = "${var.env}.programmer_monthly_daily",
        ddl_path = "${local.path_to_monthly_ddl_scripts}/programmer_monthly_daily.hql"
      }
    ]
  )
}
