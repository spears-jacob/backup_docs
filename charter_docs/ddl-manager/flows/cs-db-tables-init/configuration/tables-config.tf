locals {

  tables_config = map(
  "aggregates-nopii", [
    {
      db_table_name = "${var.env}.cs_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_in_rate.hql"
    },
    {
      db_table_name = "${var.env}.cs_page_view_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_page_view_call_in_rate.hql"
    },
    {
      db_table_name = "${var.env}.cs_site_section_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_site_section_call_in_rate.hql"
    },
    {
      db_table_name = "${var.env}.cs_call_care_data_agg_no_call_type"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_agg_no_call_type.hql"
    },
    {
      db_table_name = "${var.env}.cs_call_care_data_agg"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_agg.hql"
    },
    {
      db_table_name = "${var.env}.cs_prod_monthly_fiscal_month_metrics"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_prod_monthly_fiscal_month_metrics.hql"
    },
    {
      db_table_name = "${var.env}.cs_calls_with_prior_visits_agg"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calls_with_prior_visits_agg.hql"
    },
    {
      db_table_name = "${var.env}.cs_calendar_monthly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calendar_monthly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_calendar_monthly_quantum_pageview_cid_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calendar_monthly_quantum_pageview_cid_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_calendar_monthly_quantum_pageview_cmp_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calendar_monthly_quantum_pageview_cmp_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_daily_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_daily_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_daily_quantum_pageview_cid_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_daily_quantum_pageview_cid_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_daily_quantum_pageview_cmp_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_daily_quantum_pageview_cmp_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_monthly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_monthly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_monthly_quantum_pageview_cid_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_monthly_quantum_pageview_cid_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_monthly_quantum_pageview_cmp_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_monthly_quantum_pageview_cmp_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_weekly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_weekly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_weekly_quantum_pageview_cid_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_weekly_quantum_pageview_cid_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_weekly_quantum_pageview_cmp_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_weekly_quantum_pageview_cmp_aggregate.hql"
    },
    {
      db_table_name = "${var.env}.cs_dispositions"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions.hql"
    },
    {
      db_table_name = "${var.env}.cs_dispositions_resolution_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_resolution_groups.hql"
    },
    {
      db_table_name = "${var.env}.cs_dispositions_cause_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_cause_groups.hql"
    },
    {
      db_table_name = "${var.env}.cs_dispositions_issue_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_issue_groups.hql"
    },
    {
      db_table_name = "${var.env}.cs_qc"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_qc.hql"
    },
    {
      db_table_name = "${var.env}.cs_msa_adoption_final"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_adoption_final.hql"
    },
    {
      db_table_name = "${var.env}.cs_msa_adoption_testing"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_adoption_testing.hql"
    },
    {
      db_table_name = "${var.env}.cs_msa_use_table"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_use_table.hql"
    }
  ],
  "events-pii", [
    {
      db_table_name = "${var.env}.cs_calls_with_prior_visits"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calls_with_prior_visits.hql"
    },
    {
      db_table_name = "${var.env}.cs_quantum_cid_pageviews"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_quantum_cid_pageviews.hql"
    },
    {
      db_table_name = "${var.env}.cs_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_selectaction_aggregate.hql"
    }
  ]
  )
}
