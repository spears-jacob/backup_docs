locals {
  db_name = "${var.env}_dasp"
  # had to hardcore flow_category to 'dasp' due cs flow tables should use dasp db
  tables_config = map(
  "aggregates-nopii", [
    {
      db_table_name = "${local.db_name}.cs_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_in_rate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_page_view_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_page_view_call_in_rate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_site_section_call_in_rate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_site_section_call_in_rate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_prodmonthly_call_in_rate_source"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_prodmonthly_call_in_rate_source.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_agg_no_call_type"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_agg_no_call_type.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_agg"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_prod_monthly_fiscal_month_metrics"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_prod_monthly_fiscal_month_metrics.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_calls_with_prior_visits_agg"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calls_with_prior_visits_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_cid_cmp_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_cid_cmp_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_calendar_monthly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calendar_monthly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_daily_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_daily_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_monthly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_monthly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_weekly_pageview_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_weekly_pageview_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_dispositions"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_dispositions_resolution_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_resolution_groups.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_dispositions_cause_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_cause_groups.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_dispositions_issue_groups"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dispositions_issue_groups.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_qc"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_qc.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_msa_adoption_final"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_adoption_final.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_msa_adoption_testing"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_adoption_testing.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_msa_use_table"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_msa_use_table.hql"
    }

  ],
  "events-pii", [
    {
      db_table_name = "${local.db_name}.cs_initiate_si_metric_agg"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_initiate_si_metric_agg.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_initiate_si_and_pro_install_accounts"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_initiate_si_and_pro_install_accounts.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_calls_with_prior_visits"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_calls_with_prior_visits.hql"
    },
    {
      db_table_name = "${local.db_name}.atom_cs_call_care_data_3_prod_copy"
      ddl_path = "${local.path_to_ddl_scripts_dir}/atom_cs_call_care_data_3_prod_copy.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_sun"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_sun.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_mon"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_mon.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_tue"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_tue.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_wed"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_wed.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_thu"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_thu.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_fri"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_fri.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_call_care_data_sat"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_call_care_data_sat.hql"
    },

    {
      db_table_name = "${local.db_name}.cs_selectaction_aggregate"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_selectaction_aggregate.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_cid_cmp_extract"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_cid_cmp_extract.hql"
    }
  ]
  )
}
