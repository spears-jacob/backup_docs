locals {
  db_name = "${var.env}_dasp"
  path_to_ddl_scripts = "${local.path_to_ddl_scripts_dir}"


  tables_config = map(
  "aggregates-pii", [
    ### SI PII TABLES ###
    {
      db_table_name = "${local.db_name}.si_summary_page_base_master"
      ddl_path = "${local.path_to_ddl_scripts}/si_summary_page_base_master.hql"
    },
    {
      db_table_name = "${local.db_name}.si_summary_agg_master"
      ddl_path = "${local.path_to_ddl_scripts}/si_summary_agg_master.hql"
    },
    {
      db_table_name = "${local.db_name}.si_summary_call_agg_master"
      ddl_path = "${local.path_to_ddl_scripts}/si_summary_call_agg_master.hql"
    },
    {
      db_table_name = "${local.db_name}.si_cross_platform"
      ddl_path = "${local.path_to_ddl_scripts}/si_cross_platform.hql"
    },
     {
      db_table_name = "${local.db_name}.si_composite_score_kpi_agg_visit_daily"
      ddl_path = "${local.path_to_ddl_scripts}/si_composite_score_kpi_agg_visit_daily.hql"
    },
     {
      db_table_name = "${local.db_name}.si_core_quantum_events"
      ddl_path = "${local.path_to_ddl_scripts}/si_core_quantum_events.hql"
    },
     {
      db_table_name = "${local.db_name}.si_summary_tableau"
      ddl_path = "${local.path_to_ddl_scripts}/si_summary_tableau.hql"
    },
    {
     db_table_name = "${local.db_name}.si_lkp_encrypted_accounts"
     ddl_path = "${local.path_to_ddl_scripts}/si_lkp_encrypted_accounts.hql"
   },
    {
      db_table_name = "${local.db_name}.stg_msa_collateral"
      ddl_path = "${local.path_to_ddl_scripts}/stg_msa_collateral.hql"
    },
    {
      db_table_name = "${local.db_name}.si_etos_collateral"
      ddl_path = "${local.path_to_ddl_scripts}/si_etos_collateral.hql"
    },
    {
      db_table_name = "${local.db_name}.si_etos_collateral_incoming"
      ddl_path = "${local.path_to_ddl_scripts}/si_etos_collateral_incoming.hql"
    }

   ],
  "aggregates-nopii", [
    ### SI NO-PII TABLES ###
    {
      db_table_name = "${local.db_name}.si_composite_score_kpi_dist_daily"
      ddl_path = "${local.path_to_ddl_scripts}/si_composite_score_kpi_dist_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.si_composite_score_kpi_agg_daily"
      ddl_path = "${local.path_to_ddl_scripts}/si_composite_score_kpi_agg_daily.hql"
    },
    {
      db_table_name = "${local.db_name}.si_activation_channels"
      ddl_path = "${local.path_to_ddl_scripts}/si_activation_channels.hql"
    },
    {
      db_table_name = "${local.db_name}.si_agg_activation_channel"
      ddl_path = "${local.path_to_ddl_scripts}/si_agg_activation_channel.hql"
	},
    {
      db_table_name = "${local.db_name}.si_summary_collateral_agg_nf"
      ddl_path = "${local.path_to_ddl_scripts}/si_summary_collateral_agg_nf.hql"
	}
  ]
  )
}
