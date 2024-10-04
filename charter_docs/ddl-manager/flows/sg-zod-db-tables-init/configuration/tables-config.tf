locals {

  path_to_daily_ddl_scripts = "${local.path_to_ddl_scripts_dir}/daily"
  tables_config = map(
  "events-pii", [
    {
      db_table_name = "${var.env}.sg_zodiac_events_utc",
      ddl_path = "${local.path_to_daily_ddl_scripts}/sg_zodiac_events_utc.hql"
    }
  ]
  )
}
