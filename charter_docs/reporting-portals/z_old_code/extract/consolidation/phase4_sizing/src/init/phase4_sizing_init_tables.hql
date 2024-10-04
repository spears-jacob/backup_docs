USE ${env:ENVIRONMENT};

SELECT '***** Creating asp_phase4_sizing table ******'
;

DROP TABLE IF EXISTS asp_phase4_sizing;
CREATE TABLE IF NOT EXISTS asp_phase4_sizing (
  customer_type            string,
  account__type           string,
  system__sys             string,
  system__prin            string,
  system__agent           string,
  franchise__agent_cd     string,
  biller                  string,
  active_accounts_with_active_users       bigint,
  total_active_accounts   bigint,
  total_active_users      bigint,
  total_logins            bigint,
  total_accts_w_at_least_1_login  bigint,
  avg_login_cnt           double,
  avg_date_diff           double,
  avg_date_diff2          double,
  id_idm_date             string,
  account_history_date    string,
  fid_start_date          string,
  fid_end_date            string,
  start_date              string,
  end_date                string
) PARTITIONED BY (domain string, partition_date_month STRING)
;

SELECT '***** Creating asp_v_phase4_sizing view ******'
;

CREATE VIEW IF NOT EXISTS asp_v_phase4_sizing AS
  select * from  asp_phase4_sizing;

SELECT '***** Table and view creation complete ******'
;
