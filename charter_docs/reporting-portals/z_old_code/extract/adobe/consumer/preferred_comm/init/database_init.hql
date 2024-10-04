-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS net_preferred_comm(
  contact_info_updated bigint,
  preferences_set bigint,
  enrolled_paperlessbilling bigint)
  PARTITIONED BY (report_date STRING)
  STORED AS orc TBLPROPERTIES('serialization.null.format'='');
