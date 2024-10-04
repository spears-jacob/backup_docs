CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_osc_metrics (
  avg_accountSummaryLoadPage_ms                     STRING
  ,avg_loginDurationResiIDM_ms                      STRING
  ,avg_loginDurationSMBIDM_ms                       STRING
  ,avg_accountSummaryLoadPageSMB_ms                 STRING
  ,avg_loginDurationSpecNet_ms                      STRING
  ,avg_loginDurationSMB_ms                          STRING
  ,avg_accountSummaryUpgradeSMB_ms                  STRING
  ,ASDAutoPayPageViews                              STRING
  ,ASDAutoPaySelectActions                          STRING
  ,ASDAutoPayEnrollments                            STRING
  ,ASDPaperlessPageViews                            STRING
  ,ASDPaperlessSelectActions                        STRING
  ,ASDPaperlessEnrollments                          STRING
  ,ASDSecurityPageViews                             STRING
  ,ASDSecurityEngagements                           STRING
  ,ASDContactPageViews                              STRING
  ,ASDContactEngagements                            STRING
  ,ResiIDMLoginSuccess                              STRING
  ,ResiIDMLoginStart                                STRING
  ,ResiLoginSuccess                                 STRING
  ,ResiLoginStart                                   STRING
  ,SMBIDMLoginSuccess                               STRING
  ,SMBIDMLoginStart                                 STRING
  ,SMBLoginSuccess                                  STRING
  ,SMBLoginStart                                    STRING
  ,SettingsPageViews                                STRING
  ,SettingsButtonClicks                             STRING
)
PARTITIONED BY
(partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
