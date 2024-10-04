CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_customer_feedback(
  UserFeedback string,
  FeedbackCategory string,
  FeedbackForm string,
  footprint string,
  OS string,
  VisitId string,
  ThirdPartySessionID string,
  ApplicationName string,
  ApplicationType string,
  FeedbackCount string
)
PARTITIONED BY (DateDenver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
