USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

SELECT '***** Creating quantum_customer_feedback table ******'
;


CREATE TABLE IF NOT EXISTS quantum_customer_feedback(
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
;

--------------------------------------------------------------------------------
-- asp_quantum_customer_feedback_v VIEW Creation
--------------------------------------------------------------------------------
DROP VIEW IF EXISTS asp_quantum_customer_feedback_v;

CREATE VIEW asp_quantum_customer_feedback_v AS
select *
from prod.quantum_customer_feedback;
