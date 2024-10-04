DROP VIEW IF EXISTS asp_v_federated_identity;

CREATE VIEW asp_v_federated_identity AS
select trace_id,
       account_number_aes as account_number_aes256,
       platform as source_app,
       footprint,
       is_success,
       response_status as check_result,
       check_level,
       check_result as check_result_new,
       username_aes as username_aes256,
       epoch_timestamp as event_timestamp,
       date_hour_utc,
       date_hour_denver,
       partition_date_denver
from prod.auth_federated_identity;


DROP VIEW IF EXISTS asp_v_auth_federated_identity;

CREATE VIEW asp_v_auth_federated_identity AS
select *
from prod.auth_federated_identity;
