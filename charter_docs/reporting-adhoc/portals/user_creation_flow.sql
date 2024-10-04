--- XGANALYTIC-9555
--- Count of visits with a successful user creation
Select
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.bam' THEN visit__visit_id ELSE NULL END)) + 
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.btm' THEN visit__visit_id ELSE NULL END)) + 
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.nbtm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_all,
count(*) as count_instances
from 
prod.net_events
where
partition_date between '2017-11-22' and '2017-12-21' and
(message__name = 'my-account.create-id-final.bam' OR
message__name = 'my-account.create-id-final.btm' OR
message__name = 'my-account.create-id-final.nbtm')

--- XGANALYTIC-9615
--- List of account numbers who completed a user creation
Select
distinct prod.aes_decrypt(b.visit__account__account_number)
from 
--- Account numbers are missing from a majority of the user creation message_name events. Joining on visit_id to extract the account number from other parts of the visit.
prod.net_events a left join prod.net_events b on a.visit__visit_id = b.visit__visit_id
where
prod.aes_decrypt(b.visit__account__account_number) is not null AND
a.partition_date between '2017-11-22' and '2017-12-21' and
b.partition_date between '2017-11-22' and '2017-12-21' and
(a.message__name = 'my-account.create-id-final.bam' OR
a.message__name = 'my-account.create-id-final.btm' OR
a.message__name = 'my-account.create-id-final.nbtm')