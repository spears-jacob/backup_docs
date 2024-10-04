----- adhoc query for Gina 1/11/18

-- Request is for encrypted account numbers, decrypted account numbers, and the most recent timestamp of the accounts visit at the following custom links:
-- View Current Bill, Statement, View Statement
SELECT
visit__account__account_number,
prod.aes_decrypt(visit__account__account_number) as dec_account_number,
-- this function decrypts account numbers
message__name AS custom_link,
max(epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver')) as recent_timestamp
-- converts unix time to standard timestamp
FROM prod.net_events
WHERE (partition_date BETWEEN '2017-10-22' AND '2017-11-21')
-- Requestor asked for November fiscal
AND message__category = 'Custom Link' 
AND message__name IN('View Current Bill','Statement','View Statement') 
-- Requestor specified these custom links
and visit__account__account_number is not null
GROUP BY
visit__account__account_number,
message__name

;