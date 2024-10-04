-- Request from Rupa Chatterji on 1/23/18. Rupa is asking for a list of authenticated accounts in the last 30 days (starting from 01/23/2018). Ticket XGANALYTIC-9496
SELECT
    distinct(prod.aes_decrypt(visit__account__account_number)) as dec_account_number -- if we can generate an account number then it is authenticated
FROM prod.net_events
WHERE (partition_date between '2017-12-25' and '2018-01-23')
and visit__account__account_number is not null