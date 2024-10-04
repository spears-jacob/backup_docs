

USE ${env:ENVIRONMENT};

--INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.steve_call_data PARTITION (call_end_date_utc)
--SELECT * FROM dev.steve_call_data;
