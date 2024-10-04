TRUNCATE TABLE prod.cs_call_in_rate;

INSERT INTO TABLE prod.cs_call_in_rate PARTITION (call_date)
SELECT * FROM prod.cs_call_in_rate_corrected_20191204
;
