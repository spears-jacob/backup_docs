USE ${env:TMP_db};

TRUNCATE TABLE bhn_bill_pay_raw;
TRUNCATE TABLE bhn_bill_pay_denorm;
TRUNCATE TABLE bhn_bill_pay_calc;
TRUNCATE TABLE bhn_bill_pay_events_no_part;