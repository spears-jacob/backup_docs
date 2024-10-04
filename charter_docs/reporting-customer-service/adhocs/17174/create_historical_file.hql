set hive.cli.print.header=true;

SELECT prod.aes_decrypt(account_number) as account_number, myspectrum, specnet, spectrumbusiness
FROM dev_tmp.cs_dps_update_summary
--limit 10
;
