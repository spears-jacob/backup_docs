SELECT DISTINCT company,domain,'Adobe' AS source FROM asp_metric_pivot_resi_adobe UNION ALL
SELECT DISTINCT company,domain,'Quantum' AS source FROM asp_metric_pivot_resi_quantum UNION ALL
SELECT DISTINCT company,domain,'Adobe' AS source FROM asp_metric_pivot_smb_adobe UNION ALL
SELECT DISTINCT company,domain,'Quantum' AS source FROM asp_metric_pivot_smb_quantum UNION ALL
SELECT DISTINCT company,domain,'Adobe' AS source FROM asp_metric_pivot_app_adobe UNION ALL
SELECT DISTINCT company,domain,'Quantum' AS source FROM asp_metric_pivot_app_quantum;


DROP TABLE dev.asp_metric_pivot_app_quantum;



SELECT DISTINCT visit__application_details__application_name FROM venona_events_portals WHERE partition_date_utc >= '2018-10-14';




SELECT * FROM asp_prod_monthly_resi_adobe_stage;
SELECT * FROM asp_prod_monthly_resi_quantum_stage;
