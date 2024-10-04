Job name in AWS is daily_digital_adoption (note underscores rather than dashes)
Typical job run time 3-4 hours

2022-04-27
Amanda Ramsay
https://jira.charter.com/browse/XGANALYTIC-36007

There's a front-end defect that's not passing on MSO in some rows.  This ticket
  adds rows that are missing MSO (but not account number) to the set of data
  to be enriched from BTM,and uses the BTM mso if one isn't available in CQE.
