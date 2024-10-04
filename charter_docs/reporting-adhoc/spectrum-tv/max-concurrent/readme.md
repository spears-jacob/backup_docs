JIRA: https://jira.charter.com/browse/XGANALYTIC-6011

From: Marketing

Ask:
Peak concurrent video stream connections for the Apps by day:
- Max number of concurrent streams per day from OneApp launch to present
- by app (iOS, Android, Roku, Xbox, OVP)
- by stream type (Linear vs VOD)

Steps to replicate
```
Rscript all.R # on local
sh run.sh # on local
hive -f query.hql # on gv
```
