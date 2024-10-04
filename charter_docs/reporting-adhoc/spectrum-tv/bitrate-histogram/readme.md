JIRA:
- https://jira.charter.com/browse/XGANALYTIC-6108
- https://jira.charter.com/browse/XGANALYTIC-6318

Ask:
For the Android, Xbox, Roku, and Samsung TV, can you help pull a report from following dimensions against the distribution of bitrate? One time period (maybe peak on Sunday) is fine.
- Connection type
- ISP
- Application version
- Device version


Steps to replicate
```
hive -f query.hql # on gv and comment out grouping levels you don't need, copy paste the result into a xlsx named bitrate-histogram-apr
Rscript visualize.R # to create visualization attached in JIRA ticket
```
