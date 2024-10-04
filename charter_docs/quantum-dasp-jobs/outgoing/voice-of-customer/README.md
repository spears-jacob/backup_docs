XGANALYTIC-36454, and the broader epic XGANALYTIC-32369

Four outgoing feeds that contain cleartext, each saved in the secure s3
pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/voice_of_customer/cable/
pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/voice_of_customer/mobile/
pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/voice_of_customer/troubleshooting/cable
pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/voice_of_customer/troubleshooting/mobile

Read todays output:
```
aws s3 ls --recursive s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/voice_of_customer/ | grep $(date +%Y%m%d)
```

Voice of Customer contacts:
https://jira.charter.com/browse/XGANALYTIC-32369

-          Project SME’s: Simer Singh and Kyle Finnegan
-          Technical Contact: Santhoshi Vytla
-          DL: DL-Corp-IT-ITSD-ORDL-DevOps@charter.com

The two primary outputs are troubleshooting/cable and troubleshooting/mobile. The other two were requested but may not ultimately be used.
