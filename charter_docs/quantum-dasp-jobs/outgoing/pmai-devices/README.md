What is the purpose of this job?
  Copies the Precise Mobile Ad Insertion (PMAI) file from Video Delivery's bucket
  to the DASP secure bucket

What does it depend on?
  Permissions to read from the bucket
    s3://com.charter.focus.prod.export.files/quantum-list-match/
  Time of day?
    The Video Delivery job that makes this file runs on Fridays.

What depends on it?
  Other jobs?
    outgoing/pmai-devices-get-output
    pii-export/pmai-feed-export

Who are the stakeholders?
  Video Contact:
    "Sudolnik, Tanya" C-Tanya.Sudolnik@charter.com
  Devops contact:
    "Chism, Geimi R" Geimi.Chism@charter.com,
    and her supervisor: "Zimbelman, Gabe" Gabe.Zimbelman@charter.com

What's a good way to QC it?
  If this week's file is in s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/
    then it worked.
    If it is not, then it did not.
  The last line of the execute_hql.sh script gives an ls of the relevant directory
    so that you can check whether it worked

If you were giving someone a warning about this job, what would you say to them?
What things went different from your original outline for this job, and why?
  - The permission to read the file is specific to this job, so it has to be separated
  out from the rest of the PMAI process. 
    - the permissions have to be specific and restrictive because the file
    contains clear text of pii, so special procedures had to be put in place to
    comply with CPNI

Relevant Tickets
  https://jira.charter.com/browse/XGANALYTIC-18878



2021 Review:
Are we still using it? (What would happen if this didn't run and/or ran with wrong data?
  yes
Is this the best way to fulfill this purpose?
  It's only been in production for a couple of weeks, but it is at the moment as elegant and efficient as we can make it
Is it fragile or robust?
  Broke once because of encrypt/decrypt function in production
  Broke once because lost permissions for bucket to read from
Could it be used for other purposes?
  It provides us a good model/template if we should need to read/write to another team's bucket in the future
Does it have a README?
  yes!
