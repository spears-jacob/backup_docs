What is the purpose of this job?
  Queries the table from outgoing/pmai-devices-get-output job, and writes
  it to our secure bucket.
  Copies that file to the bucket for Video Delivery

What does it depend on?
  Other jobs?
    outgoing/pmai-devices-get-output
  Permissions to write to
    s3://com.charter.focus.prod.ingest.files/quantum/

What depends on it?
  Just the stakeholders

Who are the stakeholders?
  Video Contact:
    "Sudolnik, Tanya" C-Tanya.Sudolnik@charter.com
  Devops contact:
    "Chism, Geimi R" Geimi.Chism@charter.com,
    and her supervisor: "Zimbelman, Gabe" Gabe.Zimbelman@charter.com

What's a good way to QC it?
  If the file is in their bucket, it worked.  If it is not, it did not.
  Stakeholders notify us of problems in the file

If you were giving someone a warning about this job, what would you say to them?
What things went different from your original outline for this job, and why?
  - The permission to read the file is specific to this job, so it has to be separated
  out from the rest of the PMAI process.
    - the permissions have to be specific and restrictive because the file
    contains clear text of pii, so special procedures had to be put in place so
    as to comply with CPNI

Relevant Tickets
  https://jira.charter.com/browse/XGANALYTIC-18878
  https://jira.charter.com/browse/XGANALYTIC-27446



2021 Review:
Are we still using it? (What would happen if this didn't run and/or ran with wrong data?
  yes
Is this the best way to fulfill this purpose?
  It's only been in production for a couple of weeks, but it is at the moment as elegant and efficient as we can make it
Is it fragile or robust?
  Currently we can tell from EMR logs if it didn't work.
  Would be more robust if it programmatically failed the job if the file was not in place
Could it be used for other purposes?
  It provides us a good model/template if we should need to read/write to another team's bucket in the future
Does it have a README?
  yes!
