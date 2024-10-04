What is the purpose of this job?
  Reads the file in the secure bucket (see pmai-devices job) and combines
  (via billing ID and account number) with core_quantum_events (for oneapp)
  and core_quantum_events_sspp (for myspectrum and spectrum mobile) so as to add
  device_id to the file.

What does it depend on?
  Jobs:
    outgoing/pmai-devices

What depends on it?
  Other jobs?
    pii-export/pmai-feed-export

Who are the stakeholders?
  Video Contact:
    "Sudolnik, Tanya" C-Tanya.Sudolnik@charter.com
  Devops contact:
    "Chism, Geimi R" Geimi.Chism@charter.com,
    and her supervisor: "Zimbelman, Gabe" Gabe.Zimbelman@charter.com

What's a good way to QC it?
  Check the output table (env_dasp.asp_pmai_sia_device_out) and make sure that each
  of the apps exists for the run_date

If you were giving someone a warning about this job, what would you say to them?
What things went different from your original outline for this job, and why?
  - The permission to read the file is specific to this job, so it has to be separated
  out from the rest of the PMAI process.
    - the permissions have to be specific and restrictive because the file
    contains clear text of pii, so special procedures had to be put in place to
    comply with CPNI
      Reads from secure bucket
      writes to our pii bucket

Relevant Tickets
  https://jira.charter.com/browse/XGANALYTIC-18878



2021 Review:
Are we still using it? (What would happen if this didn't run and/or ran with wrong data?
  yes
Is this the best way to fulfill this purpose?
  It's only been in production for a couple of weeks, but it is at the moment as elegant and efficient as we can make it
Is it fragile or robust?
  hard to say because the prior job keeps failing on things we can't control
Could it be used for other purposes?
  This piece is a pretty typical DASP job and contains no particularly interesting techniques
Does it have a README?
  yes!
