2022-08-01 - Mario Williams

What is the purpose of this job?
To pull data from CQE to build metrics that feed a tableau report for the AHW (Advance Home Wifi) team

What does it depend on? Tables? Other jobs? Time of day?
It depoends on the core_quantum_events_sspp table

What depends on it? Reports? Emails? People? Other jobs?
The MSA Customer Experience Report depends on this data (see link below)
The AHW team depends on this data

Who are the stakeholders?
Julian Laatsch
Pim Khunrattanaphon

What's a good way to QC it?
Checking drastic changes in charts in the tableau report

If you were giving someone a warning about this job, what would you say to them? What things went different from your original outline for this job, and why?
This is a transfer from the mobile insights team to the insights team. See link to their repo and location of files that we're taking over below
https://gitlab.spectrumflow.net/awspilot/wifi-jobs
https://gitlab.spectrumflow.net/awspilot/wifi-jobs/-/tree/stg/scp/scp-portals-daily

Relevant Tickets
Handoff ticket https://jira.charter.com/browse/XGANALYTIC-37996
Epic https://jira.charter.com/browse/XGANALYTIC-34463


The tableau workbook 'MSA Customer Experience' is refreshed after the job is done:
https://tableau.pi.spectrumtoolbox.com/#/workbooks/6144/views

