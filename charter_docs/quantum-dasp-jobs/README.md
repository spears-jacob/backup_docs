## The latest template versions are available from their gitlab releases.
This is how to understand what is happening in the ```dot_gitlab-ci.yml``` in this same
folder when the variables are updated.

- the **variable** ```COORDINATOR_TEMPLATE_REF``` and and the **include** ```ref:``` both refer to
https://gitlab.spectrumflow.net/awspilot/jobs-coordinator-template/-/releases



-------------------------------------

What is the purpose of this job?

What does it depend on? Tables? Other jobs? Time of day? If the job fails, what should be done?

What depends on it? Reports? Emails? People? Other jobs?

Who are the stakeholders?

What's a good way to QC it?

If you were giving someone a warning about this job, what would you say to them? What things went different from your original outline for this job, and why?

Relevant Tickets (JIRA link and one-line summary)

Annual Review: Are we still using it? (What would happen if this didn't run and/or ran with wrong data? Is this the best way to fulfill this purpose? Is it fragile or robust? Could it be used for other purposes? Does it have a README?
