2022-09-09
Jacob Spears

This job is split into two parts. The first prepares a daily table population of the survey intercept data for Medallia. PII is encrypted for the data that exists in the table. The second part of this job outputs the daily pull, decrypts the PII, and exports it to a secure S3 Bucket.

The primary contacts for Medallia are:
Michael O'Neill, Jason M Delgado, Alyson Gibson


Data Dictionary for output file:
<field>             <definition>
application_name    This is where the activation took place
acct_number         Customer  account number
survey_action       Accept or decline survey	“formSubmit” or “inviteDeclined”
biller_type         What biller is the customer in
division            Spectrum division/state customer is in
division_id         Spectrum ID customer is in
sys           	    4 digit value that captures the system within the legacy biller for the account
prin            	  4 digit value that captures the principal within the legacy biller for the account
agent           	  4 digit value that captures the agent within the legacy biller for the account
acct_site_id       	ICOMS site identifier for the account
account_company    	ICOMS Company associated to the account
acct_franchise     	The Franchise the account is in
day_diff           	delta between current day of report and when survey action was taken by customer
rpt_dt            	When report was pulled
partition_date_utc 	When activation was completed
