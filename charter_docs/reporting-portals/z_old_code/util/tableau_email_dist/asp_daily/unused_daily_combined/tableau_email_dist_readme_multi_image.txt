Readme file for multi image tableau_email_dist jobs

SYNOPSIS:
The tableau_email_dist projects uses several variables to handle different portions
of downloading and sending a Tableau report in an E-mail message. Each of the
variables is found in the project.properties and in the output of the
tableau_send_email shell script.  Please consider using a distribution list (DL)
that stakeholders maintain for the recipients of E-mail messages and use it in the
BCC field of the E-mail to avoid Reply-All storms.  DLs can be requested through
the Service Now portal.

DESCRIPTION:
The two spots to review to understand what is going on and how to use them and review
code include the project.properties file and the output from tableau_send_email.sh script.
The project properties are set in order to provide information for the job to function.
Essentially, for two use cases, all the information needed to download a report and
send it either in debug mode to make sure it works or to the intended recipients is
performed by adjusting the IsDebuggingEnabled property in the project.properties
file.

=====================
project.properties:
---------------------
=-=-=-> IMPORTANT NOTE:
=-=-=-> Because everything in the project.properties file is passed into the shell script
=-=-=-> using input variables that are numbered, everything must have a value, even if
=-=-=-> that value is an empty string placeholder:    "''"


00. - failure.emails= 'dl-pi-platforms-notifications@charter.com'
Fill this out just like any other Azkaban job.  This is a comma delimited list addressing where the failure notification E-mail messages are sent.

01. - tableau_server="''"
The https address of the tableau server, such as "tableau.pi.spectrumtoolbox.com".

02. - tableau_workbook_no="599"
This is the workbook number on Tableau server.  This can be found by reviewing
the workbook and noting the number in the URL.
  For example: https://tableau.pi.spectrumtoolbox.com/#/workbooks/599/views

03. - tableau_output_filename='the_report_image.png'
The tableau_output_filename is the name of the file that will be saved for the report image.
(not used in multiple-image process)

04. - tableau_project="Monitoring"
The tableau_output_filename property specifies the project name on Tableau server.

05. - tableau_workbook="AzkabanYarnMonitoring"
The tableau_workbook property specifies the name of the workbook on Tableau server.

06. - tableau_views="DailyPlatformSummary,DailyExecutionTimeline,WeeklyPlatformSummary"
The tableau_views property specifies the comma-delimited list of names of the
views on Tableau server.  Each one will be included in the output E-mail in the
order specified in the property.

07. - tableau_metric_checking_view="MetricCheckingTotal"
The tableau_metric_checking_view property sets the view in the Tableau workbook
that is used to heck that workbook has been refreshed with data for yesterday.
This requires a view that shows the date, the number of metrics, and a
user-defined-in-Tableau threshold that sets the expectation to test against
for the Total number of metrics.

08. - tableau_width=800
The tableau_width property specifies the output width in pixels of the view.
(not apparently used in multiple-image process)

09. - tableau_height=1000
The tableau_height property specifies the output height in pixels of the view.
(not apparently used in multiple-image process)

10. - email_TO_list_comma="DL-PI-YourGroupDL@charter.com"
11. - email_CC_list_comma="''"
12. - email_BCC_list_comma="DL-PI-RecipientList@charter.com"
The above fields are the TO, CC (Courtesy Copy), and BCC (Blind Courtesy Copy) fields
for the production E-mail message.

13. - email_DEBUG_TO_list_comma="''"
14. - email_DEBUG_CC_list_comma="''"
15. - email_DEBUG_BCC_list_comma="''"
The above fields are the TO, CC (Courtesy Copy), and BCC (Blind Courtesy Copy) fields
for testing E-mail messages.  These fields are used when the IsDebuggingEnabled
property is set to one (1).

16. - email_FROM="PI.Tableau@charter.com"
17. - email_SUBJECT="Subject Here"
The above fields are the FROM and SUBJECT fields for E-mail messages.

18. - email_BOUNDARY="--B_10001110101_12152017133--"
The email_BOUNDARY separates different sections of the E-mail message.  This *must*
match what is in the email message template.

19. - email_NOTIFY="DL-PI-YourGroupDL@charter.com"
In cases where something unexpected occurs that would result in an incomplete or incorrect
E-mail being prepared, an E-mail is sent to the email_NOTIFY address indicating
that there was an issue.  This should likely be your group DL.

20. - email_TEMPLATE="multi_image_template.eml"
The email_TEMPLATE is a file that includes placeholders for several items such as
the TO, SUBJECT, and so forth to be replaced.

21. - email_POPULATED="Populated.eml"
The email_POPULATED is the output E-mail message, ready to send.

22. - email_FOLLOWUP_NOTE='For questions or comments, please reach out to DL-PI-YourGroupDL@charter.com'
The email_FOLLOWUP_NOTE is a note regarding who to contact with questions.

23. - email_BODY_IMG
This specifies the HTML that is used in the BODY section of the email that is
repeated for each view in the Tableau workbook.  It specifies the link to the
server as well as the CID placeholder that refers to the embedded png.

24. - report_table='environment.table_or_view_name'
25. - report_table_counted_field=value
26. - report_table_date_field=date_denver
27. - report_table_criterion1="Criteria Goes Here"
The four prior fields are used to query hive to ensure data is available and relevant
for use in the Tableau report.  This is a separate and distinct operation from
refreshing the Tableau data source and ensuring that the data in the report is also
satisfactory for publication. Please take a moment to ensure the hql in the shell
script will work based on the structure and data in the table or view that provides
data to Tableau.

28. - IsDebuggingEnabled=0
The IsDebuggingEnabled is the debugging flag.  When IsDebuggingEnabled=0, the production
TO, CC, and BCC fields are used.  When IsDebuggingEnabled=1, the DEBUG_TO, DEBUG_CC,
and DEBUG_BCC fields are used.  This way, the job can be debugged using a single
change in flow parameters.


----- tableau_send_email.sh
tableau_send_email is a shell script which does the following.
  0. checks that there is current data in both hive and refreshed in Tableau
  1. downloads a png image of a Tableau report view
  2. encodes it into a long string of characters (base64)
  3. chops up the long string into manageable bites
  4. places it into a predefined E-mail message template
  5. replaces some E-mail headers
  6. adds some requisite characters and boundaries to make the E-mail work
  7. sends the E-mail

Please keep in mind that even if an E-mail list is blank, it must include "''" as the value,
otherwise the input variables do not properly match the order in which they are passed.

The format of the E-mail template is entirely customizable, but needs to resemble
the multi_image_template.eml file if using the tableau_send_email shell script.  The important
bits to have include the replacement placeholders, which appear like the following.
  ∞∞∞TO∞∞∞   --==-- replaced with ==-->  email_TO_list_comma
