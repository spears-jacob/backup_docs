# looking for:
#  extract_datetime	spc_div_id	region	configuration_name	ma	work_zone	hub	node	zip	account
#  "Notification Source","Date","Billing Vendor","Template Name",

epid
tokenidin ('Maintenance_Order_Initial_Attempt','Proact_Maintenance_Intl_Resi')

//ucc-hub is noted as the 'header-payload' tables
//Vasanth Rajendran is the go-to source guy for the HP side of the data source (UCC-Hub).//
//HP data startdatetime timestamp is also in UTC//

//Skycreek data is noted as equipment-post-id data.

Ian Greenwald •2022-10-31

To summarize:
	•	It appears that we can just use the EPID data source and call it good. The Proactive stuff (PSA) from HP sourced by UCC-Hub is not related to proactive maintenance according to Khaisir in #3 above

POCs:
	•	Khaisir Chinnaraju and Vasanth Rajendran are good for the HP / UCC-Hub data.
	•	Shmuel Israeli forwarded me to Sean McDonald for SkyCreek data, however Khaisir seems to be a decent source as well.

If we only use EPID, we can do a test run against the files I have received just to make sure everyone is accounted for in notifications. We'll need a greater date range (30 days?) of EPID data so that I can make sure we are covered.

Note: for 10/23, there were only 15 records that seemed to be in the HP file and not in the EPID file.

Webex Teams 2022-10-20

Lonnie Byxbe 2:29 PM
You could call it all notifications that could become a push notification in MSA. UCC is only handling email,
text, etc. So we receive all the messages.

Lonnie Byxbe 2:33 PM
We are looking for SROPROMTV9 and CLIFIX reason codes
Ian Greenwald You 2:38 PM
Ok, so this topic receives all messages that could become a notification whether they do or not. Is it MSA
only or does it include messages we send via email/text too?

Lonnie Byxbe 2:43 PM
Good point of clarification.
This topic contains all messages (including those that were sent by email, etc.).
As designed, all messages are consumed from Kafka and then checked against the subscription service
to determine push notification status for that message. So push notifications to MSA are up to SSPP to
determine.

Lonnie Byxbe 2:47 PM
UCC handles the email, text, phone, etc. Generally under the IT umbrella.
SSPP / DP consumes the messages UCC sent (via 3rd party  Pinpoint, SkyCreek, etc.) from Kafka.

We then check to see if that message should trigger an action on the SSPP side, such as sending a push
notification, and/or storing the message for a CTA upon login.

