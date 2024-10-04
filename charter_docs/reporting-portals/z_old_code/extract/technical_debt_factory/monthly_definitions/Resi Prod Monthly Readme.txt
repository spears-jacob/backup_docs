
# Resi Prod Monthly

The purpose of the readme file is to contextualize the Resi Prod Monthly, it's elements, and nuances worth noting. Resi Prod monthly is a monthly snapshot to convey the health and utilization of Charter's consumer web portal from an Accounts and Support perspective. Historically, the report has displayed varying counts and metrics. Recently (Fiscal September), ASP Team re-organized and chose to ALSO re-org the report. The following readme will focus on the prod monthly in its current format.

## Getting Started

Using Tableau Server, navigate to either "https://pi-datamart-tableau.corp.chartercom.com/#/projects/9" or "https://pi-datamart-tableau.corp.chartercom.com/#/projects/34" to review current and previous prod monthly reports.
From Gitlab, navigate to https://gitlab.spectrumxg.com/product-intelligence/reporting-portals/tree/master/extract/Prod%20Monthly%20Definitions to review definitions and context around the definitions and their historical significance, if any.

### Prerequisites

If you don't have gitlab credentials or a tableau license, reach out to a DASP team member to get either.

## Report Split Out By...
  - Feature
  - Metric
  - Portal
  - Count 2 months Ago
  - Count Prior Month
  - Count Current Month
  - MoM%
  - 3 mo avg
  - %diff vs 3 mo avg
### Feature

The report is split out by features as follows...
Overall Metrics - Highest summary of site activity via nbr of subscribers that authenticated and total count of visits.
Support - High lvl. count of clicks on "support"
Billing Transactions  - Summary of those that viewed statements online, paid their bill via the site, and/or set up Auto Payments.
Identity - We typically refer to these as Create, Reset Recover
Equip. Troubleshooting  - Summary of how customers use the site to restart modems and set top boxes / digital receivers.
Appointment Mgmt. - Summary of customer scheduling or canceling service appointments via the site.

### Metric

In order of how they are displayed on the report, the metrics are as follows:
 - Unique Households [Authenticated]
 - Web Sessions [visits]
 - Support Section Page Views
 - View Online Statements
 - One-Time Payments (OTP)
 - Set-up Auto-Payments (AP)
 - Total New IDs Created
 - Successful Password Resets
 - Total Successful ID Recovery
 - Modem Router Resets
 - Refresh Digital Receivers Requests
 - Reschedule Service Appointments
 - Cancelled Appointments

### Portal
The following are listed for the portals with the pseudo Hive table they are sourced from...
 - Spectrum.net (prod.asp_v_venona_events_portals_specnet w/small set of definitions from prod.asp_v_net_events)
 - TimeWarnerCable.com (prod.asp_v_twc_residential_global_events)
 - BrightHouse.com (prod.asp_v_bhn_residential_events & billing metrics from prod.asp_v_bhn_bill_pay_events)

### Count 2 Months Ago
 - Using current fiscal month of 2018-10, 2 months ago count will be for fiscal 2018-08.

### Count Prior Month
 - Using current fiscal month of 2018-10, prior month count will be for fiscal 2018-09.

### Count Current Month
 - Using current fiscal month of 2018-10, current month count will be for fiscal 2018-10.

### MoM%
 - (Current Month / Prior Month) -1

### 3 mo avg
  - Prior 3 months avg (Either calculated as...
   - SUM(This Month, Last Month, 2 months ago) Divided by 3 or....
   - SUM(Last Month, 2 months ago, 3 months ago) Divided by 3

### %diff vs 3 mo avg
 - (Current Month / 3 Mo Avg) -1

### Miscellaneous
 - % Success
  - New ID's, Password Resets, and ID Recovery have % success numbers derived from...
  - Total Successes / Total Attempts
 - MoM% Success Diff
  - This is the actual difference in percentage points of current month %success to prior month %success

    ```
    Example:
    Fiscal 2018-10 % Success Total New ID's: 0.30 (0.297351519)
    Fiscal 2018-09 % Success Total New ID's: 0.31 (0.311977141)
    MoM %Success Diff 0.297351519 - 0.311977141 = -0.0146256
    Displayed as: -1.5%
    ```

### Display Rules: Update with GKnasel notes
 **NOTE** MySpectrum Prod monthly houses the most updated rules for displaying numbers. Resi and SMB will follow suit in the new year.
Incomplete month of tagging will be replaced with NULL and report will display with either a "-" or a Letter with a note in the bottom right of the report.
If any month needs a metric to be blanked out, then the Total will ALSO be blanked out.
If the month blanked out is current or prior, then the MoM% will be blanked out.
If the month blanked out is current or prior, then the 3 Mo Avg will be blanked out.
If the month blanked out is current or prior, then the %diff vs 3 mo avg will be blanked out.
 
## Contributing

## Versioning

## Authors

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
