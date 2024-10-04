The report that draws off of this (Legacy Calls With Visits Dashboard, https://pi-datamart-west-tableau.corp.chartercom.com/#/workbooks/2677/views)
is still on Tableau Server. However, there is no new data flowing in.  Since it's reached the end
of its data stream, there's no benefit to using cluster resources to have it search for new data that will never come.
Therefore the Azkaban job has been unscheduled, the extract refresh has been stopped, and this code has been archived.
