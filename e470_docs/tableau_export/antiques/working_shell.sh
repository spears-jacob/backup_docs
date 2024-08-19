

tabcmd get '/views/Wow/SummaryReports.pdf' -f 'progressReport.pdf'



tabcmd export 'https://prtableau.e-470.com/#/views/ImageProcessingDaily/OriginalView.png' --png -f '\\e-470.com\users\PHA\jspears\tableau_export\'

tabcmd get 'views/ImageProcessingDaily/OriginalView.png' --filename 'C:\users\jspears\tableau_export\IPD.png'
tabcmd get 'views/DocketsOverview_test/Overview.png' --filename 'C:\users\jspears\tableau_export\dockets.png'

# https://prtableau.e-470.com/#/views/ImageProcessingDaily/OriginalView?:iid=1



tabcmd refreshextracts --workbook "Dockets Overview_test"






# JSON WRITER:

