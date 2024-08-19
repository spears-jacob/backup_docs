# pass parameters

[Parameter(Mandatory = $true, Position = 0)]
        $user,
        [Parameter(Mandatory = $false, Position = 1)]
        $report_name

$user = '!svc.tableau'
$report_name='DocketsOverview_Overview'
$user_email="$user@e-470.com"
$output_path = "C:\Ops_Automation\tableau_exports\$report_name.png"
$email_body_path = "C:\Ops_Automation\tableau_exports\report_exports\email_templates\$report_name.html"
$report_map_path = "\\e-470.com\users\PHA\JSpears\code\projects\tabcmd_scripts\report_exports\report_map.csv"
$email_map_path = "\\e-470.com\users\PHA\JSpears\code\projects\tabcmd_scripts\report_exports\email_map.csv"

# Import the CSV report map variables
$report_map_array = Import-Csv -Path "$report_map_path"
$report_url = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty report_url
$report_title = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty report_title
$view_name = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty view_name
$export_name = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty export_name
$sender_email = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty sender_email
$to_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty to_list
$cc_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty cc_list
$bcc_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty bcc_list
$email_subject = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty email_subject

echo "

    **** Report variables:

        user is $user
        user_email is $user_email
        output_path is $output_path

    **** Report CSV variables:

        report_name is $report_name
        report_url is $report_url
        report_title is $report_title
        view_name is $view_name
        export_name is $export_name
        sender_email is $sender_email
        to_list is $to_list
        cc_list is $cc_list
        bcc_list is $bcc_list
        email_subject is $email_subject

"

#   Talk to Greg about validating tableau user password here...

$tab_cred=$password

echo "
        Running report export for Username: $user | Report: $report_name
"

echo "
        Checking that report file doesn't already exist and deleting if true.
"

if ( [System.IO.File]::Exists($output_path) -eq 'true' ) { 
    write-host("the file exists in: $output_path")
    remove-item $output_path
    write-host("removing the file")
}else { 
    write-host("there's no file with that name in $output_path")
}

#   Connect to Tableau with tabcmd in cmd prompt:

    # Logs into tabcmd with provided credentials
    # Exports the selected report file
    # Logs out of tabcmd

start-process cmd -Argument "/c C:\Ops_Automation\tableau_export.bat $user $tab_cred $export_name $output_path"

#   Email 

        sender_email is $sender_email
        to_list is $to_list
        cc_list is $cc_list
        bcc_list is $bcc_list

#   Attachment and email body processing

$attachment = Get-item -Path $outputpath
$email_body = Get-content -Path $email_body_path

send-E470Email -To $to_list -From $sender_email -Subject $email_subject -MessageBody $WarnMsg -Format HTML -Importance Normal -Attachment $attachment



