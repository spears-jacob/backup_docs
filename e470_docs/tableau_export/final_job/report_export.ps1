#   Import Modules
#   Check to see if required graphAPI modules are installed -- add failure if they are not present

#   Usage: powershell_script report_name
#       ex: C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\report_export.ps1 ImageProcessingDaily_OriginalView

if (Get-Module -ListAvailable -Name Microsoft.Graph.Mail) {
    Write-Host "Microsoft.Graph.Mail Module exists"
}else{
    Write-Host "Microsoft.Graph.Mail Module does not exist. Run the following script to install: 
    
        > Install-Module Microsoft.Graph.Mail"
}

if (Get-Module -ListAvailable -Name Microsoft.Graph.Authentication) {
    Write-Host "Microsoft.Graph.Authentication Module exists"
}else{
    Write-Host "Microsoft.Graph.Authentication Module does not exist. Run the following script to install: 
    
        > Install-Module Microsoft.Graph.Authentication"
}

if (Get-Module -ListAvailable -Name Microsoft.Graph.Users.Actions) {
    Write-Host "Microsoft.Graph.Users.Actions Module exists"
}else{
    Write-Host "Microsoft.Graph.Users.Actions Module does not exist. Run the following script to install: 
    
        > Install-Module Microsoft.Graph.Users"
}

Import-Module Microsoft.Graph.Mail

#   Pass parameters

#$report_name = 'ImageProcessingDaily_OriginalView'
$report_name = $args[0]
$user = "!svc.tableau"
$output_path = "C:\Ops_Automation\tableau_export\$report_name.png"
$report_map_path = "C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\report_map.csv"
$gen_date = Get-Date

#   Import the CSV report map variables
$report_map_array = Import-Csv -Path $report_map_path
$report_url = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty report_url
$report_title = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty report_title
$view_name = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty view_name
$export_name = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty export_name
$sender_email = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty sender_email
$to_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty to_list
$cc_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty cc_list
$bcc_list = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty bcc_list
$email_subject = $report_map_array | where { $_.report_name -eq "$report_name" } | select -expandproperty email_subject

write-host("

    **** Report variables:

        user is: $user
        output_path is: $output_path
        report_name is: $report_name
        report_url is: $report_url
        report_title is: $report_title
        view_name is: $view_name
        export_name is: $export_name
        sender_email is: $sender_email
        to_list is: $to_list
        cc_list is: $cc_list
        bcc_list is: $bcc_list
        email_subject is: $email_subject
        gen_date is: $gen_date

")

write-host("
        Running report export for Username: $user | Report: $report_name
")

write-host("
        Checking that report image file doesn't already exist and deleting if true.
")

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

$encrypted = Get-content -Path 'C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\tabcmd_password_encryption\encrypted_password.txt' | ConvertTo-SecureString
$credential = New-Object System.Management.Automation.PsCredential("seemsnot2matter", $encrypted)
$password = $credential.GetNetworkCredential().Password 

tabcmd login -s https://prtableau.e-470.com -u $user -p $password

tabcmd get "$export_name.png" --filename "$output_path"

tabcmd logout

#   Email 

write-host ("
        sender_email is $sender_email
        to_list is $to_list
        cc_list is $cc_list
        bcc_list is $bcc_list
")

#   Attachment and email body processing variables

$To = $to_list.Split(";")
$CC = $cc_list.Split(";")
$BCC = $bcc_list.Split(";")
$From = 'noreply@e-470.com'
$Subject = $email_subject
$Format = "HTML"
$Importance = "Normal"
$Attachment = Get-item -Path $output_path

#   Build HTML string

$MessageBody = "
</head>
<body><div class=WordSection1>
<p class=MsoNormal><b><span style='font-size:16.0pt'>$email_subject<o:p></o:p></span></b></p>
<b></b>
<p>
<img src='cid:$report_name.png'>
</p>
<b></b>
<p>
<a href='$report_url'>Go to the Tableau Report</a>
        <o:p></o:p>
</p>
<b></b>
<p class=MsoNormal>You're receiving this email because you are subscribed to the $view_name page of the $report_title. The image above was generated $gen_date</p>
</div>
</body>
</html>
"

$To
$CC
$BCC
$From
$Subject
$MessageBody
$Format
$Importance
$Attachment

#   Email functions construction --probably move this to a centralized location for repeatable "global" functions in the future--

Function ConvertTo-IMicrosoftGraphRecipient
{
    [cmdletbinding()]
    Param(
        [array]$smtpAddresses        
    )
    foreach ($address in $smtpAddresses)
    {
        @{
            emailAddress = @{address = $address }
        }    
    }    
}

function Send-E470Email 
{
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        $To,
        [Parameter(Mandatory = $false, Position = 5)]
        $CC,
        [Parameter(Mandatory = $false, Position = 6)]
        $BCC,
        [Parameter(Mandatory = $true, Position = 1)]
        [ValidateSet(“noreply@e-470.com", ”HelpMe@e-470.com”)]
        $From,
        [Parameter(Mandatory = $true, Position = 2)]
        $Subject,
        [Parameter(Mandatory = $true, Position = 3)]
        $MessageBody,
        [Parameter(Mandatory = $true, Position = 4)]
        [ValidateSet(“HTML”, ”Text”)]
        $Format = 'Text',
        [Parameter(Mandatory = $false, Position = 7)]
        [ValidateSet(“Normal”, ”High”)]
        $Importance = "Normal",
        [Parameter(Mandatory = $false, Position = 8)]  # An array of files can be passed into this but not a string of files.  The input format needs to be of type System.IO.FileSystemInfo.  Run $file.gettype() to validate correct input type.  Populating the varialbe with Get-Item will result in correct formatting.
        $Attachment
    )
    
    process 
    {
        # Process list of TO recipients
        [array]$toRecipients = ConvertTo-IMicrosoftGraphRecipient -SmtpAddresses $To
        [array]$CCRecipients = ConvertTo-IMicrosoftGraphRecipient -SmtpAddresses $CC
        [array]$BCCRecipients = ConvertTo-IMicrosoftGraphRecipient -SmtpAddresses $BCC

        # Connection information for MSGraph API
        # How to update Secure String
        # ConvertTo-SecureString -String 'ClientSecretValue' -AsPlainText -Force | ConvertFrom-SecureString
        # Replace The output of this command in the declaration of the varialbe $SecureString

        $ClientSecret = 'rTx8Q~X3eM-6gu-vkjnDleimLVxCeytcxhPruavH'
        $ClientID = 'f04c0c43-7c85-4478-a7b6-b315fd5c00ea'
        $TenantID = '0dec83f0-8d0a-4036-bdaa-08866d76cf19'
        $SecureString = '01000000d08c9ddf0115d1118c7a00c04fc297eb010000009d7b8ffd8bb8924dbee2fd51a162d0a50000000002000000000003660000c000000010000000b6989e4eaf2cabe98f630ab09033dbd80000000004800000a000000010000000faa202bc6f6df5cb9f1710eea3c2de0f58000000de5028a6676c0a50a9508680cc207abe923fb8397957d3b2015b10940b4caa583581c52b3b8b3c4bee6c98b7ab49d90b0377b2b846efe5caa9d2e55e241418dd55478ec449146417ef20ee6eb6c16a2de2d5f6b0ea8595971400000087acc300dca7476028801e68054f39cfc6c9096a'
        $ClientSecretPass = ConvertTo-SecureString -String $ClientSecret -AsPlainText -Force
        $ClientSecretCredential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $ClientId, $ClientSecretPass

        # Connect to Graph
        Connect-MgGraph -TenantId $tenantId -ClientSecretCredential $ClientSecretCredential

        # Process Attachments
        # If over ~3MB: https://docs.microsoft.com/en-us/graph/outlook-large-attachments?tabs=http
        
        $attachment

        if ($attachment -ne $null)
        {
            $AttachmentPath = $Attachment.FullName
            if ((Get-ItemProperty -Path $AttachmentPath).Length -gt 3072000) { Write-Warning "This script does not currently support file sizes greater than 3MB.  The script will now end.";break}
            $EncodedAttachment = [convert]::ToBase64String((Get-Content $AttachmentPath -AsByteStream -Raw)) 
        }           
        # Build the message
        Import-Module Microsoft.Graph.Users.Actions
        
        $params = @{
            Message         = @{
                Subject       = "$Subject"
                Importance    = "$Importance"
                Body          = @{
                    ContentType = "$Format"
                    Content     = $MessageBody
                }
                ToRecipients  = $toRecipients
                CcRecipients  = $CCRecipients
                BccRecipients = $BCCRecipients
                attachments   = @(
                    @{
                        "@odata.type" = "#microsoft.graph.fileAttachment"
                        name          = ($AttachmentPath -split '\\')[-1]
                        contentType   = "text/plain"
                        contentBytes  = $EncodedAttachment
                    }
                )
            }
            SaveToSentItems = "true"
        }

        # Send the email
        Send-MgUserMail -UserId $From -BodyParameter $params
        Send-EmailToSecurityDatabase -ToEmail $To -FromEmail $From -CCEmail $CC -BCCEmail $BCC -Subject $Subject -MessageBody $MessageBody
        Disconnect-MgGraph
    }
}

function Send-EmailToSecurityDatabase
{
    param (
       
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        $ToEmail,
        [Parameter(Mandatory = $false,
            ValueFromPipelineByPropertyName = $true,
            Position = 5)]
        $CCEmail = 'Empty',
        [Parameter(Mandatory = $false,
            ValueFromPipelineByPropertyName = $true,
            Position = 5)]
        $BCCEmail = 'Empty',
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 1)]
        $FromEmail,
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 2)]
        $Subject,
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 3)]
        $MessageBody
    )
   
    $MessageBody = $MessageBody.replace("'", "").Replace('"', '')    # Strips all quotes from HTML code to prevent errors in SQL query
    $ConsoleUser = ((get-childitem env:\username).value)
    $TimeStamp = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    $DBSplatread = @{
        'ServerInstance' = "InternalSQL\Pwrshell";
        'Query'          = "    USE PowerShellLogs
        GO
 
        INSERT INTO [dbo].[EmailLogs]
            ([Date]
            ,[ToEmail]
            ,[CCEmail]
            ,[BCCEmail]
            ,[FromEmail]
            ,[Subject]
            ,[MessageBody]
            ,[ExecutedBy])
        VALUES
            ('$TimeStamp',
            '$ToEmail',
            '$CCEmail',
            '$BCCEmail',
            '$FromEmail',
            '$Subject',
            '$MessageBody',
            '$ConsoleUser')
            GO"
    }
 
    try { Invoke-Sqlcmd @DBSplatread -TrustServerCertificate }
    catch { $Errors++ }
}

Send-E470Email -To $To -From $From -Subject $Subject -MessageBody $MessageBody -Format $Format -Importance $Importance -Attachment $Attachment -CC $CC -BCC $BCC

#   Remove temporary items in advance of next run

remove-item $output_path