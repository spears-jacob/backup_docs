param([string]$fullFilePath)
<# Provide the fully qualified file path and file name for the file containing the encrypted credentials.
   Example:  powershell C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\tabcmd_password_encryption\Get_EncryptedPassword.ps1 -fullfilepath C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\tabcmd_password_encryption\encrypted_password.txt
 #>
$encrypted = Get-Content $fullFilePath | ConvertTo-SecureString
$credential = New-Object System.Management.Automation.PsCredential("seemsnot2matter", $encrypted)
$credential.GetNetworkCredential().Password