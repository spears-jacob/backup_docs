param([string]$fullFilePath)
<# Provide the fully qualified file path and file name for the file containing the encrypted credentials
   Example:  powershell C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\tabcmd_password_encryption\Set_EncryptedPassword.ps1 C:\Ops_Automation\tableau_export\tab_cmd_scripts\src\tabcmd_password_encryption\encrypted_password.txt
 #>
<# Set and encrypt credentials to file using default method #>
<# From https://www.interworks.com/blogs/trhymer/2013/07/08/powershell-how-encrypt-and-store-credentials-securely-use-automation-script #>
$credential = Get-Credential
$credential.Password | ConvertFrom-SecureString | Out-file $fullFilePath