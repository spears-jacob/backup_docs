#!/bin/bash

# explains usage of script
usage ()
{
    echo ""
    echo "***************lftp.sh***************"
    echo "-------------------------------------"
    echo ""
    echo "Usage: lftp.sh curl_endpoint filemask"
    echo ""
    echo "Arguments:"
    echo "curl_endpoint = value must be a correct value provided by complimentary node.js program"
    echo "filemask = (optional) the filemask (wildcards included)"
    echo ""
    echo "Example: ./lftp.sh zodiac"
    echo ""
    echo "-------------------------------------"
    echo ""
    echo "Important details about parameters provided from Node.js"
    echo ""
    echo "***Mode***"
    echo "normal= Transfer file. No deletion."
    echo "delete= Delete the file after transfer."
    echo "archive= Delete the file after transfer. Place back on source server in new directory."
    echo "delete-done= Delete double processes files (.done.done). Delete the file after transfer. Place back on server in original directory with new suffix."
    echo "put= Transfer local file to destination server."
}

# test if we at least have one argument on the command line
if (( $# < 1 ))
then
    usage
    echo ""
    echo "ERROR: Illegal number of parameters. This script takes at least 1 parameter, the curl endpoint value must be provided to the script. The filemask is optional"
    echo "-------------------------------------"
    echo "Exiting script..."
    echo ""
    exit
fi

ftp_config=$1
echo ""
echo "Attempting to grab parameters for $ftp_config..."
echo "-------------------------------------"
connection_json=$(cat ./$ftp_config.json)
echo ""

if [ -z "$connection_json" ]
then
    usage
    echo ""
    echo "ERROR: curl payload is empty. Please check curl endpoint configurations are correct"
    echo "-------------------------------------"
    echo "Exiting script..."
    echo ""
    exit
fi

if [ -n "$2" ]
then
    filemask=$2
    echo "filemask was passed to script ($filemask). We will use this value to override what is passed in the JSON configs."
else
    filemask=$(echo "$connection_json" | grep -Po '"filemask":.*?[^\\]",' | perl -pe 's/"filemask"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
fi

# Sample JSON object
# {
#     "host":"files.website.com",
#     "username":"bla",
#     "password":"foo",
#     "ftp_directory":"/process/",
#     "filemask":".csv",
#     "filemask2":".csv.done.done",
#     "local_directory":"/home/etladmin/simon/tmp/",
#     "ftp_delete_files":"true",
#     "display":"Zodiac",
#     "curl":"curl -k --get http://website:3000/startprocess",
#     "protocol":"sftp",
#     "processed_filemask":".done",
#     "mode":"normal",
#     "ftp_archive_directory":"/tmp/bkp/",
#     "removal_filemask":".done.done"
# }

# For debug
#
# username="bla"
# password="foo"
# host="ftp.website.com"
# ftp_directory="/files"
# filemask="*csv"
# local_directory="/home/simon/Sandbox/tmp"
# ftp_archive_directory="/files/archive"
# mode="archive"
# protocol="ftp"
# processed_filemask=".done"
# removal_filemask=".done.done"

# test if we got a payload.

username=$(echo "$connection_json" | grep -Po '"username":.*?[^\\]",' | perl -pe 's/"username"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
password=$(echo "$connection_json" | grep -Po '"password":.*?[^\\]",' | perl -pe 's/"password"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
host=$(echo "$connection_json" | grep -Po '"host":.*?[^\\]",' | perl -pe 's/"host"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
ftp_directory=$(echo "$connection_json" | grep -Po '"ftp_directory":.*?[^\\]",' | perl -pe 's/"ftp_directory"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
local_directory=$(echo "$connection_json" | grep -Po '"local_directory":.*?[^\\]",' | perl -pe 's/"local_directory"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
ftp_archive_directory=$(echo "$connection_json" | grep -Po '"ftp_archive_directory":.*?[^\\]",' | perl -pe 's/"ftp_archive_directory"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
mode=$(echo "$connection_json" | grep -Po '"mode":.*?[^\\]",' | perl -pe 's/"mode"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
protocol=$(echo "$connection_json" | grep -Po '"protocol":.*?[^\\]",' | perl -pe 's/"protocol"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
processed_filemask=$(echo "$connection_json" | grep -Po '"processed_filemask":.*?[^\\]",' | perl -pe 's/"processed_filemask"://; s/^*"//; s/",$//' | perl -pe 's/^\s+|\s+$//g')
removal_filemask=$(echo "$connection_json" | grep -Po '"removal_filemask"\s*:\s*"([^"]*)"' | perl -pe 's/"removal_filemask"://; s/^*"//; s/"$//' | perl -pe 's/^\s+|\s+$//g')

# test if we have host. Bad way to test the curl worked but some sort of failsafe
if [ -z "$host" ]
then
    usage
    echo ""
    echo "ERROR: Host value not set. Please check curl endpoint configurations are correct"
    echo "-------------------------------------"
    echo "Exiting script..."
    echo ""
    exit
fi

echo ""
echo "*******Parameters for this run********"
echo "--------------------------------------"
echo ""
echo "username: $username"
#echo "password: $password"
echo "host: $host"
echo "mode: $mode"
echo "protocol: $protocol"
echo "source ftp/sftp directory: $ftp_directory"
echo "local directory: $local_directory"
echo "source ftp/sftp archive directory: $ftp_archive_directory"
echo "filemask: $filemask"
echo "processed_filemask: $processed_filemask"
echo "removal_filemask: $removal_filemask"
# echoes out lftp global settings for user
echo ""
echo "********lftp global settings*********"
echo "-------------------------------------"
cat ~/.lftp/rc
echo ""
echo "-------------------------------------"
echo ""


echo ""
echo "*******Starting file transfer********"
echo "-------------------------------------"
echo ""

connection_string="$protocol://$username:$password@$host"
#echo "Connection String: $connection_string"
echo "changing directory locally to $local_directory"
cd "$local_directory" || exit

if [ "$mode" == "delete" ]
then
    echo "Connecting to $protocol $host as $username in delete mode."
    lftp "$connection_string" << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l *$filemask
    echo "Getting all *$filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory *$filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
elif [ "$mode" == "archive" ]
then
    echo "Connecting to $protocol $host as $username in archive mode."
    lftp "$connection_string" << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l *$filemask
    echo "Getting all *$filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory *$filemask
    echo "Transfer complete. Archiving files on $host under $ftp_archive_directory..."
    echo "Changing directory to $ftp_archive_directory..."
    cd "$ftp_archive_directory"
    echo "Putting files at $ftp_archive_directory"
    mput *$filemask
    ls -l *$filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
elif [ "$mode" == "put" ]
then
    echo "Listing local files to transfer..."
    ls -l $local_directory/*$filemask
    echo "Connecting to $protocol $host as $username in put mode."
    lftp "$connection_string" << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Getting all local *$filemask files. Placing them remotely at $local_directory"
    mput $local_directory/*$filemask
    echo "Transfer complete. Listing files on $host under $ftp_archive_directory..."
    ls -l *$filemask
    bye
EOF
elif [ "$mode" == "delete-done" ]
then
    echo "Connecting to $protocol $host as $username in delete and mark done mode."
    lftp "$connection_string" << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "List *$removal_filemask files"
    ls -l *$removal_filemask
    mrm *$removal_filemask
    echo "Removing $removal_filemask files..."
    echo "Listing files to transfer..."
    ls -l *$filemask
    echo "Getting all *$filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory *$filemask
    bye
EOF
    for i in *"$filemask";do cp "$i" "$i""$processed_filemask"; done
    echo "Connecting to $protocol $host as $username in delete and mark done mode."
    lftp "$connection_string" << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Transfer complete. Placing processed files on $host under $ftp_directory with $processed_filemask suffix..."
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Putting $processed_filemask files at $ftp_directory"
    mput *$processed_filemask
    ls -l *$processed_filemask
    echo "local file cleanup..."
    bye
EOF
    rm "$local_directory"/*"$processed_filemask"
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
else
    echo "Connecting to $protocol $host as $username in normal (non-delete) mode."
    lftp "$connection_string" << EOF

    echo "doing pwd, ls"
    pwd
    ls
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l *$filemask
    echo "Getting all *$filemask files. Placing them locally at $local_directory"
    mget -O $local_directory *$filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
fi
echo ""
echo "*******File transfer complete********"
echo "-------------------------------------"
echo ""
