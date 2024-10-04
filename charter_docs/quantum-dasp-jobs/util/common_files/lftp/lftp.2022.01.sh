#!/bin/bash

# requires lftp as well as jq to be installed for json parsing, which is available in AWS EC2/EMRs and on-prem
# For Mac Laptops:
#       first install xcode:  xcode-select --install
#       then homebrew:        /usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)"
#       then jq:              brew install jq
#       then lftp:            brew install lftp
# For AWS EMR/EC2, in the startup script:
#       sudo -s yum -y install lftp


# explains usage of script
usage ()
{
    echo -e "
    ***************lftp.sh***************
    -------------------------------------

    Usage: lftp.sh config_json <filemask>

    Arguments:
    conf_json = file containing configuration in json format
    filemask = (optional) the filemask (wildcards included)

    Example: ./lftp.sh zodiac

    -------------------------------------

    *** Mode ***
    normal      = Transfer file. No deletion.
    delete      = Delete the file after transfer.
    archive     = Delete the file after transfer. Place back on source server in new directory.
    delete-done = Delete double processes files (.done.done). Delete the file after transfer. Place back on server in original directory with new suffix.
    put         = Transfer local file to destination server.

    "
}

# Ensures that there is exactly one non-null and non empty string input from the script execution
if [ $# -ne 1 ] || [ -z "$1" ] || [ ! "$1" ] ; then usage; kill -INT $$; fi

# Ensures that the input json FILE exists and is a regular file, is readable, and has a nonzero size
if [ ! -f "$1" ] || [ ! -r "$1" ] || [ ! -s "$1" ] ; then usage; echo -e "\n\n\tPlease check config_json\n\n"; kill -INT $$; fi

# Ensures that jq is available in the current environment
command -v jq  > /dev/null 2>&1
if  [ $? -ne 0 ]  ; then usage; echo -e "\n\n\tPlease install jq\n\n"; kill -INT $$; fi

# Ensures that lftp is available in the current environment
command -v lftp  > /dev/null 2>&1
if  [ $? -ne 0 ]  ; then usage; echo -e "\n\n\tPlease install lftp\n\n"; kill -INT $$; fi


ftp_config_json=$1

echo -e "
<-- lftp started -->

Attempting to grab parameters from $ftp_config_json...
-------------------------------------

"
host=$(jq -r '"\(.host)"' $ftp_config_json)
username=$(jq -r '"\(.username)"' $ftp_config_json)
password=$(jq -r '"\(.password)"' $ftp_config_json)
filemask=$(jq -r '"\(.filemask)"' $ftp_config_json)
local_directory=$(jq -r '"\(.local_directory)"' $ftp_config_json)
ftp_directory=$(jq -r '"\(.ftp_directory)"' $ftp_config_json)
ftp_archive_directory=$(jq -r '"\(.ftp_archive_directory)"' $ftp_config_json)
mode=$(jq -r '"\(.mode)"' $ftp_config_json)
protocol=$(jq -r '"\(.protocol)"' $ftp_config_json)
processed_filemask=$(jq -r '"\(.processed_filemask)"' $ftp_config_json)
removal_filemask=$(jq -r '"\(.removal_filemask)"' $ftp_config_json)

#Now, check that all the following fields are NOT null
fields_array=(host username password filemask local_directory ftp_directory protocol mode)
for field in "${fields_array[@]}"
do
  if [ -z "${!field}" ]
  then
      usage
      echo -e "
      ERROR: config_json payload is empty for the ${field} field. Please check config_json configurations are correct
      -------------------------------------
      Exiting script..."

      kill -INT $$;
  fi
done

#creating file for credentials to be temporarily stored, but never in the logs: ~/.netrc
echo "machine $host
login $username
password $password" > ~/.netrc

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
#     "protocol":"sftp",
#     "processed_filemask":".done",
#     "mode":"normal",
#     "ftp_archive_directory":"/tmp/bkp/",
#     "removal_filemask":".done.done"
# }

# For debug
#
# host="ftp.website.com"
# username="bla"
# password="foo"
# ftp_directory="/files"
# filemask="*csv"
# local_directory="/home/simon/Sandbox/tmp"
# ftp_archive_directory="/files/archive"
# mode="archive"
# protocol="ftp"
# processed_filemask=".done"
# removal_filemask=".done.done"

# test if we have host. Bad way to test the config_json worked but some sort of failsafe
if [ -z "$host" ]
then
    usage
    echo ""
    echo "ERROR: Host value not set. Please check config_json configurations are correct"
    echo "-------------------------------------"
    echo "Exiting script..."
    echo ""
    kill -INT $$;
fi

# ensure hosts are added to known_hosts
ssh-keyscan -H $host >> ~/.ssh/known_hosts

echo ""
echo "*******Parameters for this run********"
echo "--------------------------------------"
echo ""
echo "username: $username"
echo "host: $host"
echo "mode: $mode"
echo "protocol: $protocol"
echo "source ftp/sftp directory: $ftp_directory"
echo "local directory: $local_directory"
echo "source ftp/sftp archive directory: $ftp_archive_directory"
echo "filemask: $filemask"
echo "processed_filemask: $processed_filemask"
echo "removal_filemask: $removal_filemask"


echo ""
echo "*******Starting file transfer********"
echo "-------------------------------------"
echo ""

connection_string="$protocol://$username@$host"
echo "Connection String: $connection_string"
echo "changing directory locally to $local_directory"
cd "$local_directory" || kill -INT $$;

if [ "$mode" == "delete" ]
then
    echo "Connecting to $protocol $host as $username in delete mode."
    lftp  $connection_string << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l $filemask
    echo "Getting all $filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory $filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
elif [ "$mode" == "archive" ]
then
    echo "Connecting to $protocol $host as $username in archive mode."
    lftp  $connection_string << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l $filemask
    echo "Getting all $filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory $filemask
    echo "Transfer complete. Archiving files on $host under $ftp_archive_directory..."
    echo "Changing directory to $ftp_archive_directory..."
    cd "$ftp_archive_directory"
    echo "Putting files at $ftp_archive_directory"
    mput $filemask
    ls -l $filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
elif [ "$mode" == "put" ]
then
    echo "Listing local files to transfer..."
    ls -l $local_directory/$filemask
    echo "Connecting to $protocol $host as $username in put mode."
    lftp  $connection_string << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "Getting all local $filemask files. Placing them remotely at $local_directory"
    mput $local_directory/$filemask
    echo "Transfer complete. Listing files on $host under $ftp_archive_directory..."
    ls -l $filemask
    bye
EOF
elif [ "$mode" == "delete-done" ]
then
    echo "Connecting to $protocol $host as $username in delete and mark done mode."
    lftp  $connection_string << EOF
    echo "Changing directory to $ftp_directory..."
    cd "$ftp_directory"
    echo "List *$removal_filemask files"
    ls -l *$removal_filemask
    mrm *$removal_filemask
    echo "Removing $removal_filemask files..."
    echo "Listing files to transfer..."
    ls -l $filemask
    echo "Getting all $filemask files. Placing them locally at $local_directory"
    mget -E -O $local_directory $filemask
    bye
EOF
    for i in *"$filemask";do cp "$i" "$i""$processed_filemask"; done
    echo "Connecting to $protocol $host as $username in delete and mark done mode."
    lftp  $connection_string << EOF
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
    lftp  -u $username $connection_string << EOF

    echo "doing pwd, ls"
    pwd
    ls
    echo "Changing directory to $ftp_directory...    "
    cd "$ftp_directory"
    echo "Listing files to transfer..."
    ls -l $filemask
    echo "Getting all $filemask files. Placing them locally at $local_directory"
    mget -O $local_directory $filemask
    bye
EOF
    echo "Listing all locally trasferred files at $local_directory"
    ls -ltr $local_directory
fi

#remove credentials in ~/.netrc for safe keeping
rm ~/.netrc

echo ""
echo "*******File transfer complete********"
echo "-------------------------------------"
echo ""
