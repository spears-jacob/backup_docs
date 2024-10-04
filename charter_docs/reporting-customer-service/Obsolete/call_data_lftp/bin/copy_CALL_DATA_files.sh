


cd /data/tmp/


echo "################# Pulling CALL_DATA files ########################"


lftp sftp://twcacct:dummy@files.chartercom.com <<EOF

    cd 'uat'
    ls
    exit
EOF

#lftp sftp://twcacct:dummy@files.chartercom.com <<EOF


# Unzip files in place
#find CALL_P270_*.txt.gz -exec gunzip {} -f \;
#mget CALL_P270_JAN18*
#mget CARE_P270_08222018_09212018_SPA_ACCT.txt.gz
