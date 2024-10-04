#!/bin/sh

if [ $# -ne 2 ]; then
    echo $0: Usage: MVfiles.sh {Local working Folder} {Local archive Folder}
    exit 1
fi

# copies then moves all gzip/txt files to archive folder
cp -f $1*.gz $2
cp -f $1*.txt $2
rm -f $1*gz
rm -f $1*.txt

# gets oldest file based on yyyy-mm-dd filemask in archive folder
OldestDateInFilenames=$(perl ../bin/ld.pl "$2");

# writes the eldest date to rundate.txt
echo $OldestDateInFilenames > $1run_date.text
echo This run date is $OldestDateInFilenames and is now stored in a text file called run_date.text

# moves the eldest files up to working folder and cleans eldest from archive folder
echo now copying files up to working directory
cp -f $2*$OldestDateInFilenames* $1
ls -al $1

rm -f $2*$OldestDateInFilenames*
