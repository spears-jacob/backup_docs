The flow that is scheduled to run is the one that ends at 03_transfer_daily_file.job
  It creates a file that has all accounts that logged in yesterday, on all 3 portals, and
  copies that file to a location on HDFS that will cause it to be FTPd to the appropriate site

Sometimes the recipient will ask us for a "historical backfill" file, which is the same data, in
  the same format, but for all time instead of for yesterday.  When this request is made, use
  the flow backfill_00_transfer_file.job

  In both cases, the file is moved from HDFS to FTP once an hour, and so it may take up to
  60 minutes for you to see the file on FTP when it shows up.  You can use ftpcheck.sh to
  see which files are on the FTP site. 
