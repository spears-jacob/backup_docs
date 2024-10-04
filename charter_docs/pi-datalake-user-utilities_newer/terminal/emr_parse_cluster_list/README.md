## parse_cluster_list.sh
#### List EMR cluster details by name for a number of days back

_There are times when it is helpful to check on the details of prior runs of a given EMR.  Using the AWS Console to reach back several days takes a while and is unnecessarily time consuming to find a given EMR cluster by name.  The following approach uses the credentials and region of the user  running the job.  The script pulls down all EMR clusters for a given number of days, which defaults to three days.  The actual aws emr list-clusters command uses date boundaries on both sides to keep the pulls to a modest size, but the approach also uses pagination to allow all the results of each command to be returned.  After all the cluster details are pulled down into a json file called ```listOfclusters.json``` in a temporary folder that is then parsed by jq to pull out the relevant bits.  Then, grep is run against the parsed file ```listOfclusters.txt``` and there is a shortcut pointing to the file for later processing._

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- Configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## parse_cluster_list.sh Usage
```
source parse_cluster_list.sh string_to_grep_from_cluster_name <days to lag>

  -- Run this script using the cluster string you wish to search, and any number
     of days > 0 that is needed to be returned.  It defaults to three days back
     if nothing is provided.

  -- The results returned relate to the credentials currently exported

  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN
```

## Output resembles the following
|Status.Timeline.CreationDateTime| EMR Cluster Id | EMR Cluster Name     | Status.State          | CPU Hours | Elapsed days and hh:mm:ss | URL |
|--------------------------------|----------------| -------------------- | ----------            | ----------| ---                       | --- |
|2021-10-19T12:15:33.784000-06:00| j-3L4J2XBYJUMTO| pi-qtm-cs-stg-sps-cir| TERMINATED            | 816       | 21:12                     | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-3L4J2XBYJUMTO|
|2021-10-20T12:15:33.888000-06:00| j-X1LX2CGRCMKB | pi-qtm-cs-stg-sps-cir| TERMINATED            | 816       | 20:29                     | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-X1LX2CGRCMKB|
|2021-10-20T13:15:23.239000-06:00| j-2DAHNF61PGOW9| pi-qtm-cs-stg-sps-cir| TERMINATED_WITH_ERRORS| 0         | null                      | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-2DAHNF61PGOW9|
|2021-10-20T13:40:35.977000-06:00| j-3DSRIQ6KUQV1 | pi-qtm-cs-stg-sps-cir| TERMINATED            | 832       | 25:52                     | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-3DSRIQ6KUQV1|
|2021-10-20T15:23:18.076000-06:00| j-1ID67VKEZW2IS| pi-qtm-cs-stg-sps-cir| TERMINATED            | 816       | 24:37                     | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-1ID67VKEZW2IS|
|2021-10-21T12:15:33.514000-06:00| j-1UMZY9SW6GUVN| pi-qtm-cs-stg-sps-cir| TERMINATED_WITH_ERRORS| 0         | null                      | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-1UMZY9SW6GUVN|
|2021-10-21T12:41:06.566000-06:00| j-30N3SES12CA1C| pi-qtm-cs-stg-sps-cir| TERMINATED            | 816       | 30:53                     | https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-30N3SES12CA1C|
