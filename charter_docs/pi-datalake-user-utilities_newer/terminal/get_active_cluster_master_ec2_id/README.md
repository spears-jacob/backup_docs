## get_active_cluster_master_ec2_id.sh
#### Choose the master node EC2 instance id by selection from a list of running EMR clusters based on the current credentials and region.

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- See the [common_utilities readme](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md) for requirements.

## get_active_cluster_master_ec2_id.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source get_active_cluster_master_ec2_id.sh`
  2. configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)


## get_active_cluster_master_ec2_id.sh use
```
gac
```
Then use the up and down arrows to select an active cluster and use enter the confirm your choice.

```
Choose and active cluster:
 >     1        pi-qtm-dasp-stg-scp-portals-daily          j-S8RP4NM6FUFH
       2        pi-qtm-ipv-stg-m-content-analysis          j-27JWVMFYONOWE
       3        pi-spg-orig-stg-bau-1-base                 j-24UKDKA6IXU1S
       4        pi-spg-orig-stg-vod-shelf-performance-agg  j-3IXD0N15XAPPO
$ec2: i-08e5c277f48432bfb
```

Then, the $ec2 environment variable is made available for use, such as any of the following.

```
ssh $ec2
ssh -N -D 8157 $ec2
scp testfile $ec2:
```
