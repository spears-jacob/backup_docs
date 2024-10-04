## get_active_cluster_master_ec2_id.sh
#### Choose the master node EC2 instance id by selection from a list of running EMR clusters based on the current credentials and region.

#### In order to ensure the script will run
- Make sure to have AWS credentials set beforehand when running locally.

#### Requirements for running this script on a mac laptop.
  1. first install xcode:  `xcode-select --install`
  2. then homebrew:        `/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)" `
  3. then aws cli:         `brew install awscli`
  4. then jq:              `brew install jq`
  5. bash 5 -- check first: (bash --version), install if needed: https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3

## get_active_cluster_master_ec2_id.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source get_active_cluster_master_ec2_id.sh`
  2. allow for execution in any terminal by placing the contents of the script in `~/.bash_profile`
  3. create an alias that points to the script in `~/.bash_profile`


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
