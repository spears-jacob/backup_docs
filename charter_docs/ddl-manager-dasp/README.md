# ddl-manager
Repository for managing Hive/Glue/Athena table configuration (including DDL) for Charter PI Datalake

## Creating a new table
To create a new table, first define the table's DDL, then associate the table with the appropriate S3 bucket using the
`tables-config` map in `tables-config.tf` as described below.

### Create DDL script
One DDL script that is executed in Athena (Presto) can be defined for each table and will be executed if it has changed
(if hash doesn't match hash of last-deployed script file) whenever a deployment job is run from the repo.
1. In the `flows` directory, select the directory that corresponds to the flow category (e.g., `ipv-db-tables-init` for
IP Video) in which the table will be used.
1. Create a new directory that relates to the name of the table within the `scripts` directory.
1. Create a new file within the newly-created directory that contains the table's DDL. The path to this file will be
used later as the DDL path.

### Plumb the table into the appropriate bucket
1. In the `flows` directory, select the directory that corresponds to the flow category (e.g., `ipv-db-tables-init` for
IP Video) in which the table will be used.
1. Edit the `configuration/tables-config.tf` file within the directory in the flow category.
1. Add a JSON object to one of the already-created lists in the tables_config map. Use list that corresponds with the
most relevant key. For example, if the table contains aggregates and PII inforrmation, the object will be added to the
list value mapped to the `aggregates-pii` key.

The JSON object to add to the list is constructed as follows.
```
{
  db_table_name = "DATABASE.TABLE_NAME"
  ddl_path = "PATH_TO_DDL_SCRIPT"
}
```

Replace the following strings that appear in the object above with values as described below.

| string | value |
|  ----  | ----- |
|`DATABASE`|Use the literal value `${var.env}` to represent both `prod` and `dev`. Prefix/suffix it as needed. For example: `tmp_${var.env}` would resolve to `tmp_dev` in the `dev` environment and `tmp_prod` in the `prod` environment.|
|`TABLE_NAME`|The desired name of the table.|
|`PATH_TO_DDL_SCRIPT`|The local variable accessed by the literal value `${local.path_to_ddl_scripts_dir}` provides the prefix to the scripts path. Add to it the DDL path as created above to identify the script that should be run to initialize the table. For example `"${local.path_to_ddl_scripts_dir}/my_new_table/my_new_table.pql"`|


## Deployment State and Static Reference
The `master` and `dev` branches of ddl-manager are used for two separate purposes and must be managed carefully in order
for the Charter PI Datalake environments to function correctly. The purposes are as follows:

1. The `master` branch is used to deploy to the `prod` environment. The `dev` branch is used to deploy to the `dev`
environment. At any time, the code in both branches should reflect the true state of the environments.
1. The static code in the `master` branch is referenced by external repos in order to deploy resources with the proper
access privileges in the `prod` environment. The same is true for the `dev` branch and the `dev` environment.

### Workflow to ensure consistent state (all changes except table relocation)
The following workflow ensures that the state of AWS environments correspond correctly to the code in the `ddl-manager`
repo.
1. Create a branch from the `dev` branch.
1. Make, commit, and push the needed code changes.
1. Create a Merge Request targeting the `dev` branch.
1. As part of the Merge Request approval process, run the necessary planning jobs targeting the `dev` and `prod`
environments to ensure the desired changes are reflected correctly in the Terraform plan.
1. After the Merge Request is approved, merge the changes into the `dev` branch.
1. Run the appropriate plan and deploy jobs targeting the `dev` environment from the dev branch.
1. **Temporary**: from external repos, create branches where `ddl-manager` module references are targeted at the `dev`
branch. Perform development testing this way.
1. Once changes are validated in the `dev` environment, create a Merge Request from `dev` to `master`.
1. After the Merge Request is approved, merge the changes into `master`.
1. Run the appropriate plan and deploy jobs targeting the `prod` environment from the master branch.
1. **Temporary**: in external repos that reference `ddl-manager`, make sure to reset `ddl-manager` module references
back to the `master`.

### Handling table relocation
If a table is being relocated (table config object is moved to a list under a different S3-key in the table-config map),
it is necessary to redeploy all external entities that reference the table in order for these entities to obtain the
permissions necessary to access the new location. For example, if an IP Video job lists the table as a read/write table,
then the last deployment of that job should be re-run. The Terraform plan for the job should show the updated
location in the IAM Role's permission set.

## S3 Bucket Creation
Developers should not need to create or manage S3 buckets directly. S3 buckets should be created by DevOps/Admins and
defined in the dir of the `flows/<flow category>/s3-buckets-init` directory. See
[tfmodules datalake_bucket](https://gitlab.spectrumflow.net/awspilot/charter-telemetry-pilot-tfmodules/tree/master/datalake_bucket)
for more information.

## Limitations
The following limitations apply to `ddl-manager`.
1. A single query is executed as part of the DDL script execution. This means that updates to DDL should be handled
manually in Athena or Glue.
1. It is necessary to re-scan for partitions if DDL changes affect partitioning or if the S3 location of a table
changes. This can be triggered in Athena.
