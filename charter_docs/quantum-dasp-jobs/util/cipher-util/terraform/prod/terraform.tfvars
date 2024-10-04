### Change per job
job_name                  = "cipher-util"

### Use the default config in the job-scope-template
### or choose an emr_config_key
emr_config_key            = "memory_690_2c_6t_150"
### or roll your own below
###  --> FIX NEEDED /// the default_target_capacity, still required
default_target_capacity   = 20

## Seldom used
on_demand_switch          = "false"

## Run multiple EMRs of this job at the same time using the lambda to start each one
allow_parallel_execution  = "true"

### Only set enable_schedule = true if job is to be run independent of orchestration for testing
### It is highly preferred to have each job in a coordinator so they can be reviewed and run along
### with other jobs after checking dependencies
schedule_expression = "cron(01 09 * * ? *)"
enable_schedule     = false

### Unlikely to change (per line of business) ###
flow_category = "dasp"