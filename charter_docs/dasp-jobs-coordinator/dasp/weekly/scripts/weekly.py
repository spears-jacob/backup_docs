import argparse
import json

from coordinator import Flow
from coordinator import Job
from coordinator import StartSharedEmr

parser = argparse.ArgumentParser(description='Python coordinator')
parser.add_argument('--path',
                    help='Path for saving *.json coordinator for AWS StepFunc',
                    default='.',
                    type=str)
parser.add_argument('--name',
                    help='Name for coordinator json file',
                    default='coordinator',
                    type=str)
args = parser.parse_args()

job_input_params = {
    "TargetLambdaInput": {
        "JobInput": {
            'RunDate.$': '$$.Execution.Input.RUN_TIME'
        },
    },
    'SkipJobs.$': '$$.Execution.Input.SKIP_JOBS',
    'RunOnly.$': '$$.Execution.Input.RUN_ONLY',
    'CurrentJob.$': '$$.State.Name'
}

if __name__ == '__main__':
    flow = Flow(comment='Execution of weekly dasp jobs').start_at(
        StartSharedEmr(name="blue", unit_size=500, emr_version="emr-5.25.0").next(
            Job('pmai-devices',
                job_input=job_input_params,
                shared_emr_name="blue"
                ).next(
                Job('pmai-devices-get-output',
                    job_input=job_input_params,
                    shared_emr_name="blue"
                    ).next(
                    Job('pmai-feeds-export',
                        job_input=job_input_params,
                        shared_emr_name="blue"
                        )
                )
            )
        )
    )

    flow.to_file(f'{args.path}/{args.name}.json')
