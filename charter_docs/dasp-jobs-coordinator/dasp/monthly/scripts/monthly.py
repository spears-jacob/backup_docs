import argparse
import json

from coordinator import Flow
from coordinator import Parallel
from coordinator import Job

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
    flow = Flow(comment='Execution of monthly dasp jobs').start_at(
        Parallel('parallel-1',
                 Job('m-msa-onboarding',
                     job_input=job_input_params,
                     job_error_handler=True),
                 Job('product-monthly-data',
                     job_input=job_input_params,
                     job_error_handler=True)
                 )
    )
    flow.to_file(f'{args.path}/{args.name}.json')
