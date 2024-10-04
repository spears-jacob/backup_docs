import argparse

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

new_job_input_params = {
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
    flow = Flow(comment='Execution of daily SSPP quality jobs').start_at(
        Parallel('parallel-1',
                 Job('quality-visit-agg',
                     job_input=new_job_input_params,
                     job_error_handler=True),
                 Job('idm-quality-visit-agg',
                     job_input=new_job_input_params,
                     job_error_handler=True),
                 Job('quality-auth',
                     job_input=new_job_input_params,
                     job_error_handler=True)
                 ).next(
            Parallel('parallel-2',
                     Job('qva-tableau',
                         job_input=new_job_input_params,
                         job_error_handler=True),
                     Job('component-distribution',
                         job_input=new_job_input_params,
                         job_error_handler=True),
                     Job('quality-error-agg',
                         job_input=new_job_input_params,
                         job_error_handler=True)
                     )
        )
    )

    flow.to_file(f'{args.path}/{args.name}.json')
