import argparse
import json

from coordinator import Flow
from coordinator import Parallel
from coordinator import Job
from coordinator import Alert
from coordinator import Wait
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

alert_portal_events_sma =     "arn:aws:states:${region}:" \
                              "${aws_account_id}:stateMachine:" \
                              "${project_tag}-${environment}" \
                              "-alert-portal-events-alert-email-sfn"

alert_app_figures_sma   =     "arn:aws:states:${region}:" \
                              "${aws_account_id}:stateMachine:" \
                              "${project_tag}-${environment}" \
                              "-alert-app-figures-alert-email-sfn"

alert_cs_call_data_sma  =     "arn:aws:states:${region}:" \
                              "${aws_account_id}:stateMachine:" \
                              "${project_tag}-${environment}" \
                              "-alert-cs-call-data-alert-email-sfn"

core_instance_type_configs = [
    {
        "InstanceType": "m5a.4xlarge",
        "WeightedCapacity": 10,
        "BidPriceAsPercentageOfOnDemandPrice": 100,
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 128,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 1
                }
            ],
            "EbsOptimized": True
        }
    },
    {
        "InstanceType": "m5.4xlarge",
        "WeightedCapacity": 10,
        "BidPriceAsPercentageOfOnDemandPrice": 100,
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 128,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 1
                }
            ],
            "EbsOptimized": True
        }
    },
    {
        "InstanceType": "r4.4xlarge",
        "WeightedCapacity": 10,
        "BidPriceAsPercentageOfOnDemandPrice": 100,
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 128,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 1
                }
            ],
            "EbsOptimized": True
        }
    },
    {
        "InstanceType": "r5d.4xlarge",
        "WeightedCapacity": 10,
        "BidPriceAsPercentageOfOnDemandPrice": 100,
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 128,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 1
                }
            ],
            "EbsOptimized": True
        }
    }
]

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
    flow = Flow(comment='Execution of extract dasp jobs').start_at(
        Alert('alert-portal-events',
              job_state_machine_arn=alert_portal_events_sma,
              job_input=job_input_params).next(
            Parallel('parallel-1',
                     StartSharedEmr(name="blue", unit_size=500, emr_version="emr-6.2.0", core_instance_type_configs=core_instance_type_configs).next(
                         Job('portals-ss-metric-agg',
                             job_input=job_input_params,
                             job_error_handler=True,
                             shared_emr_name="blue").next(
                             Parallel('inner-parallel',
                                      Job('d-portals-ss-set-agg',
                                          job_input=job_input_params,
                                          job_error_handler=True),
                                      Job('w-portals-ss-set-agg',
                                          job_input=job_input_params,
                                          job_error_handler=True,
                                          shared_emr_name="blue"),
                                      Job('d-msa-onboarding',
                                          job_input=job_input_params,
                                          job_error_handler=True),
                                      Job('w-msa-onboarding',
                                          job_input=job_input_params,
                                          job_error_handler=True)
                                      )
                         )),
                     Wait('wait_twelve_minutes',
                          wait_seconds=720).next(
                         StartSharedEmr(name="orange", unit_size=500, emr_version="emr-5.25.0").next(
                         Parallel('parallel-1-orange',
                                  Job('mvno-sspp-activity',
                                      job_input=job_input_params,
                                      job_error_handler=True),
                                 Job('portals-ps-metric-agg',
                                     job_input=job_input_params,
                                     job_error_handler=True,
                                     shared_emr_name="orange").next(
                                     Job('d-portals-ps-set-agg',
                                         job_input=job_input_params,
                                         job_error_handler=True,
                                         shared_emr_name="orange")
                                 ))
                     )),
                     Alert('alert-app-figures-alert-email',
                           job_input=job_input_params,
                           job_state_machine_arn=alert_app_figures_sma,
                           job_error_handler=True).next(
                         StartSharedEmr(name="black", unit_size=500, emr_version="emr-5.25.0").next(
                             Job('ingest-app-figures',
                                 job_input=job_input_params,
                                 job_error_handler=True,
                                 shared_emr_name="black").next(
                                 Job('app-figures-ratings-d',
                                     job_input=job_input_params,
                                     job_error_handler=True,
                                     shared_emr_name="black")
                             ))
                     ),
                     Wait('wait_three_minutes', wait_seconds=181).next(
                         Job('ccpa-usage',
                             job_input=job_input_params,
                             job_error_handler=True)
                     ),
                     Wait('wait_four_minutes', wait_seconds=240).next(
                         Job('bounces-entries',
                             job_input=job_input_params,
                             job_error_handler=True)
                     ),
                     Wait('wait_five_minutes', wait_seconds=300).next(
                         Job('login-data',
                             job_input=job_input_params,
                             job_error_handler=True)
                     ),
                     Wait('wait_six_minutes', wait_seconds=360).next(
                         Job('asapp-visitid-data',
                             job_input=job_input_params,
                             job_error_handler=True)
                     ),
                     Wait('wait_seven_minutes', wait_seconds=720).next(
                         Job('spec-mobile-data',
                             job_input=job_input_params,
                             job_error_handler=True)
                     ),
                     Alert('alerts-cs-call-data',
                           job_state_machine_arn=alert_cs_call_data_sma,
                           job_input=job_input_params,
                           job_error_handler=True).next(
                         Parallel('support-jobs',
                            Job('sup-search-perf',
                                job_input=job_input_params,
                                job_error_handler=True),
                            Job('sup-article-call-disp',
                                job_input=job_input_params,
                                job_error_handler=True)
                        )
                     ),
                     Job('page-render-times',
                         job_input=job_input_params,
                         job_error_handler=True),
                     Wait('wait_eleven_minutes', wait_seconds=660).next(
                         Job('idm-quality',
                             job_input=job_input_params,
                             job_error_handler=True)
                     )
                     ).next(
                Parallel('parallel-2',
                         Job('d-report-data',
                             job_input=job_input_params,
                             job_error_handler=True),
                         Job('dasp-quality',
                             job_input=job_input_params,
                             job_error_handler=True),
                         Job('m-portals-ss-set-agg',
                             job_input=job_input_params,
                             job_error_handler=True),
                         Job('fm-portals-ss-set-agg',
                             job_input=job_input_params,
                             job_error_handler=True)
                         )
            )
        )
    )

    flow.to_file(f'{args.path}/{args.name}.json')
