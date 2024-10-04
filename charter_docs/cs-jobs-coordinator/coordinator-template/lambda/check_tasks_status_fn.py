import json
import logging
import traceback
import boto3

sfn = boto3.client('stepfunctions')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def get_succeeded_jobs(sfn_event):
    succeeded_events = [success_event for success_event in sfn_event['events'] if success_event['type'] == "TaskSucceeded"]
    LOGGER.info("Success Events: {0}".format(succeeded_events))
    if succeeded_events:
        task_succeeded_event_details = [json.loads(details['taskSucceededEventDetails']["output"]) for details in succeeded_events]
        succeeded_input = [json.loads(detail["Input"]) for detail in task_succeeded_event_details]
        succeeded_jobs = [job["CurrentJob"] for job in succeeded_input]
    else:
        succeeded_jobs = []
    return succeeded_jobs

def get_failed_jobs(sfn_event):
    failed_events = [failed_event for failed_event in sfn_event['events'] if failed_event['type'] == "TaskFailed"]
    LOGGER.info("Failed Events: {0}".format(failed_events))
    if failed_events:
        task_failed_event_details = [json.loads(details['taskFailedEventDetails']["cause"]) for details in failed_events]
        failed_input = [json.loads(detail["Input"]) for detail in task_failed_event_details]
        failed_jobs = [job["CurrentJob"] for job in failed_input]
    else:
        failed_jobs = []
    return failed_jobs

def lambda_handler(event, context):
    try:
        execution_arn = event.get('SFN_EXECUTION_ARN')
        response = sfn.get_execution_history(
            executionArn=execution_arn,reverseOrder=True,maxResults=1000
        )
        succeeded_jobs = get_succeeded_jobs(response)
        failed_jobs = get_failed_jobs(response)

        status_code = 1 if failed_jobs else 0
        return {'code': status_code, 'succeeded_jobs': succeeded_jobs, 'failed_jobs': failed_jobs}

    except Exception as excepts:
        trc = traceback.format_exc()
        fail_message = "Failed to execute: {0}\n\n{1}".format(str(excepts), trc)
        LOGGER.error(fail_message)
        return {'code': 1, 'msg': fail_message}