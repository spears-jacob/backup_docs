import json
import logging
import os
import traceback
import datetime
import boto3

sfn = boto3.client('stepfunctions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Function just calls specified state machine with input event
def handler(event, context):
    try:
        run_time = event.get('RUN_TIME', datetime.datetime.now().strftime('%Y-%m-%d'))
        skip_jobs = event.get('SKIP_JOBS',[])
        run_only = event.get('RUN_ONLY',[])
        event['RUN_TIME'] = run_time
        event['SKIP_JOBS'] = skip_jobs
        event['RUN_ONLY'] = run_only
        event_as_json = json.dumps(event)
        LOGGER.info("Incoming event: {0}".format(event_as_json))
        sfn_arn = os.environ['SFN_ARN']

        response = sfn.start_execution(
            stateMachineArn=sfn_arn,
            input=event_as_json
        )

        LOGGER.info("Response from sfn: {0}".format(response))
        return {'code': 0, 'msg': "", 'sfn': sfn_arn, 'run_time': run_time, 'skip_jobs': skip_jobs, 'run_only': run_only }

    except Exception as e:
        trc = traceback.format_exc()
        s = "Failed to execute: {0}\n\n{1}".format(str(e), trc)
        LOGGER.error(s)
        return {'code': 1, 'msg': s}