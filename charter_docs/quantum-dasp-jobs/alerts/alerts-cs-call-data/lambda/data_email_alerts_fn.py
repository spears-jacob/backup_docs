# This script checks to see if files are available for the RUN_TIME,
# which is generally today.  If files are not available, then an E-mail alert is
# send to the E-mail recipients to check it out.

# Returns {'code': 0} - if alert was not send
#         Exception("An exception occured", alert_message) is thrown - if alert was send
# Input example:
# {
#   "S3_BUCKET_NAME": "pi-qtm-dasp-dev-aggregates-nopii",
#   "ENVIRONMENT": "dev",
#   "DATA_DESCRIPTION": "CS Call Data (cs_call_data)",
#   "START_DATE": "2020-02-06",
#   "DATA_USAGE_THRESHOLD": 10000000,
#   "EMAIL_FROM": "PI.Tableau@charter.com",
#   "EMAIL_TO_LIST_COMMA": "dl-pi-asp-reporting@charter.com",
#   "EMAIL_CC_LIST_COMMA": "dl-pi-asp-reporting@charter.com",
#   "EMAIL_DEBUG_TO_LIST_COMMA": "elliott.easterly@charter.com",
#   "EMAIL_DEBUG_CC_TO_LIST_COMMA": "c-elliott.easterly@charter.com",
#   "TZ": "America/Denver",
#   "IS_DEBUGGING_ENABLED": true,
#   "USE_HOURS": true,
#   "PATH": "data/prod/atom_cs_call_care_data_3/",
#   "FILE_MASKS": "partition_date_utc=<datemask>/partition_date_hour_utc=<datehourmask>",
#   "RETRY_PERIOD": "2",
#   "CALC_SIZE_ACROSS_ALL_FILEMASKS" : false
# }

#  TODO WHAT about exec url?
import logging
import datetime
import boto3
import os
import pytz
import json

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ses_client = boto3.client('ses')
s3 = boto3.client('s3')


def build_alert_email_body(folder_size_too_small_messages, time_zone,
                           start_date, data_description, next_retry_attempt, total_attempts, retry_period_sec):
    dev_now_part = build_den_now_alert_part(time_zone, start_date)
    headline_part = build_headline_alert_part(data_description, start_date, next_retry_attempt, total_attempts)
    try_again_part = build_try_again_alert_part(next_retry_attempt, total_attempts, retry_period_sec)
    command_output_part = build_command_output_alert_email_body_part(folder_size_too_small_messages)

    return "\n".join([dev_now_part, headline_part, try_again_part, command_output_part])


# build 'headline' alert email body part
def build_headline_alert_part(data_description, start_date, next_retry_attempt, total_attempts):
    return "{0} data delayed for {1} attempt {2} of {3}" \
        .format(data_description, start_date, next_retry_attempt, total_attempts)


# build 'try.again' alert email body part
def build_try_again_alert_part(next_retry_attempt, total_attempts, retry_period_sec):
    if next_retry_attempt < total_attempts:
        retry_period_min = retry_period_sec // 60
        return "Will sleep for {} minutes and try again".format(retry_period_min)
    else:
        return "Since data remains unavailable after {} retry attempts, the job is failing.".format(total_attempts)


# build 'den.now' alert email body part
def build_den_now_alert_part(time_zone, start_date):
    return str(pytz.timezone(time_zone).localize(start_date))


# build 'command.output' alert email body part
def build_command_output_alert_email_body_part(folder_size_too_small_messages):
    return "\n".join(folder_size_too_small_messages)


def get_date_hour_list(time_zone, start_date, use_hours,start_offset):

    start_date += datetime.timedelta(days=start_offset)
    tz = pytz.timezone(time_zone).localize(start_date)
    utc_tz = tz.astimezone(pytz.utc)
    date_hour_list = [utc_tz.strftime("%Y-%m-%d_%H")]
    if not use_hours:
        return date_hour_list

    for _ in range(23):
        utc_tz += datetime.timedelta(hours=1)
        date_hour_list.append(utc_tz.strftime("%Y-%m-%d_%H"))
    return date_hour_list


# build email message in SES compatible format
def build_ses_email_message(data_description, start_date, next_retry_attempt, total_attempts, message):
    subject = "{0} data delayed for {1} attempt {2} of {3}" \
        .format(data_description, start_date, next_retry_attempt, total_attempts)

    charset = 'UTF-8'
    alert_message = {
        'Subject': {
            'Charset': charset,
            'Data': subject
        },
        'Body': {
            'Text': {
                'Charset': charset,
                'Data': message
            }
        }
    }
    return alert_message


# send alert message via SES to 'email_to_list', 'email_cc_list'
def send_alert(email_from, email_to_list, email_cc_list, body):
    LOGGER.info("Sending email FROM {} TO '{}' and CC {}".format(email_from, email_to_list, email_cc_list))
    return ses_client.send_email(
        Source=email_from,
        Destination={
            "ToAddresses": email_to_list,
            "CcAddresses": email_cc_list
        },
        Message=body
    )


def build_s3_path(base_path, date_hour, file_mask):
    # datemask is date_hour without hour part(e.g. '_%H' is 3 last chars need to cut)
    datehour_mask = file_mask.replace('<datehourmask>', date_hour) \
        .replace('<datemask>', date_hour[:-3])

    LOGGER.info("datehourmask={0}".format(datehour_mask))

    return base_path + datehour_mask

# Checks data size for each date in date_hour_list using all values of filemask in list
# returns list of 'folder is too small' if total files size in this folder lower than 'du_threshold'
def check_data_size_for_all_filemasks(date_hour_list, du_threshold, s3_base_path, s3_bucket_name, file_masks):
    messages = []
    for date_hour in date_hour_list:
        total_size = 0
        s3_locations = []
        for file_mask in file_masks:
            s3_path = build_s3_path(s3_base_path, date_hour, file_mask)
            size_for_s3_path = get_total_size(s3_bucket_name, s3_path)
            LOGGER.info("s3_path: {} size: {}".format(s3_path,size_for_s3_path))
            total_size += size_for_s3_path
            s3_locations.append("s3://{}/{}".format(s3_bucket_name, s3_path))
        if total_size < du_threshold:
            message = "Folder size is too small ({} is < {} threshold) in following locations: {}" \
                .format(total_size, du_threshold, s3_locations)
            LOGGER.warn(message)
            messages.append(message)
    return messages

# Checks data size for each path generated by multiple filemasks
# returns list of 'folder is too small' if total files size in this folder lower than 'du_threshold'
def check_data_size_for_each_filemask(date_hour_list, du_threshold, s3_base_path, s3_bucket_name, file_masks):
    messages = []
    for date_hour in date_hour_list:
        for file_mask in file_masks:
            s3_path = build_s3_path(s3_base_path, date_hour, file_mask)
            total_size = get_total_size(s3_bucket_name, s3_path)
            LOGGER.info("s3_path: {} total_size: {}".format(s3_path,total_size))
            if total_size < du_threshold:
                s3_location = "s3://{}/{}".format(s3_bucket_name, s3_path)
                message = "Folder size is too small ({} is < {} threshold) for {}" \
                    .format(total_size, du_threshold, s3_location)
                LOGGER.warn(message)
                messages.append(message)
    return messages

# Calculates total objects size in specific s3 bucket
# returns 0 if there is no objects by 'path' in s3
# returns total objects size in bytes if at least 1 file exists in s3 by 'path'
def get_total_size(s3_bucket_name, path):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=s3_bucket_name,
        Prefix=path,
        # s3 list_objects_v2 can return max 1000 object in 1 request
        PaginationConfig={
            'MaxItems': 1000,
            'PageSize': 1000
        }
    )

    total_size = 0
    for page in page_iterator:
        # if there is no objects in s3 by specified path, iterator will return only 1 page with 'KeyCount' = 0
        keys_count = int(page['KeyCount'])
        LOGGER.info("Got {} keys chunk for {}".format(keys_count, path))
        if keys_count != 0:
            contents = page['Contents']
            for obj in contents:
                total_size += obj['Size']
        else:
            LOGGER.warn("No objects found in s3, set total size to 0, path: {}".format(path))
    return total_size


def parse_comma_separated_to_list(comma_separated_str):
    value = comma_separated_str.split(',')
    return [i.lstrip() for i in value]


def lambda_handler(event, context):
    LOGGER.info("Received event, {}".format(event))
    # extract input variables

    # variable start_date has datetime type here:
    start_date = datetime.datetime.strptime(
        event.get('START_DATE', datetime.datetime.now().strftime('%Y-%m-%d')), "%Y-%m-%d"
    )
    du_threshold = int(event.get('DATA_USAGE_THRESHOLD', os.environ['DATA_USAGE_THRESHOLD']))
    data_description = event.get('DATA_DESCRIPTION', os.environ['DATA_DESCRIPTION'])

    s3_bucket_name = event.get('S3_BUCKET_NAME', os.environ['S3_BUCKET_NAME'])
    s3_base_path = event.get('S3_BASE_PATH', os.environ['S3_BASE_PATH'])
    file_masks = parse_comma_separated_to_list(event.get('FILE_MASKS', os.environ['FILE_MASKS']))

    use_hours = json.loads(event.get('USE_HOURS', os.environ['USE_HOURS']))
    time_zone = event.get('TZ', os.environ['TZ'])
    start_offset = int(event.get('START_OFFSET', os.environ['START_OFFSET']))
    calc_size_across_all_filemasks = json.loads(event.get('CALC_SIZE_ACROSS_ALL_FILEMASKS', os.environ['CALC_SIZE_ACROSS_ALL_FILEMASKS']))

    email_from = event.get('EMAIL_FROM', os.environ['EMAIL_FROM'])
    email_to_list = parse_comma_separated_to_list(event.get('EMAIL_TO_LIST_COMMA', os.environ['EMAIL_TO_LIST_COMMA']))
    email_cc_list = parse_comma_separated_to_list(event.get('EMAIL_CC_LIST_COMMA', os.environ['EMAIL_CC_LIST_COMMA']))

    is_debugging_enabled = json.loads(event.get('IS_DEBUGGING_ENABLED', os.environ['IS_DEBUGGING_ENABLED']))
    email_debug_to_list = parse_comma_separated_to_list(
        event.get('EMAIL_DEBUG_TO_LIST_COMMA', os.environ['EMAIL_DEBUG_TO_LIST_COMMA'])
    )
    email_debug_cc_list = parse_comma_separated_to_list(
        event.get('EMAIL_DEBUG_CC_TO_LIST_COMMA', os.environ['EMAIL_DEBUG_CC_TO_LIST_COMMA'])
    )

    current_retry_attempt = int(event.get('CURRENT_RETRY_ATTEMPT', 0))
    total_retry_attempts = int(event.get('RETRY_ATTEMPTS', os.environ['RETRY_ATTEMPTS']))
    retry_period_sec = int(event.get('RETRY_PERIOD', os.environ['RETRY_PERIOD']))

    # use debug emails if debug mode is enabled
    if is_debugging_enabled:
        email_to_list = email_debug_to_list
        email_cc_list = email_debug_cc_list
        LOGGER.info("DEBUGGING IS enabled, so E-mail messages go out to the DEBUG lists")
    else:
        LOGGER.info("DEBUGGING is NOT enabled")

    # build hour array by autoincrementing the hour_increment by one each time
    date_hour_list = get_date_hour_list(time_zone, start_date, use_hours,start_offset)

    if calc_size_across_all_filemasks:
        folder_size_too_small_messages = check_data_size_for_all_filemasks(
            date_hour_list, du_threshold, s3_base_path, s3_bucket_name, file_masks)
    else:
        folder_size_too_small_messages = check_data_size_for_each_filemask(
            date_hour_list, du_threshold, s3_base_path, s3_bucket_name, file_masks)


    # check if the alert E-mail needs to be sent, and do so if needed
    if not folder_size_too_small_messages:
        LOGGER.info("E-mail alert is NOT needed")
        LOGGER.info("{} is available for {}".format(data_description, start_date))
        return {"code": 0}
    else:
        next_retry_attempt = current_retry_attempt + 1
        LOGGER.warn("{} data is delayed for {}".format(data_description, start_date))
        LOGGER.info("Next attempt No. {} of {}".format(next_retry_attempt, total_retry_attempts))
        LOGGER.info("E-mail alert is needed")

        # build alert email body
        alert_email_body = build_alert_email_body(
            folder_size_too_small_messages, time_zone, start_date, data_description, next_retry_attempt,
            total_retry_attempts, retry_period_sec
        )
        # build email message in SES compatible format
        alert_message = build_ses_email_message(
            data_description, start_date, next_retry_attempt, total_retry_attempts, alert_email_body
        )
        LOGGER.info("E-mail alert is structured as below, {}".format(alert_message))
        send_alert(email_from, email_to_list, email_cc_list, alert_message)

        raise Exception("An exception occured", alert_message)