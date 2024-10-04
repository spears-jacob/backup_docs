#!/usr/bin/python3

# Note: Requires Python 3.3 or higher
import os
import sys
import boto3
import requests
import configparser
import base64
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from os.path import expanduser
from getpass import getpass


##########################################################################
# Variables

# region: The default AWS region that this script will connect
# to for all API calls
region = 'us-east-1'

# output format: The AWS CLI output format that will be configured in the
# saml profile (affects subsequent CLI calls)
outputformat = 'json'

# awsconfigfile: The file where this script will store the temp
# credentials under the saml profile
awsconfigfile = '/.aws/credentials'

# SSL certificate verification: Whether or not strict certificate
# verification is done, False should only be used for dev/test
sslverification = True

# idpentryurl: The initial URL that starts the authentication process.
idpentryurl = 'https://login.sso.charter.com/nidp/saml2/idpsend?id=pdmi'
unpwurl = 'https://login.sso.charter.com:8443/nidp/app/login?sid=0&sid=0'
tokenurl = 'https://login.sso.charter.com:8443/nidp/app/login?sid=0&sid=0'

# account-aliases: Map of account numbers to account aliases for human-readable output.
account_aliases = {
    "387455165365": "impulse-prod",
    "213705006773": "impulse-dev",
    "068078431923": "warp"
}

##########################################################################

# Initiate session handler
session_vpn = requests.Session()

# Get Credentials from users
credentials = {'username': input('username: '), 'password': getpass(prompt='password: '), 'token': ''}
response = session_vpn.get(idpentryurl, verify=sslverification)

response = session_vpn.post(unpwurl, 'option=credential&test=null&Ecom_User_ID={username}&Ecom_Password={password}'.format(**credentials), headers={'Content-Type': 'application/x-www-form-urlencoded'}, verify=sslverification)

response = session_vpn.post(tokenurl, 'option=credential&test=null&Ecom_User_ID={username}&Ecom_Token={token}'.format(**credentials), headers={'Content-Type': 'application/x-www-form-urlencoded'}, verify=sslverification)

response = session_vpn.get(idpentryurl, verify=sslverification)

# Decode the response and extract the SAML assertion
soup = BeautifulSoup(response.text, features="html.parser")
assertion = ''

# Look for the SAMLResponse attribute of the input tag (determined by
# analyzing the debug print lines above)
for tag in soup.find_all('input'):
    if tag.get('name') == 'SAMLResponse':
        # print(inputtag.get('value'))
        assertion = tag.get('value')

session_vpn.close()

if assertion == '':

    session_no_vpn = requests.Session()

    credentials["token"] = input("you are not on VPN please enter VIP-Token: ")

    response = session_no_vpn.get(idpentryurl, verify=sslverification)

    response = session_no_vpn.post(unpwurl, 'option=credential&test=null&Ecom_User_ID={username}&Ecom_Password={password}'.format(**credentials), headers={'Content-Type': 'application/x-www-form-urlencoded'}, verify=sslverification)

    response = session_no_vpn.post(tokenurl, 'option=credential&test=null&Ecom_User_ID={username}&Ecom_Token={token}'.format(**credentials), headers={'Content-Type': 'application/x-www-form-urlencoded'},verify=sslverification)

    response = session_no_vpn.get(idpentryurl, verify=sslverification)

    # Decode the response and extract the SAML assertion
    soup = BeautifulSoup(response.text, features="html.parser")
    assertion = ''

    # Look for the SAMLResponse attribute of the input tag (determined by
    # analyzing the debug print lines above)
    for tag in soup.find_all('input'):
        if (tag.get('name') == 'SAMLResponse'):
            # print(inputtag.get('value'))
            assertion = tag.get('value')

    if assertion == '':
        print("Check your Username, Password, or VIP Token. \n Please Try Again.")
        sys.exit(0)

# print(response.text)
# Overwrite and delete the credential variables, just for safety
credentials["username"] = '##############################################'
credentials["password"] = '##############################################'
credentials["token"] = '#############################'
del credentials

# Debug only
# print(base64.b64decode(assertion))
# Parse the returned assertion and extract the authorized roles
sts_assume_role_kwargs = {}
awsroles = []
session_duration = ''
root = ET.fromstring(base64.b64decode(assertion))

for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
    if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
        for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
            awsroles.append(saml2attributevalue.text)
    elif (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/SessionDuration'):
        for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
            sts_assume_role_kwargs['DurationSeconds'] = int(saml2attributevalue.text)

# Note the format of the attribute value should be role_arn,principal_arn
# but lots of blogs list it as principal_arn,role_arn so let's reverse
# them if needed
for awsrole in awsroles:
    chunks = awsrole.split(',')
    if 'saml-provider' in chunks[0]:
        newawsrole = chunks[1] + ',' + chunks[0]
        index = awsroles.index(awsrole)
        awsroles.insert(index, newawsrole)
        awsroles.remove(awsrole)

aws_roles_maps = []
for awsrole in awsroles:
    role_item = {'samlRole': awsrole, 'roleName': awsrole.split(',')[0][31:]}
    account_number = awsrole.split(',')[0][13:25]
    role_item['accountName'] = account_aliases[account_number] if account_number in account_aliases else account_number
    aws_roles_maps.append(role_item)

# If I have more than one role, ask the user which one they want,
# otherwise just proceed
print("")
selectedroleindex = 0
if len(awsroles) > 1:
    i = 0
    print("Please choose the role you would like to assume:")
    for role_item in aws_roles_maps:
        print('[', i, ']:', role_item['accountName'], '-', role_item['roleName'])
        i += 1

    print("Selection: ", end=' ')
    selectedroleindex = int(input())

    # Basic sanity check of input
    if selectedroleindex > (len(aws_roles_maps) - 1):
        print('You selected an invalid role index, please try again')
        sys.exit(0)

    role_arn = aws_roles_maps[selectedroleindex]['samlRole'].split(',')[0]
    print("role_arn = " + str(role_arn))
    principal_arn = aws_roles_maps[selectedroleindex]['samlRole'].split(',')[1]
    print("principal_arn = " + str(principal_arn))

else:
    role_arn = aws_roles_maps[selectedroleindex]['samlRole'].split(',')[0]
    print("principal_arn = " + str(role_arn))
    principal_arn = aws_roles_maps[selectedroleindex]['samlRole'].split(',')[1]
    print("principal_arn = " + str(principal_arn))

profile_name = aws_roles_maps[selectedroleindex]['accountName']

sts_assume_role_kwargs['SAMLAssertion'] = assertion
sts_assume_role_kwargs['RoleArn'] = role_arn
sts_assume_role_kwargs['PrincipalArn'] = principal_arn

# Use the assertion to get an AWS STS token using Assume Role with SAML
sts_client = boto3.client('sts')
token = sts_client.assume_role_with_saml(**sts_assume_role_kwargs)

# Write the AWS STS token into the AWS credential file
home = expanduser("~")
filename = home + awsconfigfile

if not os.path.exists(filename):
    os.system(' mkdir ~/.aws/')

# Read in the existing config file
config = configparser.RawConfigParser()
config.read(filename)

# Put the credentials into a saml specific section instead of clobbering
# the default credentials
if not config.has_section(profile_name):
    config.add_section(profile_name)

config.set(profile_name, 'output', outputformat)
config.set(profile_name, 'region', region)
config.set(profile_name, 'aws_access_key_id', token['Credentials']['AccessKeyId'])
config.set(profile_name, 'aws_secret_access_key', token['Credentials']['SecretAccessKey'])
config.set(profile_name, 'aws_session_token', token['Credentials']['SessionToken'])

# Write the updated config file
with open(filename, 'w+') as configfile:
    config.write(configfile)

# Give the user some basic info as to what has just happened
print('\n\n----------------------------------------------------------------')
print('Your new access key pair has been stored in the AWS configuration file {0} under the {1} profile.'.format(
    filename, profile_name))
print('Note that it will expire at {0}.'.format(token['Credentials']['Expiration']))
print('After this time, you may safely rerun this script to refresh your access key pair.')
print(
    'To use this credential, call the AWS CLI with the --profile option (e.g. aws --profile {0} ec2 describe-instances).'.format(profile_name))
print('----------------------------------------------------------------\n\n')


print('export AWS_ACCESS_KEY_ID=' + '"' + str(token['Credentials']['AccessKeyId']) + '"')
print('export AWS_SECRET_ACCESS_KEY=' + '"' + str(token['Credentials']['SecretAccessKey']) + '"')
print('export AWS_SESSION_TOKEN=' + '"' + str(token['Credentials']['SessionToken']) + '"')
