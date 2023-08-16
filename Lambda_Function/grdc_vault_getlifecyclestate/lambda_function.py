import requests
import datetime
import pandas as pd
import json
import boto3
import botocore 
import botocore.session 
import logging
import base64
from botocore.config import Config
import env_constants
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Retrieving credentials from Secret Manager & creating session ID
def create_session():
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=env_constants.region_name, config=Config(env_constants.proxies))
    print("Session is created")
    try:
        get_secret_value = client.get_secret_value(SecretId=env_constants.secret_arn)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + env_constants.secret_arn + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value:
            text_secret_data = get_secret_value['SecretString']
        else:
            binary_secret_data = get_secret_value['SecretBinary']
        get_secret_value_response = json.loads(text_secret_data)
        secret_cred = get_secret_value_response[env_constants.scrt_fldr]
        x = "{" + secret_cred.replace("'",'"') + "}"
        login_payload = json.loads(x)
        response = requests.request("POST", env_constants.auth_url, headers=env_constants.login_headers, data=login_payload)
        session_id = response.json()['sessionId']
        print(session_id)
    return session_id
    

    
def parse_objectlifecycle_json(headers, payload):
    objlifcyc_url = env_constants.server+"/api/v23.1/configuration/Objectlifecycle"
    objlifcyc_url_response = requests.request("GET", objlifcyc_url, headers=headers, data=payload)
    json_data_objlifcyc_url = objlifcyc_url_response.json()
    json_response = {}
    parse_response = []
    for ele in json_data_objlifcyc_url['data']:
        field_name = ele['name']
        field_label = ele['label']
        for i in ele['states']:
            states_name = i['name']
            states_label = i['label']
            states_status = i['record_status']
            states_active = i['active']
            id = str(field_name) + '-' + str(states_name)
            parse_response.append( {'id': id, 'lifecycle_name': field_name, 'lifecycle_label': field_label, 'states' : states_name, 'label': states_label, 'status': states_status, 'active': states_active} )
    ##Mac add data key to return dictionary to keep consistency
    objectlifecycle_df = pd.DataFrame(parse_response, columns=["id","lifecycle_name","lifecycle_label","states","label","status","active"])
    #json_response['data'] = parse_response
    return objectlifecycle_df


def lambda_handler(event, context):
    print(event)
    #Generating sessionId by connecting with SecretsManager
    try:
        gnrt_session_id = create_session()
        logger.info('sessionId is created')
        
        payload = {}
        headers = {
        'Authorization': gnrt_session_id,
        'Accept': 'application/json'
        }
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        
        #getting data from objectlifecycle
        objlifcyc_df = parse_objectlifecycle_json(headers, payload)
        
        #Writing dataframe to csv
        objlifcyc_df.to_csv(f's3://cld-aws-emea-dev-veevavaultlayer-grdc-dev/grdc-destination/136157_4818311_objectlifecycle_{today}_updates.csv', index = False)
        logger.info('Written Objectlifecycle dataframe to csv')
        logger.info('Lambda function for Objectlifecycle has been successfully completed!')
        
    except botocore.exceptions.ClientError as error:
        logger.error("There was an error copying the file to the destination bucket")
        print('Error Message: {}'.format(error))
        
    except botocore.exceptions.ParamValidationError as error:
        logger.error("Missing required parameters while calling the API.")
        print('Error Message: {}'.format(error))
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function for Objectlifecycle!')
    }