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
        res = requests.request("POST", env_constants.auth_url, headers=env_constants.login_headers, data=login_payload)
        session_id = res.json()['sessionId']
        #print(session_id)
    return session_id

def get_picklist(headers, payload):
    curr_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    pklst_url = env_constants.server+"/api/v23.1/objects/picklists"
    pklst_res = requests.request("GET", pklst_url, headers=headers, data=payload)
    pklst_data = pklst_res.json()
    picklist = pklst_data['picklists']
    #print(picklist)
    lists = []
    picklist_picklist = []
    #print(lists)

    for eachpicklist in picklist:
        if eachpicklist.get('usedIn') == None:
            picklist_picklist.append(eachpicklist.get('name'))
        else:
            for listvalues in eachpicklist.get('usedIn'):
                if listvalues.get('objectName') == None:
                    picklist_picklist.append(eachpicklist.get('name'))
                else:
                    addtuple = (eachpicklist.get('name'),listvalues.get('objectName'),listvalues.get('propertyName') ,  'new' , curr_time )
                    #print('addtuple : ', addtuple)
                    lists.append(addtuple)
                    
    #picklist not contains either usedIn or objectname
    pk_df = pd.DataFrame(picklist_picklist).drop_duplicates(keep='first')
    
    #dataframe for lists
    df = pd.DataFrame(lists, columns= ("picklist_name" , "object_name" , "object_fieldname", "load_status" , "refresh_dt" ))
    
    #concatenate only picklist_name from pk_df & df["picklist_name"]
    concat_df = pd.concat([df["picklist_name"], pk_df]).drop_duplicates(keep='first')
    concat_df['picklist_name'] = concat_df
    print(concat_df)
    
    return df, concat_df
        
        
def get_name_label(df, headers, payload):
    #df = df.drop_duplicates(['picklist_name'],keep='first')
    print(df)
    list_consolidated_picklist_json = []
    json_response = {}
    for i in df['picklist_name']:
        
        url = (f"{env_constants.server}/api/v23.1/objects/picklists/{i}")
        response = requests.request("GET", url, headers=headers, data=payload)
        response = response.json()
        picklist_name = i
        json_payload = response
        
        try:
            
            if json_payload['picklistValues']:
                
                curr_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                for ele in json_payload['picklistValues']:
                    picklist_name = picklist_name
                    name = ele['name']
                    label = ele['label']   
                    id = str(picklist_name) + '-' + str(name)
                    ####object_name = ele['usedIn']['objectName']
                    list_consolidated_picklist_json.append( {'id': id, 'picklist_name': picklist_name, 'name': name, 'label': label})
            else:
                print(f'There is no name & label in {picklist_name}')
        except:
            print(f'{picklist_name} doesnt have object & key value')
    value_df = pd.DataFrame(list_consolidated_picklist_json, columns=["id","picklist_name","name","label"])
    #print(f'value_df is {value_df}')
    return value_df
    
def get_metadata_vobject(session_id):
    
    curr_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    metadata_url = env_constants.server+"/api/v23.1/metadata/vobjects"
    metadata_headers = {
        'Authorization': session_id,
        'Accept': 'application/json'
        }
        
    metadata_response = requests.request("GET", metadata_url, headers=metadata_headers, data=env_constants.metadata_payload)
    json_data_metadata = metadata_response.json()
    #print(json_data_metadata)
    lookup_list = []
    for response in json_data_metadata['objects']:
        print(response['url'])
        obj_url = env_constants.server+response['url']
        obj_headers = {
        'Authorization': session_id,
        'Accept': 'application/json'
        }
        
        obj_response = requests.request("GET", obj_url, headers=obj_headers, data=env_constants.obj_payload)
        obj_response_json = obj_response.json()
        
        
        for field in obj_response_json['object']['fields']:
            try:
                if (field['type'] == 'Picklist') and (field['searchable'] == True):
                    lookup_list.append({"picklist_name" : field['picklist'], "object_name" : response['url'].split('/')[5], "object_fieldname" : field['name'],  'load_status' : 'searchable', 'refresh_dt' : curr_time})
            except:
                print(f'Either type is not picklist or searchable is False for this field')
    lookup_list_df = pd.DataFrame(lookup_list, columns=("picklist_name" , "object_name" , "object_fieldname", "load_status" , "refresh_dt"))
    return lookup_list_df
    

def lambda_handler(event, context):
    print(event)
    print('--------------Code Start-----------------------')
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    #Retrieving credentials from Secret Manager
    gnrt_session_id = create_session()
    print(gnrt_session_id)
    
    payload = {}
    headers = {
    'Authorization': gnrt_session_id,
    'Accept': 'application/json'
    }

    #getting picklist value & converting that to dataframe
    try:
        pklst_df = get_picklist(headers, payload)
        
        #picklists
        value_df = get_name_label(pklst_df[1], headers, payload)
        
        lookup_list = get_metadata_vobject(gnrt_session_id)
        #picklist_mappings
        combined_df = pd.concat([pklst_df[0],lookup_list])
        
        #Writing dataframe to csv
        
        value_df.to_csv(f's3://cld-aws-emea-dev-veevavaultlayer-grdc-dev/grdc-destination/136157_4818311_picklist_{today}_updates.csv', index = False)
        logger.info('Written picklist dataframe to csv')
        combined_df.to_csv(f's3://cld-aws-emea-dev-veevavaultlayer-grdc-dev/grdc-destination/136157_4818311_picklist_mappings_{today}_updates.csv', index = False)
        logger.info('Written combined picklist_mappings & lookup dataframe to csv')
        logger.info('Successfully loaded the CSV files in S3')
        
    except botocore.exceptions.ClientError as error:
        logger.error("There was an error copying the file to the destination bucket")
        print('Error Message: {}'.format(error))
        
    except botocore.exceptions.ParamValidationError as error:
        logger.error("Missing required parameters while calling the API.")
        print('Error Message: {}'.format(error))
    print('----------------Code Ends ---------------------')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Picklist & Picklist_mapping!')
    }
