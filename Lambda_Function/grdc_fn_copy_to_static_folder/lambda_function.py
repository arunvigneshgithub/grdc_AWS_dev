import boto3
import botocore
import json
import os
import logging
import datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')
print('s3 resource is connected')
s3_client = boto3.client('s3')
print('s3 client is connected')

def lambda_handler(event, context):
    print(event)
    logger.info("New files uploaded to the source bucket.")
    srcbucket = os.environ['srcbucket']
    destbucket = os.environ['destbucket']
    src_subfldr_path = os.environ['src_subfldr_path']
    dest_subfldr_path = os.environ['dest_subfldr_path']

    #source bucket
    srcbucket = s3.Bucket(srcbucket)
    #destination bucket
    destbucket = s3.Bucket(destbucket)
    #iterating destination bucket's object key to find the match for destination folder
    for dest_obj in destbucket.objects.all():
        print(dest_obj.key)
        if dest_obj.key == 'grdc-destination/':
            obj_key = 'grdc-destination/'
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    try:
        #iterating source bucket's folder to find the objects which needs to be copied
        for src_obj in srcbucket.objects.filter(Prefix=src_subfldr_path):
            if today in src_obj.key.split('/')[2]:
                #getting tmp files from source bucket and storing it in copy_source
                copy_source = {
                    'Bucket':srcbucket.name,
                    'Key':src_subfldr_path+src_obj.key.split('/')[2]+'/'+src_obj.key.split('/')[3]
                }
                #copying the files to the destination folder using object key
                destbucket.copy(copy_source, dest_subfldr_path+src_obj.key.split('/')[3])
                logger.info("File copied to the destination bucket successfully!")
    except botocore.exceptions.ClientError as error:
        logger.error("There was an error copying the file to the destination bucket")
        print('Error Message: {}'.format(error))
        
    except botocore.exceptions.ParamValidationError as error:
        logger.error("Missing required parameters while calling the API.")
        print('Error Message: {}'.format(error))
