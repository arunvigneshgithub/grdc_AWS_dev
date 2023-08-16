import json
import botocore
import boto3
import os
import logging
import datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')
print("S3 resource is connected")
s3_client = boto3.client('s3')
print("S3 client is connected")

def lambda_handler(event, context):
    print(event)
    logger.info("New files uploaded to the source bucket.")
    destbucket = os.environ['destbucket']
    target_folder_name = os.environ['target_folder_name']
    srcbucket_folder = os.environ['srcbucket_folder']
    srcbucket = s3.Bucket(destbucket)
    print("srcbucket is connected")
    destbucket = s3.Bucket(destbucket)
    dest_files = destbucket.objects.all()
    today = datetime.datetime.now()+datetime.timedelta(days=0)
    today = today.strftime("%Y-%m-%d")
    folder_name = os.path.join(target_folder_name, today)
    archive = s3_client.put_object(Bucket=destbucket.name, Body='', Key=(folder_name+'/'))
    prefix = target_folder_name+today+'/'
    response = s3_client.list_objects(Bucket=destbucket.name, Prefix=prefix)
    try:
        #copying files f rom source to achive folder & deleting once its archived
        for result in srcbucket.objects.filter(Prefix=srcbucket_folder):
            obj = result.key.split('/')[1]
            match = obj.endswith('.csv') or obj.endswith('.txt') or obj.endswith('.py') or obj.endswith('.CSV')
            if match:
                copy_source = {
                            'Bucket': srcbucket.name,
                            #object.key holds the name of the current object. Pass that name to the key
                            'Key': srcbucket_folder+result.key.split('/')[1]
                        }
                destbucket.copy(copy_source, target_folder_name+today+'/'+result.key.split('/')[1])
                print(target_folder_name+today+'/'+result.key.split('/')[1] +'- File Copied')
                logger.info("File copied to the destination bucket successfully!")
                s3.Object(srcbucket.name, srcbucket_folder+result.key.split('/')[1]).delete()
                print('Objects deleted successfully!')
                logger.info("Objects deleted in the destination bucket successfully!")
    except botocore.exceptions.ClientError as error:
        logger.error("There was an error copying the file to the destination bucket")
        print('Error Message: {}'.format(error))
        
    except botocore.exceptions.ParamValidationError as error:
        logger.error("Missing required parameters while calling the API.")
        print('Error Message: {}'.format(error))
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
