import json
import boto3


def lambda_handler(event, context):
    '''
    Funcion para extraer la data del evento de inputacion de un objeto en el bucket
    '''
    print(event)
    message_body = json.loads(event['Records'][0]['body'])
    print(message_body)
    file_name = message_body['Records'][0]['s3']['object']['key']  # Separamos el nombre del archivo con la ruta
    bucketName = message_body['Records'][0]['s3']['bucket']['name']  # Separamos el nombre del bucket

    file_name = file_name.replace("+(1)", "") # eliminamos el "+(1)" del nombre del archivo
    print("File Name : ",file_name)
    print("Bucket Name : ",bucketName)
    glue = boto3.client('glue',
                        aws_access_key_id='AKIAXRYNTY2XL6GE3J75',
                        aws_secret_access_key='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',
                        region_name='us-east-1')
    
    if "metadata" in file_name: # Si viene del bucket de metadata activara un etl para metadata                
        response = glue.start_job_run(JobName="etl-raw-metadata", Arguments={"--VAL1":file_name,"--VAL2":bucketName})
        print("Lambda Invoke ")
    else:                       # Si viene del bucket de reviews activara un etl para reviews
        response = glue.start_job_run(JobName="etl-raw-reviews", Arguments={"--VAL1":file_name,"--VAL2":bucketName})
        print("Lambda Invoke ")