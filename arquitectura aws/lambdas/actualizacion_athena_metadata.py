import boto3

def lambda_handler(event, context):
    '''
    Funcion que sirve para agregar a la tabla athena existente de metadata, nueva data,
    esta funcion es disparada por un evento en el bucket.
    '''
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='MSCK REPAIR TABLE metadata',
        QueryExecutionContext={
            'Database': 'metadata-database'
        },
        ResultConfiguration={
            'OutputLocation': 's3://save-athena-query/'
        }
    )
    return response