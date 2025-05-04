import boto3
import json
import base64
import uuid
from datetime import datetime


s3 = boto3.client('s3')
bucket_name = 'kinesis_s3_output_bucket'

def lambda_handler(evnet,context):

    records = []

    for record in evnet['Records']:

        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {"raw" : payload}

        records.append(data)

    json_data = '\n'.join([json.dumps(r) for r in records])


    timestamp = datetime.utcnow().strftime('%Y%m%D%H%m%S')
    object_name = f'kinesis_data_{timestamp}_{uuid.uuid4()}.jsonl'


    s3.put_object(Bucket=bucket_name,Key=object_name,Body=json_data.encode('utf-8'))


    return {  "status_code" :  200  , "body" : "file written to bucket successfully"}    