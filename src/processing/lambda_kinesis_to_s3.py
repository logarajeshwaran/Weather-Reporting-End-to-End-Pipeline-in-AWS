import zipfile
import os
import boto3
import json
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class KinesisToS3LambdaDeployer:
    def __init__(self,
                 aws_access_key=None,
                 aws_secret_key=None,
                 aws_region=None,
                 function_name="KinesisToS3Writer",
                 role_name="KinesisToS3LambdaRole",
                 bucket_name='kinesis-s3-output-bucket-20251521510',
                 kinesis_arn="arn:aws:kinesis:us-east-1:180294190881:stream/weather_data"):
        self.aws_access_key = aws_access_key or os.environ.get("AWS_ACCESS_KEY")
        self.aws_secret_key = aws_secret_key or os.environ.get("AWS_SECRET_KEY")
        self.aws_region = aws_region or os.environ.get("AWS_REGION")
        self.function_name = function_name
        self.role_name = role_name
        self.bucket_name = bucket_name
        self.kinesis_arn = kinesis_arn

        self.iam = boto3.client('iam',
                                aws_access_key_id=self.aws_access_key,
                                aws_secret_access_key=self.aws_secret_key,
                                region_name=self.aws_region)
        self.lambda_client = boto3.client('lambda',
                                          aws_access_key_id=self.aws_access_key,
                                          aws_secret_access_key=self.aws_secret_key,
                                          region_name=self.aws_region)

        self.assume_role_policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }]
        })

    def create_iam_role(self):
        try:
            response = self.iam.get_role(RoleName=self.role_name)
            print(f"Role already exists: {response['Role']['Arn']}")
            return response['Role']['Arn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print(f"Creating role: {self.role_name}")
                create_response = self.iam.create_role(
                    RoleName=self.role_name,
                    AssumeRolePolicyDocument=self.assume_role_policy_document,
                    Description='Lambda role to process Kinesis records and write to S3',
                )
                role_arn = create_response['Role']['Arn']
                print(f"Created role: {role_arn}")

                self.iam.attach_role_policy(
                    RoleName=self.role_name,
                    PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                )
                self.iam.attach_role_policy(
                    RoleName=self.role_name,
                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
                )
                self.iam.attach_role_policy(
                    RoleName=self.role_name,
                    PolicyArn="arn:aws:iam::aws:policy/AmazonKinesisFullAccess"
                )
                return role_arn
            else:
                raise

    def write_lambda_code(self):
        lambda_code = f'''import boto3
import json
import base64
import uuid
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = '{self.bucket_name}'

def lambda_handler(event, context):

    records = []

    for record in event['Records']:

        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {{"raw" : payload}}

        records.append(data)

    json_data = '\\n'.join([json.dumps(r) for r in records])

    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    object_name = f'kinesis_data_{{timestamp}}_{{uuid.uuid4()}}.jsonl'

    s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data.encode('utf-8'))

    return {{"status_code": 200, "body": "file written to bucket successfully"}}
'''
        with open('lambda_function.py', 'w') as f:
            f.write(lambda_code)
        print("Lambda code file created.")

    def zip_lambda_function(self):
        with zipfile.ZipFile('lambda.zip', 'w') as zipf:
            zipf.write('lambda_function.py')
        print("Lambda function zipped.")

    def deploy_lambda_function(self, role_arn):
        with open('lambda.zip', 'rb') as f:
            zipped_code = f.read()

        try:
            self.lambda_client.get_function(FunctionName=self.function_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                pass
            else:
                raise

        try:
            self.lambda_client.delete_function(FunctionName=self.function_name)
            print(f"[!] Deleted existing Lambda function: {self.function_name}")
            time.sleep(3)  # Short delay to ensure deletion propagation
        except ClientError as e:
            print(f"[x] Error deleting function: {e}")

        response = self.lambda_client.create_function(
            FunctionName=self.function_name,
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zipped_code},
            Timeout=30,
            MemorySize=256,
            Publish=True
        )
        lambda_arn = response['FunctionArn']
        print("Lambda function created:", lambda_arn)
        return lambda_arn

    def setup_event_source_mapping(self):
        response = self.lambda_client.list_event_source_mappings(
            FunctionName=self.function_name,
            EventSourceArn=self.kinesis_arn
        )

        for mapping in response['EventSourceMappings']:
            self.lambda_client.delete_event_source_mapping(UUID=mapping['UUID'])
            print(f"Deleted mapping: {mapping['UUID']}")

        mapping_response = self.lambda_client.create_event_source_mapping(
            EventSourceArn=self.kinesis_arn,
            FunctionName=self.function_name,
            Enabled=True,
            BatchSize=100,
            StartingPosition="LATEST"
        )
        print(f"Recreated event source mapping: {mapping_response['UUID']}")

    def run(self):
        role_arn = self.create_iam_role()
        print("IAM Role created:", role_arn)
        # Wait for propagation
        time.sleep(20)
        self.write_lambda_code()
        self.zip_lambda_function()
        self.deploy_lambda_function(role_arn)
        self.setup_event_source_mapping()


if __name__ == "__main__":
    deployer = KinesisToS3LambdaDeployer()
    deployer.run()
