import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import NoCredentialsError,PartialCredentialsError


class KinesisManager:

    def __init__(self,aws_access_key=None,aws_secret_key=None,aws_region="us-east-1"):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region
        
        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS Credentials are not set in the environment varialbes")
        
        self.kinesis_client = boto3.client('kinesis',aws_access_key_id=self.aws_access_key,aws_secret_access_key=self.aws_secret_key,region_name=self.aws_region)

    def create_stream(self,stream_name,shards):
        try:
            response = self.kinesis_client.create_stream(StreamName=stream_name,ShardCount=shards)
            print(f"Stream {stream_name} is created with the {shards} shards")
            print(f"Response : {response}")

            waiter = self.kinesis_client.get_waiter("stream_exists")
            waiter.wait(StreamName=stream_name)
            print("Stream is Now Active")
        
        except self.kinesis_client.exceptions.ResourceInUseException:
            print(f"Stream '{stream_name}' already exists.")
        except NoCredentialsError:
            print("No Credenatials present in the environment file")
        except PartialCredentialsError:
            print("Incomplete AWS Credentials")


if __name__ == '__main__':
    load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    aws_region = os.getenv("AWS_REGION")
    kinesis_clinet = KinesisManager(aws_access_key=aws_access_key,aws_secret_key=aws_secret_key,aws_region=aws_region).create_stream(stream_name='weather_data',shards=1)