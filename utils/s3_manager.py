import boto3
from botocore.exceptions import NoCredentialsError,PartialCredentialsError
import os
from dotenv import load_dotenv

class S3Manager:

    def __init__(self,aws_access_key=None,aws_secret_key=None,aws_region="us-east-1"):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region
        
        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS Credentials are not set in the environment varialbes")
        
        self.s3_clinet = boto3.client('s3',aws_access_key_id=self.aws_access_key,aws_secret_access_key=self.aws_secret_key,region_name=self.aws_region)

    def create_bucket(self,bucket_name):
        try:
            if self.aws_region == "us-east-1":
                response = self.s3_clinet.create_bucket(Bucket=bucket_name)
            else:
                response = self.s3_clinet.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': self.aws_region })

            print(f"Bucket {bucket_name} is being created")
            print(f"Response : {response}")

        except self.s3_clinet.exceptions.BucketAlreadyExists as err:
            print("Bucket {} already exists!".format(err.response['Error']['BucketName']))
        except NoCredentialsError:
            print("No Credenatials present in the environment file")
        except PartialCredentialsError:
            print("Incomplete AWS Credentials")
