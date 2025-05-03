import sys
import os 
sys.path.append(os.path.abspath(os.path.join(__file__,'../../..')))
from dotenv import load_dotenv
from utils.s3_manager import S3Manager
from mock_weather_data import generate_weather_data


load_dotenv()
output_path = os.path.join(os.path.abspath(os.path.join(__file__,'../../../data')),'data') + 'weather_data.json'
print(output_path)
class WeatherDataManager:

    def __init__(self):
        self.output_path = output_path
        self.aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        self.aws_secret_key = os.environ.get("AWS_SECRET_KEY")
        self.aws_region = os.environ.get("AWS_REGION")
        self.bukcet_name = os.environ.get("WEATHERDATAINPUTBUCKET")
    
    def generate_file(self):
        generate_weather_data(1000,output_file=self.output_path)
        if os.path.exists(self.output_path) and os.path.getsize(self.output_path) > 0:
            s3_client = S3Manager(aws_access_key=self.aws_access_key,aws_secret_key=self.aws_secret_key,aws_region=self.aws_region)
            s3_client.create_bucket(bucket_name=self.bukcet_name)
            s3_client.s3_clinet.upload_file(self.output_path, self.bukcet_name, f"input_files/{self.output_path}")
            print(f"Upload {self.output_path} to S3 Bucket {self.bukcet_name}")


if __name__ == '__main__':
    weather_data = WeatherDataManager()
    weather_data.generate_file()