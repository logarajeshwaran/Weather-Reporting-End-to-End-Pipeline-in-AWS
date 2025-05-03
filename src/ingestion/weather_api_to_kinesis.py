import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__,'../../..')))
import json
import requests
import time
import random
from utils.kinesis_manager import KinesisManager


class WeatherKinesisPipeline:

    def __init__(self):
        self.api_key = os.environ.get("API_KEY")
        print(self.api_key)
        self.aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        self.aws_secret_key = os.environ.get("AWS_SECRET_KEY")
        self.aws_region = os.environ.get("AWS_REGION")
        self.stream_name = os.environ.get("STREAM_NAME")
        self.shards = os.environ.get("SHARDS")
        self.cities = [  "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    
        self.intialize_kinesis = KinesisManager(aws_access_key=self.aws_access_key,aws_secret_key=self.aws_secret_key,aws_region=self.aws_region)
    
    def fetch_weather_data(self,city):
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.api_key}"
        print(url)
        response = requests.get(url=url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed API call for the city {city} : HTTP {response.status_code}")
            return None
        
    def push_data_to_kinesis(self,data):
        self.intialize_kinesis.create_stream(stream_name=self.stream_name,shards=int(self.shards))
        try:
            self.intialize_kinesis.kinesis_client.put_record(StreamName=self.stream_name,Data=json.dumps(data),PartitionKey='Weather')
            print(f"Psuhed the data into kinesis stream : {self.stream_name}")
        except Exception as e:
            print(f"Error occured as {e}")
    

    def run(self):
        print("Starting Kinesis Pipeline for pushing data from weather API")
        while True:
            city = random.choice(self.cities)
            weather_data = self.fetch_weather_data(city=city)
            if weather_data:
                self.push_data_to_kinesis(weather_data)
                print(f"Pushed Weather Data  for the city {city} to kinesis at {time.time()}")
            time.sleep(2)
