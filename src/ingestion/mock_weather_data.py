from faker import Faker
import json 
import random



class MockWeatherData:

    def __init__(self):
        self.fake = Faker()

    def generate_weather_data(self,city):

        weather_conditions = ["Clear", "Cloudy", "Rain", "Snow", "Sunny", "Windy", "Thunderstorm"]
        tempratures = {"min" : random.uniform(-10,15) , "max" : random.uniform(16,40)}
        temprature = round(random.uniform(tempratures["min"],tempratures["max"]),1)
        humidity = random.randint(1,100)
        wind_speed = round(random.uniform(0.5,15.0),1)
        weather_condition = random.choice(weather_conditions)

        return {"city": city,
            "temperature": temprature,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "weather_condition": weather_condition,
            "description": self.fake.sentence(nb_words=6),
            "timestamp": self.fake.iso8601()}


def generate_weather_data(record_count,output_file):
    cities = [  "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    
    mockweatherdata = MockWeatherData()
    data = []

    for _ in range(record_count):
        city = random.choice(cities)
        weather_data = mockweatherdata.generate_weather_data(city=city)
        data.append(weather_data)

    if output_file:
        with open(output_file,"w") as file:
            json.dump(data,file,indent=4)
        print(f"Generated {record_count} mock weather records and saved to '{output_file}'.") 