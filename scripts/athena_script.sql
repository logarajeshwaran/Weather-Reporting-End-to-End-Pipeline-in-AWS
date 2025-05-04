create database weather_data;

create external table weather_data.weather_reporting
(
name STRING,
temp double,
pressure INt,
humidity INT,
wind_speed double,
country STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
 WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION
  's3://my-kinesis-to-s3-bucket/kinesis_data_20250505/04/';