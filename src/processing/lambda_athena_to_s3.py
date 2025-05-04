import boto3


athena = boto3.client('athena')
s3_output = 's3://athena-query-results-bucket-20251213'


def lambda_handler(event,context):
    database= 'weather_data'
    query = """  WITH cleaned_data AS (
    SELECT DISTINCT 
        name AS city_name,
        temp AS temperature_kelvin,
        pressure AS pressure_hpa,
        humidity AS humidity_percent,
        wind_speed AS wind_speed_mps,
        country AS country_code
    FROM weather_data.weather_reporting
)

SELECT 
    city_name,
    -- Convert Kelvin to Celsius for more familiar readings
    ROUND(temperature_kelvin - 273.15, 2) AS temperature_celsius,
    pressure_hpa,
    humidity_percent,
    wind_speed_mps,
    -- Categorize temperature
    CASE 
        WHEN temperature_kelvin - 273.15 > 30 THEN 'Hot'
        WHEN temperature_kelvin - 273.15 > 20 THEN 'Warm'
        WHEN temperature_kelvin - 273.15 > 10 THEN 'Mild'
        WHEN temperature_kelvin - 273.15 > 0 THEN 'Cool'
        ELSE 'Cold'
    END AS temperature_category,
    -- Categorize wind speed
    CASE 
        WHEN wind_speed_mps > 10 THEN 'Very Windy'
        WHEN wind_speed_mps > 5 THEN 'Windy'
        WHEN wind_speed_mps > 3 THEN 'Breezy'
        ELSE 'Calm'
    END AS wind_category
FROM cleaned_data
ORDER BY temperature_kelvin DESC """

    response  =  athena.start_query_execution(QueryString=query,QueryExecutionContext={'Database': database},ResultConfiguration={'OutputLocation':s3_output})

    query_execution_id = response['QueryExecutionId']

    return {"staus_code" : 200 , "body" : "Athena quey executed"}
