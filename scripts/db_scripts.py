CREATE_DATABASE_QUERY="CREATE DATABASE IF NOT EXISTS {0}"
DROP_DATA_BASE_QUERY="DROP DATABASE IF EXISTS {0}"
CREATE_TABLE_QUERY="""CREATE TABLE IF NOT EXISTS {0} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            humidity INT,
            wind_speed INT,
            weather_condition VARCHAR(100),
            description VARCHAR(100),
            timestamp DATETIME
        )"""
DROP_TABLE_QUERY = "DROP TABLE IF EXISTS {0}"

INSERT_QUERY = """
        INSERT INTO {0} (city, temperature, humidity, wind_speed,weather_condition,description, timestamp)
        VALUES (%s, %s, %s, %s, %s,%s,%s)
        """
