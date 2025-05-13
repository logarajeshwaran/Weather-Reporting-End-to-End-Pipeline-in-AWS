import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__, '../../..')))
import json
from dotenv import load_dotenv
from mock_weather_data import generate_weather_data
from utils.decrtpy_file import EncryptedConfigManager
from utils.db_connectors import get_mysql_instance, connect_rds, close_connection
from utils.db_operators import create_database, create_table, insert_data, insert_into_select
import scripts.db_scripts as db


load_dotenv()

class WeatherDataPipeline:
    def __init__(self):
        self.out_path = os.path.join(os.path.abspath(os.path.join(__file__, '../../..')), 'data', 'weather_data.json')
        self.decrypt = EncryptedConfigManager()
        self.decrypt_data = self.decrypt.decrypt_config()
        self.db_config = self.decrypt_data.split('|')
        self.local_conn = None
        self.rds_connection = None
        self.aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        self.aws_secret_key = os.environ.get("AWS_SECRET_KEY")

    def generate_data(self, num_records=1500):
        generate_weather_data(num_records, output_file=self.out_path)

    def setup_local_db(self):
        print("Setting up local database...")
        try:
            self.local_conn = get_mysql_instance(
                username=self.db_config[0].split('=')[1],
                password=self.db_config[1].split('=')[1],
                host='localhost',
                port=3306
            )
            print(self.local_conn)
            create_database(self.local_conn, query=db.CREATE_DATABASE_QUERY.format('weather_data'))
            create_table(self.local_conn, query=db.CREATE_TABLE_QUERY.format('weather_reading'), database='weather_data')
        except Exception as e:
            print(f"Error setting up local database: {e}")
            
    def insert_local_data(self):
        with open(self.out_path) as f:
            weather_data = json.load(f)
        columns = ",".join(weather_data[0].keys())
        values = [tuple(records.values()) for records in weather_data]
        insert_data(
            connection=self.local_conn,
            insert_sql=db.INSERT_QUERY.format('weather_reading'),
            database='weather_data',
            data=values
        )
        return columns

    def setup_rds_db(self):
        self.rds_connection = connect_rds(
            instance_identifier='database-1',
            user='admin',
            password='weather_data_pipeline!',
            region='us-east-1',
            aws_access_key=self.aws_access_key,
            aws_secret_key=self.aws_secret_key
        )
        create_database(self.rds_connection, query=db.CREATE_DATABASE_QUERY.format('weather_data'))
        create_table(self.rds_connection, query=db.CREATE_TABLE_QUERY.format('weather_reading'), database='weather_data')

    def transfer_data_to_rds(self, columns):
        insert_into_select(
            source_db=self.local_conn,
            target_db=self.rds_connection,
            source_table='weather_reading',
            target_table='weather_reading',
            columns=columns,
            source_database='weather_data',
            target_database='weather_data'
        )

    def run_pipeline(self):
        self.generate_data()
        self.setup_local_db()
        columns = self.insert_local_data()
        self.setup_rds_db()
        self.transfer_data_to_rds(columns)
        # Close connections if needed
        if self.local_conn:
            close_connection(self.local_conn)
        if self.rds_connection:
            close_connection(self.rds_connection)

if __name__ == "__main__":
    pipeline = WeatherDataPipeline()
    pipeline.run_pipeline()
