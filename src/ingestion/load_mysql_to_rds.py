import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__,'../../..')))
import json
from mock_weather_data import generate_weather_data
from utils.decrtpy_file import EncryptedConfigManager
from utils.db_connectors import get_mysql_instance,get_rds_end_point,connect_rds,close_connection
from utils.db_operators import create_database,create_table,drop_database,drop_table,insert_data,insert_into_select
import scripts.db_scripts as db
from dotenv import load_dotenv
load_dotenv()

out_path = os.path.join(os.path.abspath(os.path.join(__file__,'../../..')),'data') + 'weather_data.json'
generate_weather_data(1500,output_file=out_path)
decrypt = EncryptedConfigManager()
decrypt_data = decrypt.decrypt_config()
db_confif = decrypt_data.split('\n')
conn = get_mysql_instance(username=db_confif[0].split('=')[1],password=db_confif[1].split('=')[1],host='localhost',port=3306)
create_database(conn,query=db.CREATE_DATABASE_QUERY.format('weather_data'))
create_table(conn,query=db.CREATE_TABLE_QUERY.format('weather_reading'),database='weather_data')
with open(out_path) as f:
    weather_data = json.load(f)
columns = ",".join(weather_data[0].keys())
values = [tuple(records.values()) for records in weather_data]
insert_data(connection=conn,insert_sql=db.INSERT_QUERY.format('weather_reading'),database='weather_data',data=values)
rds_end_point = get_rds_end_point(instance_identifier='database-1',region_name='us-east-1',aws_access_key=os.environ.get("AWS_ACCESS_KEY"),aws_secret_key=os.environ.get("AWS_SECRET_KEY"))
rds_connection = connect_rds(instance_identifier='database-1',user='admin',password='weather_data_pipeline!',region='us-east-1',aws_access_key=os.environ.get("AWS_ACCESS_KEY"),aws_secret_key=os.environ.get("AWS_SECRET_KEY"))
create_database(rds_connection,query=db.CREATE_DATABASE_QUERY.format('weather_data'))
create_table(rds_connection,query=db.CREATE_TABLE_QUERY.format('weather_reading'),database='weather_data')
insert_into_select(source_db=conn,target_db=rds_connection,source_table='weather_reading',target_table='weather_reading',columns=columns,source_database='weather_data',target_database='weather_data')

