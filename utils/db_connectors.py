import boto3
import mysql.connector
from mysql.connector import Error



def get_rds_end_point(instance_identifier,region_name):
    try:
        rds_client = boto3.client('rds',region_name=region_name)
        response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_identifier)
        endpoint = response['DBInstances'][0]['Endpoint']['Address']
        return endpoint
    except Exception as e:
        print(f"Failed to fetch endpoint {e}")
        return None
    

def get_mysql_instance(host,port,username,password):
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password
        )
        if conn.is_connected():
            print(f" Connected to local MySQL at {host}:{port}")
            return conn
    except Error as e:
        print(f"[ERROR] MySQL connection failed: {e}")
        return None

def connect_rds(instance_identifier,region,user,password,port=3306):
    try:
        endpoint = get_rds_end_point(instance_identifier=instance_identifier,region_name=region)
        if not endpoint:
            return None
        conn = mysql.connector.connect(host=endpoint,user=user,passwods=password,port=port)
        if conn.is_connected():
            print(f" Connected to local MySQL at {endpoint}:{port}")
            return conn
    except Error as e:
        print(f"[ERROR] RDS connection failed: {e}")
        return None

def close_connection(connection):
    try:
        if connection and connection.is_connected():
            connection.close()
            print(f"Connection Closed Successfully")
    except Exception as e:
        print(f"Cloing connection failed: {e}")
