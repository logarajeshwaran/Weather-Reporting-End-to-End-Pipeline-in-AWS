from mysql.connector import Error

# Create a new database
def create_database(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        print(f"[INFO] Database '{query}' created or already exists.")
    except Error as e:
        print(f"[ERROR] Failed to create database {query}: {e}")
    finally:
        cursor.close()

# Drop an existing database
def drop_database(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        print(f"[INFO] Database '{query}' dropped if it existed.")
    except Error as e:
        print(f"[ERROR] Failed to drop database {query}: {e}")
    finally:
        cursor.close()

# Create a table given a CREATE TABLE SQL statement
def create_table(connection, query,database):
    try:
        cursor = connection.cursor()
        cursor.execute(f"USE {database}")
        cursor.execute(query)
        print("[INFO] Table created successfully.")
    except Error as e:
        print(f"[ERROR] Failed to create table: {e}")
    finally:
        cursor.close()

# Drop a table by name
def drop_table(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        print(f"[INFO] Table '{query}' dropped if it existed.")
    except Error as e:
        print(f"[ERROR] Failed to drop table {query}: {e}")
    finally:
        cursor.close()

# Insert data given an INSERT SQL and data tuple/list
def insert_data(connection, insert_sql, data,database):
    try:
        cursor = connection.cursor()
        cursor.execute(f"USE {database}")
        cursor.executemany(insert_sql, data)
        connection.commit()
        print(f"[INFO] Inserted {cursor.rowcount} rows.")
    except Error as e:
        print(f"[ERROR] Failed to insert data: {e}")
        connection.rollback()
    finally:
        cursor.close()

def insert_into_select(source_db,target_db,source_table,target_table,source_database,target_database,columns):
    source_cursor = source_db.cursor()
    target_cursor = target_db.cursor()
    select_query = f"SELECT {columns} FROM {source_database}.{source_table}"
    print(select_query)
    source_cursor.execute(select_query)
    rows = source_cursor.fetchall()
    print(f"Fetched {len(rows)} rows from the local table.")
    target_cursor.executemany(f"INSERT INTO {target_database}.{target_table} ({columns}) VALUES ({', '.join(['%s'] * len(columns.split(',')))})", rows)
    target_db.commit()
    print(f"Transferred {target_cursor.rowcount} rows from `{source_table}` to `{target_table}`.")
    source_cursor.close()
    target_cursor.close()
    pass
