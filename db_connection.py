import mysql.connector

def get_db_connection():
    """Establishes a connection to the database."""
    try:
        # Using same credentials as unlimit_cb_fight_system/database.py
        conn = mysql.connector.connect(
            user='jose.collins.replica',
            password='Bhc7}FxgT9_Y*8@',
            host='live-replica.cpi0yoqzrjqz.us-west-1.rds.amazonaws.com',
            port='3306',
            database='saldogra_gamma', # Defaulting to saldogra_gamma as it seems main one
            use_pure=True,
            ssl_disabled=True,
            charset='utf8mb4'
        )
        conn.autocommit = True
        print("Connected to database!")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None
