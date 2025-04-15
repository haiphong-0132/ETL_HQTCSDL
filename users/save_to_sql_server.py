import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_UID')};"
        f"PWD={os.getenv('DB_PWD')};"
        f"TrustServerCertificate={os.getenv('TRUST_SERVER_CERTIFICATE')};"
    )

    return conn

user_data = pd.read_csv("users.csv")

conn = get_db_connection()

cursor = conn.cursor()


cursor.execute("DROP TABLE users")
cursor.execute("""
    CREATE TABLE users (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(255),
            username NVARCHAR(255),
            password NVARCHAR(255),
            gender INT,
            email NVARCHAR(255),
            phone NVARCHAR(20),
            address NVARCHAR(255),
            created_at DATETIME,
            status NVARCHAR(255),
            )
""")

cursor.execute("SET IDENTITY_INSERT users ON")

for index, row in tqdm(user_data.iterrows(), total=user_data.shape[0], desc="Inserting records", unit="record"):
    cursor.execute("INSERT INTO Users(id, name, username, password, gender, email, phone, address, created_at, status) values(?,?,?,?,?,?,?,?,?,?)", index, row["name"], row['username'], row['password'], row["gender"], row["email"], row["phone"], row["address"], row['create_at'], row['status'])

conn.commit()
cursor.close()

print("Data inserted successfully.")
            


