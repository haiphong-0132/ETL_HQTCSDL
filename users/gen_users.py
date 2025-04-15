import pyodbc
import os
from dotenv import load_dotenv
import pandas as pd
import random
import string
import re
from vn_fullname_generator import generator
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

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

# Account: username, name, password, email, phone, address, status, create_at

def normalize_name(name):
    vietnamese_chars = {
        'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
        'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
        'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
        'đ': 'd',
        'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
        'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
        'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
        'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
        'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
        'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
        'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
        'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
        'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'
    }

    name = name.lower()
    for vn_char, latin_char in vietnamese_chars.items():
        name = name.replace(vn_char, latin_char)

    name = re.sub(r'[^a-z ]','', name)

    return name

def generate_email(name):
    normalized = normalize_name(name)
    words = normalized.split()

    email_format = random.choice([
        f'{words[-1]}.{words[0]}',
        f'{words[0]}.{words[-1]}',
        f'{words[0]}{words[-1]}',
        f'{words[-1]}{words[0]}',
        f'{words[0][0]}{words[-1]}',
        f'{words[-1]}{words[0][0]}',
        f'{words[0]}{random.randint(1, 999)}',
        f'{words[-1]}{random.randint(1, 999)}',
        f'{words[0]}_{random.randint(1, 999)}',
        f'{words[-1]}_{random.randint(1, 999)}',
    ])

    if random.random() < 0.3:
        email_format += str(random.randint(1,999))
    
    domain = random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com', 'gdscptit.dev', 'ptit.edu.vn', 'stu.ptit.edu.vn'])

    return f'{email_format}@{domain}'

def generate_phone_number():
    prefixes = ['032', '033', '034', '035', '036', '037', '038', '039', '096', '097', '098', '086',
                '088', '091', '094', '081', '082', '083', '084', '085',
                '070', '079', '077', '076', '078', '090', '092', '089'
                ]
    
    prefix = random.choice(prefixes)

    remaining = ''.join(str(random.randint(0,9)) for _ in range(7))

    return prefix + remaining

def generate_random_string(length=8):
    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choices(chars, k=length))

def generate_random_date(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    random_days = random.randint(0, (end_date - start_date).days)

    random_date = start_date + timedelta(days=random_days)

    return random_date.strftime("%Y-%m-%d")

def generate_random_user_status():
    status = ['active', 'inactive', 'banned']
    return random.choice(status)

def fetch_addresses():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM address", conn)
    conn.close()
    return df

def generate_random_address(address_df):
    row = address_df.sample(1).iloc[0]
    ward = row['wards']
    district = row['districts']
    province = row['provinces']

    return f"{ward}, {district}, {province}"

def generate_single_user(address_df):
    try:
        gender = random.randint(0, 1)
        name = generator.generate(gender)
        email = generate_email(name)
        phone = generate_phone_number()
        status = generate_random_user_status()
        username = generate_random_string(random.randint(5, 10))
        password = generate_random_string( random.randint(8, 12))
        create_at = generate_random_date("2023-01-01", datetime.now().strftime("%Y-%m-%d"))
        address = generate_random_address(address_df)

        return (name, username, password, gender, email, phone, address, create_at, status)
    except Exception as e:
        print('Error generating user:', e)
        return None

def generate_users(n, address_df, max_workers=10):
    data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(generate_single_user, address_df) for _ in range(n)]
        for f in tqdm(as_completed(futures), total=n, desc="Generating user data", unit="record"):
            result = f.result()
            if result:
                data.append(result)

    return pd.DataFrame(data, columns=["name", "username", "password", "gender", "email", "phone", "address", "create_at", "status"])

def save_to_csv(df, filename='users.csv'):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f'CSV file: {filename} | Number of records: {len(df)}')
    print(df.head())

if __name__ == "__main__":
    NUM_RECORDS = 10**6
    MAX_THREADS = 10
    print('Fetching address data...')
    address_df = fetch_addresses()

    print("Generating user data...")
    user_data = generate_users(NUM_RECORDS, address_df, max_workers=MAX_THREADS)

    print("User data generation completed.")
    print("Saving to CSV...")

    save_to_csv(user_data, filename='users.csv')

    print("CSV file saved successfully.")
    print("Done.")