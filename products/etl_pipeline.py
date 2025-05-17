import os
import re
import random
import pyodbc
import hashlib
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime, timedelta
from prefect import flow, task

load_dotenv()

RANDOM_SEED = 42
DB_NAME = os.getenv("DB_NAME")

def get_db_connection(DB_NAME:str):
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={DB_NAME};"
        f"UID={os.getenv('DB_UID')};"
        f"PWD={os.getenv('DB_PWD')};"
        f"TrustServerCertificate={os.getenv('TRUST_SERVER_CERTIFICATE')};"
    )
    
    return conn

def get_md5_hash(row: pd.Series):
    str_row = row.astype(str).str.cat(sep=' ')
    hash_object = hashlib.md5()
    hash_object.update(str_row.encode('utf-8'))
    return hash_object.hexdigest()
    

def toPascalCase(x: str):
    return ' '.join(word.capitalize() for word in x.lower().split())

def normalize_vietnamese_string(s: str):
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

    s = s.lower()

    for vn_char, latin_char in vietnamese_chars.items():
        s = s.replace(vn_char, latin_char)
    
    s = re.sub(r'[^a-z ]','', s)

    return s

def rename_attribute(attribute: str, value: str):
    attribute = attribute.lower()
    value = value.lower()

    if any(attr in attribute for attr in ['màu', 'colour', 'color']) \
        and all(not char.isdigit() for char in value):
        return 'Màu'
    
    if any(attr in attribute for attr in ['dung lượng', 'ram', 'memory', 'storage']):
        return 'Dung lượng'
    
    if any(attr in attribute for attr in ['model', 'model camera', 'lựa chọn mẫu', 'mẫu']):
        return 'Model'
    
    if any(attr in attribute for attr in ['độ phân giải', 'phân giải', 'resolution']):
        return 'Độ phân giải'
    
    if any(attr in attribute for attr in ['công suất', 'power']):
        return 'Công suất'

    if any(attr in attribute for attr in ['bảo hành', 'warranty']):
        return 'Bảo hành'
    
    if any(attr in attribute for attr in ['chip', 'cpu', 'vi xử lý', 'processor']):
        return 'Chip'
    
    if any(attr in attribute for attr in ['hệ điều hành', 'os', 'operating system', 'win']):
        return 'Hệ điều hành'
    
    if any(attr in attribute for attr in ['màn', 'display', 'screen']):
        return 'Màn hình'
    
    if any(attr in attribute for attr in ['bút', 'pen']):
        return 'Bút đi kèm'

    return 'Lựa chọn'

def extract_options(versions: str):
    if '=' not in versions:
        return [
            {
                'attrs': ['Loại'],
                'values': ['Mặc Định'],
                'price': int(versions.split()[0].strip())
            }
        ]

    options = []

    lines = versions.strip().split('\n')

    for line in lines:
        attributes, price_str = line.rsplit('=', 1)
        
        price = int(price_str.strip().split()[0])

        split_attributes = attributes.strip().split('$$')

        attrs, values = [], []

        for pair in split_attributes:
            attribute, value = map(str.strip, pair.split(':', 1))
            
            value = toPascalCase(value)
            attribute = rename_attribute(attribute.lower(), value)

            attrs.append(attribute)
            values.append(value)

        options.append({
            'attrs': attrs,
            'values': values,
            'price': price
        })

    return options

def random_date(start_date: str, end_date: str = datetime.now().strftime('%Y-%m-%d')):
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')

    try:        
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')


    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_days)

    return random_date.strftime('%Y-%m-%d %H:%M:%S')

def check_status(start_date: str, end_date: str, current_date: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')):
    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

    current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S')

    if start_date <= current_date <= end_date:
        return 'Active'
    elif current_date < start_date:
        return 'Inactive'
    else:
        return 'Expired'

@task(name="read_product_csv_file", retries=3, retry_delay_seconds=5)
def read_product_csv_file():
    product_file_name = {
        'dienthoai': 'dienthoai.csv',
        'mayban': 'dienthoaiban.csv',
        'cucgach': 'dienthoaiphothong.csv',
        'dieuhoa': 'dieuhoa.csv',
        'laptop': 'laptop.csv',
        'maydocsach': 'maydocsach.csv',
        'maygiat': 'maygiat.csv',
        'maytinhbang': 'maytinhbang.csv',
        'tivi': 'tivi.csv',
        'tulanh': 'tulanh.csv',
        'camgiamsat': 'cameragiamsat.csv',
        'pc': 'maytinhdeban.csv',
        'mayanh': 'mayanh.csv',
    }

    product_dataframes = {
        name: pd.read_csv(f'./rawData/products/{filename}', encoding='utf-8')
        for name, filename in product_file_name.items()
    }

    return product_dataframes

def remove_missing_values(df: pd.DataFrame):
    return df.dropna()
def remove_duplicates(df: pd.DataFrame):
    return df.drop_duplicates()

@task(name="remove_missing_and_duplicate_values")
def remove_missing_and_duplicate_values(product_dataframes: dict[str, pd.DataFrame]):

    def remove_missing_and_duplicate(df: pd.DataFrame):
        df = remove_missing_values(df)
        df = remove_duplicates(df)
        return df
    
    product_dataframes_cleaned = {
        name: remove_missing_and_duplicate(df)
        for name, df in product_dataframes.items()
    }

    return product_dataframes_cleaned

@task(name="Re-categorize product and create product dataframe")
def recategorize_product(product_dataframes: dict[str, pd.DataFrame]):
    (
        dienthoai, mayban, cucgach, dieuhoa, 
        laptop, maydocsach, maygiat, maytinhbang, 
        tivi, tulanh, camgiamsat, pc, mayanh
    ) = product_dataframes.values()

    dienthoai['Danh mục'] = dienthoai['Danh mục'].replace({
        'Điện Thoại - Máy Tính Bảng': 'Điện thoại Smartphone',
        'Root': 'Điện thoại Smartphone',
        'Phụ kiện': 'Phụ kiện điện thoại',
    })

    mayban['Danh mục'] = 'Điện thoại bàn'

    cucgach['Danh mục'] = 'Điện thoại phổ thông'

    laptop['Danh mục'] = laptop['Danh mục'].replace({
        'Laptop - Máy Vi Tính - Linh kiện': 'Laptop Truyền Thống',
        'Laptop': 'Laptop Truyền Thống',
        'Root': 'Laptop Truyền Thống',
    })    

    maydocsach['Danh mục'] = 'Máy đọc sách'

    maydocsach.loc[maydocsach['Tên sản phẩm'].str.contains('máy tính bảng', case=False), 'Danh mục'] = 'Máy tính bảng'

    maytinhbang['Danh mục'] = 'Máy tính bảng'

    def assign_category_to_tivi(df: pd.DataFrame, keywords: str, category: str):
        mask = df['Danh mục'].isin(['Điện Tử - Điện Lạnh', 'Root']) & df['Tên sản phẩm'].str.contains(keywords, case=False)
        df.loc[mask, 'Danh mục'] = category

    assign_category_to_tivi(tivi, 'oled', 'Tivi OLED')
    assign_category_to_tivi(tivi, 'qled', 'Tivi QLED')
    assign_category_to_tivi(tivi, 'smart|android', 'Smart Tivi - Android Tivi')
    assign_category_to_tivi(tivi, 'led', 'Tivi thường (LED)')
    assign_category_to_tivi(tivi, '4k', 'Tivi 4K')

    tivi.loc[tivi['Danh mục'] == 'Điện Tử - Điện Lạnh', 'Danh mục'] = 'Smart Tivi - Android Tivi'

    tulanh['Danh mục'] = tulanh['Danh mục'].replace({
        'Điện Tử - Điện Lạnh': 'Tủ lạnh',
    })

    camgiamsat['Danh mục'] = camgiamsat['Danh mục'].replace({
        'Camera IP': 'Camera IP - Camera Wifi',
        'Máy Ảnh - Máy Quay Phim': 'Camera IP - Camera Wifi',
    })

    camgiamsat.loc[
        (camgiamsat['Danh mục'] == 'Root') & (camgiamsat['Tên sản phẩm'].str.contains('ip|wifi', case=False)), 
        'Danh mục'
        ] = 'Camera IP - Camera Wifi'

    camgiamsat['Danh mục'] = camgiamsat['Danh mục'].replace({
        'Root': 'Phụ Kiện Camera Giám Sát',
    })

    pc['Danh mục'] = pc['Danh mục'].replace({
        'Máy Tính Bộ Thương Hiệu': 'Máy tính đồng bộ',
        'Root': 'Máy tính đồng bộ',
        'PC - Máy Tính Bộ': 'Máy tính đồng bộ',
    })

    pc.loc[
        (pc['Danh mục'] == 'Laptop - Máy Vi Tính - Linh kiện') & (pc['Tên sản phẩm'].str.contains('mini|siêu nhỏ')),
        'Danh mục'
    ] = 'Mini PC'

    pc['Danh mục'] = pc['Danh mục'].replace({
        'Laptop - Máy Vi Tính - Linh kiện': 'Máy tính đồng bộ',
    })

    products = pd.concat([
            dienthoai, mayban, cucgach, dieuhoa, 
            laptop, maydocsach, maygiat, maytinhbang, 
            tivi, tulanh, camgiamsat, pc, mayanh
        ],
        ignore_index=True
    )

    products.drop_duplicates(subset=['Id'], inplace=True)

    return products

@task(name="Create category dataframe")
def create_category_df(products: pd.DataFrame):
    category_rows = []
    for index, row in enumerate(products['Danh mục'].unique()):
        category_rows.append({
            'id': index + 1,
            'name': row,
        })
    
    category_df = pd.DataFrame(category_rows)

    category_df['hash'] = category_df.apply(get_md5_hash, axis=1)

    return category_df


@task(name="Create attribute and attribute_value dataframe")
def create_attribute_df(products: pd.DataFrame):
    
    def extract_attributes(df: pd.DataFrame):
        attrs = {}

        rows = df['Phiên bản'].astype(str).str.strip()

        for versions in rows:

            options = extract_options(versions)

            for option in options:
                for attr, value in zip(option['attrs'], option['values']):
                    
                    attrs.setdefault(attr, set()).add(value)

        return attrs

    attributes = extract_attributes(products)
    attributes = sorted(attributes.items(), key=lambda x: (x[0], x[1]))

    attribute_rows = []

    for index, (name, _) in enumerate(attributes):
        attribute_rows.append({
            'id': index + 1,
            'name': name,
        })

    attribute_df = pd.DataFrame(attribute_rows)

    attribute_df['hash'] = attribute_df.apply(get_md5_hash, axis=1)

    attribute_rows = []

    attribute_value_id_counter = 1

    for index, (name, values) in enumerate(attributes):
        for value in values:
            
            attribute_rows.append({
                'id': attribute_value_id_counter,
                'attribute_id': index + 1,
                'value': value,
            })

            attribute_value_id_counter += 1

    attribute_value_df = pd.DataFrame(attribute_rows)

    attribute_value_df['hash'] = attribute_value_df.apply(get_md5_hash, axis=1)

    return {
        'attribute_df': attribute_df,
        'attribute_value_df': attribute_value_df,
    }

@task(name="Create product, product_variant and attribute_variant dataframe")
def create_df_related_to_product(products: pd.DataFrame, category_dict: dict[str, int], attribute_dict: dict[str, int], attribute_value_dict: dict[str, int]):
    def create_sku(brand, category, variant_id):
        brand = normalize_vietnamese_string(brand)
        category = normalize_vietnamese_string(category)

        sku = f"{brand[:2].upper()}-{''.join(map(lambda x: x[0].upper(), category.split()))}-{variant_id}"

        return sku

    def create_original_price(price: float):
        mean = 0.8
        std_dev = 0.05

        original_price = price * random.gauss(mean, std_dev)
        original_price = max(original_price, price * 0.5)

        return round(original_price)
    
    option_to_variant_id = {}

    old_product_id_to_new_product_id = {}

    product_rows = []
    product_variant_rows = []
    attribute_variant_rows = []

    variant_id_counter = 1

    for index, row in tqdm(products.iterrows(), total=products.shape[0], desc="Processing products", unit="rows", colour="green"):
        versions = str(row['Phiên bản']).strip()

        options = extract_options(versions)

        product_id = index

        category_id = category_dict[row['Danh mục']]

        product_rows.append({
            'id': product_id,
            'category_id': category_id,
            'name': row['Tên sản phẩm'],
            'description': row['Mô tả'],
            'specification': row['Thông số kỹ thuật'],
            'image_url': row['Hình ảnh'],
            'brand': row['Thương hiệu'],
        })
        
        random.seed(RANDOM_SEED)
        
        for option in options:
            
            stock_quantity = random.randint(0, 120)
            
            price = option['price']

            original_price = create_original_price(price)

            profit = price - original_price

            sku = create_sku(row['Thương hiệu'], row['Danh mục'], variant_id_counter)

            sold_quantity = random.randint(stock_quantity // 2, stock_quantity) if stock_quantity >  10 else 0

            product_variant_rows.append({
                'id': variant_id_counter,
                'product_id': product_id,
                'price': price,
                'original_price': original_price,
                'profit': profit,
                'sku': sku,
                'stock_quantity': stock_quantity,
                'sold_quantity': sold_quantity,
            })

            for attr, val in zip(option['attrs'], option['values']):
                attribute_value_id = attribute_value_dict[val]
                attrirbute_id = attribute_dict[attr]

                attribute_variant_rows.append({
                    'product_variant_id': variant_id_counter,
                    'attribute_id': attrirbute_id,
                    'attribute_value_id': attribute_value_id,
                })

            key = (
                int(row['Id']),
                tuple(sorted(zip(option['attrs'], option['values']))),
            )

            option_to_variant_id[key] = variant_id_counter

            old_product_id_to_new_product_id[int(row['Id'])] = product_id

            variant_id_counter += 1

    product_df = pd.DataFrame(product_rows)
    product_variant_df = pd.DataFrame(product_variant_rows)
    attribute_variant_df = pd.DataFrame(attribute_variant_rows)

    product_df['hash'] = product_df.apply(get_md5_hash, axis=1)
    product_variant_df['hash'] = product_variant_df.apply(get_md5_hash, axis=1)
    attribute_variant_df['hash'] = attribute_variant_df.apply(get_md5_hash, axis=1)

    return {
        'product_df': product_df,
        'product_variant_df': product_variant_df,
        'attribute_variant_df': attribute_variant_df,
        'option_to_variant_id': option_to_variant_id,
        'old_product_id_to_new_product_id': old_product_id_to_new_product_id
    }

@task(name="Read feedback csv file")
def read_feedback_csv_file():
    feedback_file_name = {
        'camera_fb': 'camera_fb.csv',
        'dienthoai_fb': 'dienthoai_fb.csv',
        'mayban_fb': 'dienthoaiban_fb.csv',
        'cucgach_fb': 'dienthoaiphothong_fb.csv',
        'dieuhoa_fb': 'dieuhoa_fb.csv',
        'laptop_fb': 'laptop_fb.csv',
        'mayanh_fb': 'mayanh_fb.csv',
        'maydocsach_fb': 'maydocsach_fb.csv',
        'maygiat_fb': 'maygiat_fb.csv',
        'maytinhbang_fb': 'maytinhbang_fb.csv',
        'tivi_fb': 'tivi_fb.csv',
        'tulanh_fb': 'tulanh_fb.csv',
        'pc_fb': 'maytinhdeban_fb.csv',
    }

    feedback_dataframes = {
        name: pd.read_csv(f'./rawData/feedback/{filename}', encoding='utf-8')
        for name, filename in feedback_file_name.items()
    }

    feedback_dataframes = {
        name: remove_duplicates(df)
        for name, df in feedback_dataframes.items()
    }

    return pd.concat([
        df for df in feedback_dataframes.values()
    ], ignore_index=True)

@task(name="Create feedback dataframe")
def create_feedback_df(feedbacks: pd.DataFrame, customer_df: pd.DataFrame, option_to_variant_id: dict, old_product_id_to_new_product_id: dict):
    def mapping_option_to_variant_id(product_id: int, option: str):
        if pd.isna(option) or option == '' or option == 'nan':
            return None
        
        attributes = option.split('$$')
        attrs, values = [], []

        for pair in attributes:
            attr_value = pair.split(':')
            attr, value = attr_value[0].strip().lower(), attr_value[-1].strip().lower()

            attr = rename_attribute(attr, value)
            value = toPascalCase(value)

            attrs.append(attr)
            values.append(value)

        key = (
            product_id,
            tuple(sorted(zip(attrs, values))),
        )

        return option_to_variant_id.get(key, None)
    
    def mapping_old_product_id_to_new_product_id(old_product_id: str):
        return old_product_id_to_new_product_id.get(old_product_id, None)
    
    num_of_feedbacks = feedbacks.shape[0]

    random.seed(RANDOM_SEED)

    customer_sample = customer_df.sample(num_of_feedbacks, random_state=RANDOM_SEED)
    customer_sample.reset_index(drop=True, inplace=True)

    customer_ids_sample = customer_sample['id'].tolist()
    account_created_at_sample = list(map(
        lambda x: x.strftime('%Y-%m-%d'), 
        customer_sample['created_at'].tolist()
        ))
    
    old_customer_id_to_new_customer_id = {}

    old_feedback_id_to_feedback = {}

    feedback_rows = []

    random.seed(RANDOM_SEED)

    for index, row in tqdm(feedbacks.iterrows(), total=feedbacks.shape[0], desc='Processing feedbacks', unit="rows", colour='green'):
        id = index + 1

        customer_id = old_customer_id_to_new_customer_id.get(
            row['customer_id'], random.choice(customer_ids_sample)
        )
        
        old_customer_id_to_new_customer_id.setdefault(row['customer_id'], customer_id)

        product_id = mapping_old_product_id_to_new_product_id(row['product_id'])

        variant_id = mapping_option_to_variant_id(row['product_id'], row['variant'])

        rating = row['rating']

        comment = row['content']

        created_at = random_date(
            random.choice(account_created_at_sample), 
        )

        feedback_row = {
            'id': id,
            'customer_id': customer_id,
            'product_id': product_id,
            'product_variant_id': variant_id,
            'rating': rating,
            'comment': comment,
            'created_at': created_at,
        }

        feedback_rows.append(feedback_row)

        old_feedback_id_to_feedback.setdefault(int(row['feedback_id']), feedback_row)

    feedback_df = pd.DataFrame(feedback_rows)

    feedback_df['hash'] = feedback_df.apply(get_md5_hash, axis=1)

    return {
        'feedback_df': feedback_df,
        'old_customer_id_to_new_customer_id': old_customer_id_to_new_customer_id,
        'old_feedback_id_to_feedback': old_feedback_id_to_feedback,
    }

@task(name="Read feedback response csv file")
def read_feedback_response_csv_file():
    feedback_response_file_name = {
        'camera_response': 'camera_fb_ma.csv',
        'dienthoai_response': 'dienthoai_fb_ma.csv',
        'mayban_response': 'dienthoaiban_fb_ma.csv',
        'cucgach_response': 'dienthoaiphothong_fb_ma.csv',
        'dieuhoa_response': 'dieuhoa_fb_ma.csv',
        'laptop_response': 'laptop_fb_ma.csv',
        'mayanh_response': 'mayanh_fb_ma.csv',
        'maydocsach_response': 'maydocsach_fb_ma.csv',
        'maygiat_response': 'maygiat_fb_ma.csv',
        'maytinhbang_response': 'maytinhbang_fb_ma.csv',
        'pc_response': 'maytinhdeban_fb_ma.csv',
        'tivi_response': 'tivi_fb_ma.csv',
        'tulanh_response': 'tulanh_fb_ma.csv',
    }

    feedback_response_dataframes = {
        name: pd.read_csv(f'./rawData/feedback_manager/{filename}', encoding='utf-8')
        for name, filename in feedback_response_file_name.items()
    }

    feedback_response_dataframes = {
        name: remove_duplicates(df)
        for name, df in feedback_response_dataframes.items()
    }

    return pd.concat([
        df for df in feedback_response_dataframes.values()
    ], ignore_index=True)

@task(name="Create feedback response dataframe")
def create_feedback_response_df(feedback_responses: pd.DataFrame, service_customer_df: pd.DataFrame, old_feedback_id_to_feedback: dict[int, pd.Series]):

    service_customer_ids = service_customer_df['id'].tolist()
    feedback_responses['content'] = feedback_responses['content'].str.replace('tiki', 'PTIT-EShop', case=False)

    def mapping_old_feedback_id_to_feedback(fb_id):
        return old_feedback_id_to_feedback.get(fb_id, None)
    
    feedback_response_rows = []

    random.seed(RANDOM_SEED)

    for index, row in tqdm(feedback_responses.iterrows(), total=feedback_responses.shape[0], desc='Processing feedback responses', unit="rows", colour='green'):
        
        id = index + 1
        
        manager_id = random.choice(service_customer_ids)

        customer_feedback = mapping_old_feedback_id_to_feedback(row['feedback_id'])

        feedback_id = customer_feedback['id'] if customer_feedback else None

        comment = row['content']

        create_at = random_date(
            customer_feedback['created_at'] if customer_feedback else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )

        feedback_response_rows.append({
            'id': id,
            'manager_id': manager_id,
            'feedback_id': feedback_id,
            'comment': comment,
            'created_at': create_at,
        })

        feedback_response_df = pd.DataFrame(feedback_response_rows)

        feedback_response_df['hash'] = feedback_response_df.apply(get_md5_hash, axis=1)

    return feedback_response_df

@task(name="Create discount dataframe")
def create_discount_df(product_variant_df: pd.DataFrame):
   
    def get_discount_value(type: str, original_price: float, price: float, min_discount=0.05, max_discount=0.3, min_profit=0.05):
        valid_discounts = []
        
        for i in range(int(min_discount * 100), int(max_discount * 100) + 1):
            discount_rate = i / 100
            discounted_price = price * (1 - discount_rate)

            if discounted_price > original_price * (1 + min_profit):
                valid_discounts.append({
                    'rate': discount_rate,
                    'price': discounted_price,
                })
        
        if valid_discounts:
            if type == 'Percentage':
                return round(random.choice(valid_discounts)['rate'], 3)
            elif type == 'FixedAmount':
                return round(random.choice(valid_discounts)['price'], 3)
        
        return round(price, 3)

        
    types = ['Percentage', 'FixedAmount']

    voucher_name = [
        'Giảm giá sinh nhật',
        'Giảm giá ngày lễ',
        'Giảm giá hot',
        'Giảm giá sốc',
        'Giảm giá cực mạnh',
        'Giảm giá lớn',
        'Giảm giá hấp dẫn',
        'Giảm giá cực chất',
        'Giảm giá không thể bỏ qua',
        'Giảm giá cực đã',
        'Giảm giá cực phê',
        'Giảm giá cực chất',
        'Giảm giá cực đỉnh',
        'Giảm giá cực chất lượng',
        'Giảm giá cực chất lượng cao'
    ]

    variant_id_to_voucher = {}

    discount_rows = []

    random_product_varriant = product_variant_df[['id', 'price', 'original_price', 'profit']].sample(n=1000, replace=False, random_state=RANDOM_SEED)

    for index, row in tqdm(random_product_varriant.iterrows(), total=random_product_varriant.shape[0], desc='Processing discounts', unit="rows", colour='green'):
        
        id = index + 1

        random_string = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))

        product_variant_id = row['id']

        code = f"PTIT-{random_string}"

        discount_name = random.choice(voucher_name)

        discount_type = random.choice(types)

        discount_value = get_discount_value(discount_type, row['original_price'], row['price'])

        start_date = random_date('2024-01-01', '2025-12-30')

        end_date = random_date(start_date, '2025-12-31')

        status = check_status(start_date, end_date)

        discount_rows.append({
            'id': id,
            'product_variant_id': product_variant_id,
            'code': code,
            'name': discount_name,
            'type': discount_type,
            'value': discount_value,
            'status': status,
            'start_date': start_date,
            'end_date': end_date,
        })

        variant_id_to_voucher[product_variant_id] = {
            'type': discount_type, 
            'value': discount_value, 
            'start_date': start_date, 
            'end_date': end_date
        }

        discount_df = pd.DataFrame(discount_rows)

        discount_df['hash'] = discount_df.apply(get_md5_hash, axis=1)

    return {
        'discount_df': discount_df,
        'variant_id_to_voucher': variant_id_to_voucher
    }

@task(name="Create order, order_item, order_history dataframe")
def create_df_related_to_order(feedback_df: pd.DataFrame, customer_df: pd.DataFrame, product_variant_df: pd.DataFrame, manager_df: pd.DataFrame, variant_id_to_voucher: dict[int, dict] ):
    def get_order_status(manage_order_status, payment_status=None):
        if manage_order_status == 'Pending':
            return 'Processing'
        
        if manage_order_status == 'Rejected':
            return 'Rejected'
        
        if manage_order_status == 'Processing':
            if payment_status == 'Paid' or payment_status == 'Partially Paid':
                return 'Processing'
            
            return 'Rejected'

        if manage_order_status == 'Completed':
                return 'Completed'
        
        return 'Processing'

    def get_payment_status(payment_method, manage_order_status, order_status):
        if order_status == 'Rejected':
            return 'Cancelled' if payment_method == 'COD' else 'Refunded'
        
        if manage_order_status == 'Pending':
            return 'Pending'
        
        if manage_order_status == 'Processing':
            if payment_method == 'COD':
                return 'Pending'
            
            return 'Partially Paid'
        
        if manage_order_status == 'Completed':
            return 'Paid'
        
        if manage_order_status == 'Cancelled':
            return 'Cancelled' if payment_method == 'COD' else 'Refunded'
        
        return 'Pending'

    def random_order(payment_method=None, manage_order_status=None, order_status=None, payment_status=None):
        if payment_method is None:
            payment_method = random.choice(['COD', 'Credit Card', 'Bank Transfer', 'PayPal'])
        
        if manage_order_status is None:
            statuses = ['Pending', 'Processing', 'Cancelled', 'Completed']
            probabilities = [0.01, 0.01, 0.01, 0.97]
            manage_order_status = random.choices(statuses, weights=probabilities, k=1)[0]

        if order_status is None:
            order_status = get_order_status(manage_order_status, payment_status)

        if payment_status is None:
            payment_status = get_payment_status(payment_method, manage_order_status, order_status)

        return {
            'payment_method': payment_method,
            'manage_order_status': manage_order_status,
            'order_status': order_status,
            'payment_status': payment_status
        }
    
    feedback_customers = feedback_df[['customer_id', 'created_at', 'product_id', 'product_variant_id']].rename(
        columns={
            'created_at': 'feedback_created_at',
        }
    )

    feedback_customers = feedback_customers.merge(
        customer_df[['id', 'address']], how = 'left', left_on='customer_id', right_on='id'
    ).drop(columns=['id'])

    customer_sample = customer_df[['id', 'created_at', 'address']] \
                .sample(n=5000, replace=True, random_state=RANDOM_SEED) \
                .rename(columns={
                    'id': 'customer_id',
                    'created_at': 'customer_created_at',
                })
    
    customer_order_df = pd.concat([
        feedback_customers, customer_sample
    ], axis=0, ignore_index=True)
    
    random.seed(RANDOM_SEED)

    product_variant_ids_sample = random.choices(
        product_variant_df['id'].tolist(), k=5000
    )

    product_id_to_variant_id = product_variant_df.groupby('product_id')['id'].apply(list).to_dict()

    def fill_product_variant_id(row):

        if pd.isna(row['product_variant_id']):
            if pd.isna(row['product_id']):
                return int(random.choice(product_variant_ids_sample))
            
            return int(random.choice(product_id_to_variant_id[row['product_id']]))
        
        return int(row['product_variant_id'])

    random.seed(RANDOM_SEED)

    customer_order_df['product_variant_id'] = customer_order_df.apply(
        lambda row: fill_product_variant_id(row), axis=1
    )

    customer_order_df.drop(columns=['product_id'], inplace=True)

    customer_order_df['customer_created_at'] = customer_order_df['customer_created_at'].astype(str).str.strip()

    manager_ids = manager_df['id'].tolist()

    def get_order_date(row):
        if pd.isna(row['feedback_created_at']):
            return random_date(row['customer_created_at'])
        
        return (datetime.strptime(row['feedback_created_at'], '%Y-%m-%d %H:%M:%S') - timedelta(days=random.randint(1, 3))).strftime('%Y-%m-%d %H:%M:%S')
    
    def get_status(row):
        if pd.isna(row['feedback_created_at']):
            return random_order()
        
        return random_order(manage_order_status='Completed', order_status='Completed', payment_status='Paid')
    
    def get_payment_date(row, payment_method, order_date, order_status):
        if pd.isna(row['feedback_created_at']):
            if payment_method == 'COD' and order_status == 'Completed':
                    return random_date(order_date)
            if payment_method != 'COD':
                return order_date
            
            return None
        
        if payment_method == 'COD':
            return random_date(order_date)
        
        return order_date
    
    def get_payment_amount(unit_price, quantity, order_date, voucher):

        if voucher is None or check_status(voucher['start_date'], voucher['end_date'], order_date) == 'Expired':
            return unit_price * quantity
        

        if voucher['type'] == 'Percentage':
            return unit_price * quantity * (1 - voucher['value'])
        
        return unit_price * quantity - voucher['value']
    
    order_rows = []
    order_item_rows = []
    order_history_rows = []


    for index, row in tqdm(customer_order_df.iterrows(), total=customer_order_df.shape[0], desc='Processing orders', unit="rows", colour='green'):
        id = index + 1

        customer_id = row['customer_id']

        order_date = get_order_date(row)

        shipping_address = row['address']

        status_dict = get_status(row)

        status = status_dict['order_status']

        payment_method = status_dict['payment_method']

        payment_date = get_payment_date(row, payment_method, order_date, status)

        payment_status = status_dict['payment_status']

        product_variant_id = row['product_variant_id']

        quantity = random.randint(1, 5)

        unit_price = product_variant_df.loc[product_variant_df['id'] == product_variant_id, 'price'].values[0]

        voucher = variant_id_to_voucher.get(product_variant_id, None)

        payment_amount = get_payment_amount(unit_price, quantity, order_date, voucher)

        manager_id = random.choice(manager_ids)

        processing_time = random_date(order_date, payment_date) if payment_date else None

        previous_status = random.choice(['Pending', 'Processing', 'Completed', 'Cancelled'])

        new_status = status

        order_rows.append({
            'id': id,
            'customer_id': customer_id,
            'order_date': order_date,
            'shipping_address': shipping_address,
            'status': status,
            'payment_method': payment_method,
            'payment_date': payment_date,
            'payment_status': payment_status,
            'payment_amount': payment_amount,
        })

        order_item_rows.append({
            'id': id,
            'product_variant_id': product_variant_id,
            'order_id': id,
            'quantity': quantity,
            'unit_price': unit_price,
            'note': '',
        })

        order_history_rows.append({
            'id': id,
            'manager_id': manager_id,
            'order_id': id,
            'processing_time': processing_time,
            'previous_status': previous_status,
            'new_status': new_status,
        })

    order_df = pd.DataFrame(order_rows)
    order_item_df = pd.DataFrame(order_item_rows)
    order_history_df = pd.DataFrame(order_history_rows)

    order_df['hash'] = order_df.apply(get_md5_hash, axis=1)
    order_item_df['hash'] = order_item_df.apply(get_md5_hash, axis=1)
    order_history_df['hash'] = order_history_df.apply(get_md5_hash, axis=1)

    return {
        'order_df': order_df,
        'order_item_df': order_item_df,
        'order_history_df': order_history_df
    }

@task(name="Save category dataframe to sql server")
def save_category_df(category_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_category(
            name NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_category_hash ON #new_category(hash);
    """)

    category_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(category_df.itertuples(index=False, name=None), total=category_df.shape[0], desc="Creating new category data", unit="rows", colour="green")
    ]
    
    cursor.executemany("""
        INSERT INTO #new_category(name, hash)
        VALUES (?, ?);
    """, category_tuples)
    

    cursor.execute("""
        INSERT INTO category(name, hash)
        SELECT name, hash
        FROM #new_category
        WHERE NOT EXISTS (
            SELECT 1
            FROM category c
            WHERE c.hash = #new_category.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_category;
    """)

@task(name="Save product dataframe to sql server")
def save_product_df(product_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_product(
            category_id INT,
            name NVARCHAR(255),
            description NVARCHAR(MAX),
            specification NVARCHAR(MAX),
            image_url NVARCHAR(255),
            brand NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_product_hash ON #new_product(hash);
    """)
    product_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(product_df.itertuples(index=False, name=None), total=product_df.shape[0], desc="Creating new product data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_product(category_id, name, description, specification, image_url, brand, hash)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """, product_tuples)

    cursor.execute("""
        INSERT INTO product(category_id, name, description, specification, image_url, brand, hash)
        SELECT category_id, name, description, specification, image_url, brand, hash
        FROM #new_product
        WHERE NOT EXISTS (
            SELECT 1
            FROM product p
            WHERE p.hash = #new_product.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_product;
    """)

@task(name="Save attribute dataframe to sql server")
def save_attribute_df(attribute_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_attribute(
            name NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_attribute_hash ON #new_attribute(hash);
    """)

    attribute_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(attribute_df.itertuples(index=False, name=None), total=attribute_df.shape[0], desc="Creating new attribute data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_attribute(name, hash)
        VALUES (?, ?);
    """, attribute_tuples)

    cursor.execute("""
        INSERT INTO attribute(name, hash)
        SELECT name, hash
        FROM #new_attribute
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute a
            WHERE a.hash = #new_attribute.hash
        );
    """)
    cursor.execute("""
        DROP TABLE #new_attribute;
    """)

@task(name="Save attribute value dataframe to sql server")
def save_attribute_value_df(attribute_value_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_attribute_value(
            attribute_id INT,
            value NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_attribute_value_hash ON #new_attribute_value(hash);
    """)

    attribute_value_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(attribute_value_df.itertuples(index=False, name=None), total=attribute_value_df.shape[0], desc="Creating new attribute value data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_attribute_value(attribute_id, value, hash)
        VALUES (?, ?, ?);
    """, attribute_value_tuples)

    cursor.execute("""
        INSERT INTO attribute_value(attribute_id, value, hash)
        SELECT attribute_id, value, hash
        FROM #new_attribute_value
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute_value av
            WHERE av.hash = #new_attribute_value.hash
        );
    """)
    cursor.execute("""
        DROP TABLE #new_attribute_value;
    """)

@task(name="Save product_variant dataframe to sql server")
def save_product_variant_df(product_variant_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_product_variant(
            product_id INT,
            price FLOAT,
            original_price FLOAT,
            profit FLOAT,
            sku NVARCHAR(255),
            stock_quantity INT,
            sold_quantity INT,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_product_variant_hash ON #new_product_variant(hash);
    """)

    product_variant_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(product_variant_df.itertuples(index=False, name=None), total=product_variant_df.shape[0], desc="Creating new product variant data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_product_variant(product_id, price, original_price, profit, sku, stock_quantity, sold_quantity, hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, product_variant_tuples)

    cursor.execute("""
        INSERT INTO product_variant(product_id, price, original_price, profit, sku, stock_quantity, sold_quantity, hash)
        SELECT product_id, price, original_price, profit, sku, stock_quantity, sold_quantity, hash
        FROM #new_product_variant
        WHERE NOT EXISTS (
            SELECT 1
            FROM product_variant pv
            WHERE pv.hash = #new_product_variant.hash
        );           
    """)

    cursor.execute("""
        DROP TABLE #new_product_variant;
    """)

@task(name="Save attribute_variant dataframe to sql server")
def save_attribute_variant_df(attribute_variant_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_attribute_variant(
            product_variant_id INT,
            attribute_id INT,
            attribute_value_id INT,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_attribute_variant_hash ON
        #new_attribute_variant(hash);
    """)

    attribute_variant_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(attribute_variant_df.itertuples(index=False, name=None), total=attribute_variant_df.shape[0], desc="Creating new attribute variant data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_attribute_variant(product_variant_id, attribute_id, attribute_value_id, hash)
        VALUES (?, ?, ?, ?);
    """, attribute_variant_tuples)

    cursor.execute("""
        INSERT INTO attribute_variant(product_variant_id, attribute_id, attribute_value_id, hash)
        SELECT product_variant_id, attribute_id, attribute_value_id, hash
        FROM #new_attribute_variant
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute_variant av
            WHERE av.hash = #new_attribute_variant.hash
        );           
    """)

    cursor.execute("""
        DROP TABLE #new_attribute_variant;
    """)

@task(name="Save feedback dataframe to sql server")
def save_feedback_df(feedback_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_feedback(
            customer_id INT,
            product_id INT,
            product_variant_id INT,
            rating FLOAT,
            comment NVARCHAR(MAX),
            created_at DATETIME,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_feedback_hash ON #new_feedback(hash);
    """)

    feedback_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(feedback_df.itertuples(index=False, name=None), total=feedback_df.shape[0], desc="Creating new feedback data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_feedback(customer_id, product_id, product_variant_id, rating, comment, created_at, hash)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """, feedback_tuples)

    cursor.execute("""
        INSERT INTO feedback(customer_id, product_id, product_variant_id, rating, comment, created_at, hash)
        SELECT customer_id, product_id, product_variant_id, rating, comment, created_at, hash
        FROM #new_feedback
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback f
            WHERE f.hash = #new_feedback.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_feedback;
    """)

@task(name="Save feedback response dataframe to sql server")
def save_feedback_response_df(feedback_response_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_feedback_response(
            manager_id INT,
            feedback_id INT,
            comment NVARCHAR(MAX),
            created_at DATETIME,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_feedback_response_hash ON #new_feedback_response(hash);
    """)

    feedback_response_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(feedback_response_df.itertuples(index=False, name=None), total=feedback_response_df.shape[0], desc="Creating new feedback response data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_feedback_response(manager_id, feedback_id, comment, created_at, hash)
        VALUES (?, ?, ?, ?, ?);
    """, feedback_response_tuples)

    cursor.execute("""
        INSERT INTO feedback_response(manager_id, feedback_id, comment, created_at, hash)
        SELECT manager_id, feedback_id, comment, created_at, hash
        FROM #new_feedback_response
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback_response fr
            WHERE fr.hash = #new_feedback_response.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_feedback_response;
    """)

@task(name="Save discount dataframe to sql server")
def save_discount_df(discount_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_discount(
            product_variant_id INT,
            code NVARCHAR(255),
            name NVARCHAR(255),
            type NVARCHAR(255),
            value FLOAT,
            status NVARCHAR(255),
            start_date DATETIME,
            end_date DATETIME,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_discount_hash ON #new_discount(hash);
    """)

    discount_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(discount_df.itertuples(index=False, name=None), total=discount_df.shape[0], desc="Creating new discount data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_discount(product_variant_id, code, name, type, value, status, start_date, end_date, hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, discount_tuples)

    cursor.execute("""
        INSERT INTO discount(product_variant_id, code, name, type, value, status, start_date, end_date, hash)
        SELECT product_variant_id, code, name, type, value, status, start_date, end_date, hash
        FROM #new_discount
        WHERE NOT EXISTS (
            SELECT 1
            FROM discount d
            WHERE d.hash = #new_discount.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_discount;
    """)

@task(name="Save order dataframe to sql server")
def save_order_df(order_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_order(
            customer_id INT,
            order_date DATETIME,
            shipping_address NVARCHAR(255),
            status NVARCHAR(255),
            payment_method NVARCHAR(255),
            payment_date DATETIME,
            payment_status NVARCHAR(255),
            payment_amount FLOAT,
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_order_hash ON #new_order(hash);
    """)
    
    order_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_df.itertuples(index=False, name=None), total=order_df.shape[0], desc="Creating new order data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_order(customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount, hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, order_tuples)
    
    cursor.execute("""
        INSERT INTO [order](customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount, hash)
        SELECT customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount, hash
        FROM #new_order
        WHERE NOT EXISTS (
            SELECT 1
            FROM [order] o
            WHERE o.hash = #new_order.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_order;
    """)

@task(name="Save order_item dataframe to sql server")
def save_order_item_df(order_item_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_order_item(
            product_variant_id INT,
            order_id INT,
            quantity INT,
            unit_price FLOAT,
            note NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_order_item_hash ON #new_order_item(hash);
    """)

    order_item_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_item_df.itertuples(index=False, name=None), total=order_item_df.shape[0], desc="Creating new order item data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_order_item(product_variant_id, order_id, quantity, unit_price, note, hash)
        VALUES (?, ?, ?, ?, ?, ?);
    """, order_item_tuples)

    cursor.execute("""
        INSERT INTO order_item(product_variant_id, order_id, quantity, unit_price, note, hash)
        SELECT product_variant_id, order_id, quantity, unit_price, note, hash
        FROM #new_order_item
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_item oi
            WHERE oi.hash = #new_order_item.hash
        );           
    """)

    cursor.execute("""
        DROP TABLE #new_order_item;
    """)

@task(name="Save order_history dataframe to sql server")
def save_order_history_df(order_history_df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("""
        CREATE TABLE #new_order_history(
            manager_id INT,
            order_id INT,
            processing_time DATETIME,
            previous_status NVARCHAR(255),
            new_status NVARCHAR(255),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_order_history_hash ON #new_order_history(hash);
    """)

    order_history_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_history_df.itertuples(index=False, name=None), total=order_history_df.shape[0], desc="Creating new order history data", unit="rows", colour="green")
    ]
    cursor.executemany("""
        INSERT INTO #new_order_history(manager_id, order_id, processing_time, previous_status, new_status, hash)
        VALUES (?, ?, ?, ?, ?, ?);
    """, order_history_tuples)

    cursor.execute("""
        INSERT INTO order_history(manager_id, order_id, processing_time, previous_status, new_status, hash)
        SELECT manager_id, order_id, processing_time, previous_status, new_status, hash
        FROM #new_order_history
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_history oh
            WHERE oh.hash = #new_order_history.hash
        );
    """)

    cursor.execute("""
        DROP TABLE #new_order_history;
    """)

@flow(name="ETL Pipeline")
def etl_pipeline():
    product_dataframes = read_product_csv_file()
    product_dataframes = remove_missing_and_duplicate_values(product_dataframes)
    
    products = recategorize_product(product_dataframes)

    category_df = create_category_df.submit(products)


    attribute_df, attribute_value_df = create_attribute_df.submit(products).result().values()

    category_df_result = category_df.result()

    category_dict = dict(zip(category_df_result['name'], category_df_result['id']))

    attribute_dict = dict(zip(attribute_df['name'], attribute_df['id']))

    attribute_value_dict = dict(zip(attribute_value_df['value'], attribute_value_df['id']))

    (
        product_df, product_variant_df, attribute_variant_df,
         option_to_variant_id, old_product_id_to_new_product_id
     ) = create_df_related_to_product.submit(products, category_dict, attribute_dict, attribute_value_dict).result().values()
    
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()

    customer_df = pd.read_sql_query("""
        SELECT c.id, a.created_at, c.address
        FROM customer as c
        JOIN account as a
        ON c.account_id = a.id
        WHERE a.status != 'banned';
    """, conn)

    feedbacks = read_feedback_csv_file()
    (
        feedback_df,
        old_customer_id_to_new_customer_id,
        old_feedback_id_to_feedback
    ) = create_feedback_df.submit(feedbacks, customer_df, option_to_variant_id, old_product_id_to_new_product_id).result().values()

    feedback_responses = read_feedback_response_csv_file()

    service_customer_df = pd.read_sql_query("""
        SELECT m.id as id
        FROM manager as m
        JOIN role as r
        ON m.role_id = r.id
        WHERE r.name = 'service_customer';
    """, conn)

    feedback_response_df = create_feedback_response_df.submit(feedback_responses, service_customer_df, old_feedback_id_to_feedback)

    (
        discount_df,
        variant_id_to_voucher
    ) = create_discount_df.submit(product_variant_df).result().values()

    manager_df = pd.read_sql_query("""
        SELECT m.id
        FROM manager as m
        JOIN role as r
        ON m.role_id = r.id
        WHERE r.name = 'product_manager';
    """, conn)

    (
        order_df,
        order_item_df,
        order_history_df
    ) = create_df_related_to_order.submit(feedback_df, customer_df, product_variant_df, manager_df, variant_id_to_voucher).result().values()

    save_category_df.submit(category_df, cursor)
    save_product_df.submit(product_df, cursor)
    save_attribute_df.submit(attribute_df, cursor)
    save_attribute_value_df.submit(attribute_value_df, cursor)
    save_product_variant_df.submit(product_variant_df, cursor)
    save_attribute_variant_df.submit(attribute_variant_df, cursor)
    save_feedback_df.submit(feedback_df, cursor)
    save_feedback_response_df.submit(feedback_response_df.result(), cursor)
    save_discount_df.submit(discount_df, cursor)
    save_order_df.submit(order_df, cursor)
    save_order_item_df.submit(order_item_df, cursor)
    save_order_history_df.submit(order_history_df, cursor)

    conn.commit()
    conn.close()

    
if __name__ == "__main__":
    etl_pipeline()
    print("ETL pipeline completed successfully.")