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
from prefect.cache_policies import NONE as NO_CACHE
from extract.id_product import get_id_product
from extract.info_product import get_info_product
from extract.feedback_users import get_feedback_users


load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))

RANDOM_SEED = 42
FIXED_CURRENT_DATE_AND_TIME = "2025-06-20 00:00:00"
FIXED_CURRENT_DATE = datetime.strptime(FIXED_CURRENT_DATE_AND_TIME, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

DB_NAME = os.getenv("DB_NAME")

time_now = datetime.now()

random_generator = random.Random(RANDOM_SEED)

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
    standardized_values = []
    for val in row:
        if pd.isna(val):
            standardized_values.append('')
        elif isinstance(val, float):
            standardized_values.append(f"{val:.6f}")
        else:
            standardized_values.append(str(val))
    str_row = ' '.join(standardized_values)
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

def random_date(start_date: str, end_date: str = FIXED_CURRENT_DATE_AND_TIME):
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')

    try:        
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')


    delta = end_date - start_date
    random_days = random_generator.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_days)

    return random_date.strftime('%Y-%m-%d %H:%M:%S')

def check_status(start_date: str, end_date: str, current_date: str = FIXED_CURRENT_DATE_AND_TIME):
    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

    current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S')

    if start_date <= current_date <= end_date:
        return 'Active'
    elif current_date < start_date:
        return 'Inactive'
    else:
        return 'Expired'
    
@task(name="crawl id of products", retries=3, retry_delay_seconds=5)
def crawl_id_product(current_dir: str, time_now: datetime):
    get_id_product(current_dir, time_now)

    return True

@task(name="crawl product infomation", retries=3, retry_delay_seconds=5)
def crawl_product_info(current_dir: str, time_now: datetime):
    get_info_product(current_dir, time_now)

    return True

@task(name="crawl feedback of customers and responses of manager", retries=3, retry_delay_seconds=5)
def crawl_feedback_users(current_dir: str, time_now: datetime):
    get_feedback_users(current_dir, time_now)

    return True

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
        name: pd.read_csv(os.path.join(current_dir, "rawData", "products", filename), encoding='utf-8')
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

    products.sort_values(by=['Id'], inplace=True)
    products.reset_index(drop=True, inplace=True)

    products.drop_duplicates(subset=['Id'], inplace=True)

    return products

@task(name="Create category dataframe")
def create_category_df(products: pd.DataFrame):
    category_rows = []
    for index, row in enumerate(products['Danh mục'].unique()):
        category_rows.append({
            'id': '',
            'name': row,
        })
    
    category_df = pd.DataFrame(category_rows)

    category_df['id'] = category_df.drop(columns=['id']).apply(get_md5_hash, axis=1)

    category_df.sort_values(by='id', inplace=True)
    category_df.reset_index(drop=True, inplace=True)

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
            'id': '',
            'name': name,
        })

    attribute_df = pd.DataFrame(attribute_rows)

    attribute_df['id'] = attribute_df.drop(columns=['id']).apply(get_md5_hash, axis=1)
   
    attribute_df.sort_values(by='id', inplace=True)
    attribute_df.reset_index(drop=True, inplace=True)

    attribute_value_rows = []

    for index, (name, values) in enumerate(attributes):
        for value in values:
            
            attribute_value_rows.append({
                'id': '',
                'attribute_id': get_md5_hash(pd.Series({'name': name})),
                'value': value,
            })

    attribute_value_df = pd.DataFrame(attribute_value_rows)

    attribute_value_df['id'] = attribute_value_df.drop(columns=['id']).apply(get_md5_hash, axis=1)

    attribute_value_df.sort_values(by='id', inplace=True)
    attribute_value_df.reset_index(drop=True, inplace=True)

    return {
        'attribute_df': attribute_df,
        'attribute_value_df': attribute_value_df,
    }

@task(name="Create product, product_variant and attribute_variant dataframe")
def create_df_related_to_product(products: pd.DataFrame, category_dict: dict[str, int], attribute_dict: dict[str, int], attribute_value_dict: dict[str, int]):
    
    def create_sku(brand, category, variant_id: str):
        brand = normalize_vietnamese_string(brand)
        category = normalize_vietnamese_string(category)

        sku = f"{brand[:2].upper()}-{''.join(map(lambda x: x[0].upper(), category.split()))}-{variant_id.upper()}"

        return sku

    def create_original_price(price: float):
        mean = 0.8
        std_dev = 0.05

        original_price = price * random_generator.gauss(mean, std_dev)
        original_price = max(original_price, price * 0.5)

        return round(original_price)
    
    option_to_variant_id = {}

    old_product_id_to_new_product_id = {}

    product_rows = []
    product_variant_rows = []
    attribute_variant_rows = []

    for index, row in tqdm(products.iterrows(), total=products.shape[0], desc="Processing products", unit="rows", colour="green"):
        versions = str(row['Phiên bản']).strip()

        options = extract_options(versions)

        category_id = category_dict[row['Danh mục']]

        product_id = get_md5_hash(pd.Series({
            'category_id': category_id,
            'name': row['Tên sản phẩm'],
            'description': row['Mô tả'],
            'specification': row['Thông số kỹ thuật'],
            'image_url': row['Hình ảnh'],
            'brand': row['Thương hiệu'],
        }))


        product_rows.append({
            'id': product_id,
            'category_id': category_id,
            'name': row['Tên sản phẩm'],
            'description': row['Mô tả'],
            'specification': row['Thông số kỹ thuật'],
            'image_url': row['Hình ảnh'],
            'brand': row['Thương hiệu'],
        })
        

        
        for option in options:
            
            stock_quantity = random_generator.randint(0, 120)
            
            price = option['price']

            original_price = create_original_price(price)

            profit = price - original_price

            sku = create_sku(row['Thương hiệu'], row['Danh mục'], product_id[0:4])

            sold_quantity = random_generator.randint(stock_quantity // 2, stock_quantity) if stock_quantity >  10 else 0

            product_variant_id = get_md5_hash(pd.Series({
                'product_id': product_id,
                'price': price,
                'original_price': original_price,
                'profit': profit,
                'sku': sku,
                'stock_quantity': stock_quantity,
                'sold_quantity': sold_quantity,
            }))

            product_variant_rows.append({
                'id': product_variant_id,
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
                    'product_variant_id': product_variant_id,
                    'attribute_id': attrirbute_id,
                    'attribute_value_id': attribute_value_id,
                })

            key = (
                int(row['Id']),
                tuple(sorted(zip(option['attrs'], option['values']))),
            )

            option_to_variant_id[key] = product_variant_id

            old_product_id_to_new_product_id[int(row['Id'])] = product_id

    product_df = pd.DataFrame(product_rows)
    product_variant_df = pd.DataFrame(product_variant_rows)
    attribute_variant_df = pd.DataFrame(attribute_variant_rows)

    product_df.sort_values(by='id', inplace=True)
    product_df.reset_index(drop=True, inplace=True)

    product_variant_df.sort_values(by='id', inplace=True)
    product_variant_df.reset_index(drop=True, inplace=True)


    attribute_variant_df['hash'] = attribute_variant_df.apply(get_md5_hash, axis=1)
    attribute_variant_df.sort_values(by='product_variant_id', inplace=True)
    attribute_variant_df.reset_index(drop=True, inplace=True)

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
        name: pd.read_csv(os.path.join(current_dir, "rawData", "feedback", filename), encoding='utf-8')
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
    def mapping_option_to_variant_id(product_id: str, option: str):
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

    customer_df = customer_df.sort_values('id').reset_index(drop=True)

    feedbacks = feedbacks.sort_values('feedback_id').reset_index(drop=True)

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


    for index, row in tqdm(feedbacks.iterrows(), total=feedbacks.shape[0], desc='Processing feedbacks', unit="rows", colour='green'):

        customer_id = old_customer_id_to_new_customer_id.get(
            row['customer_id'], random_generator.choice(customer_ids_sample)
        )
        
        old_customer_id_to_new_customer_id.setdefault(row['customer_id'], customer_id)

        product_id = mapping_old_product_id_to_new_product_id(row['product_id'])

        variant_id = mapping_option_to_variant_id(row['product_id'], row['variant'])

        rating = row['rating']

        comment = row['content']

        created_at = random_date(
            random_generator.choice(account_created_at_sample), 
        )

        id = get_md5_hash(pd.Series({
            'customer_id': customer_id,
            'product_id': product_id,
            'product_variant_id': variant_id,
            'rating': rating,
            'comment': comment,
            'created_at': created_at,
        }))

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

        old_feedback_id_to_feedback.setdefault(row['feedback_id'], feedback_row)

    feedback_df = pd.DataFrame(feedback_rows)

    feedback_df.sort_values(by='id', inplace=True)
    feedback_df.reset_index(drop=True, inplace=True)

    return {
        'feedback_df': feedback_df,
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
        name: pd.read_csv(os.path.join(current_dir, "rawData", "feedback_manager", filename), encoding='utf-8')
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


    for index, row in tqdm(feedback_responses.iterrows(), total=feedback_responses.shape[0], desc='Processing feedback responses', unit="rows", colour='green'):
        
        manager_id = random_generator.choice(service_customer_ids)

        customer_feedback = mapping_old_feedback_id_to_feedback(row['feedback_id'])

        feedback_id = customer_feedback['id'] if customer_feedback else None

        comment = row['content']

        create_at = random_date(
            customer_feedback['created_at'] if customer_feedback else FIXED_CURRENT_DATE_AND_TIME
        )

        id = get_md5_hash(pd.Series({
            'manager_id': manager_id,
            'feedback_id': feedback_id,
            'comment': comment,
            'created_at': create_at,
        }))

        feedback_response_rows.append({
            'id': id,
            'manager_id': manager_id,
            'feedback_id': feedback_id,
            'comment': comment,
            'created_at': create_at,
        })

    feedback_response_df = pd.DataFrame(feedback_response_rows)

    feedback_response_df.sort_values(by='id', inplace=True)
    feedback_response_df.reset_index(drop=True, inplace=True)

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
                return round(random_generator.choice(valid_discounts)['rate'], 3)
            elif type == 'FixedAmount':
                return round(random_generator.choice(valid_discounts)['price'], 3)
        
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
        'Giảm giá cực chất',
        'Giảm giá cực đỉnh',
        'Giảm giá cực chất lượng',
        'Giảm giá siêu hời',
        'Giảm giá siêu hấp dẫn',
        'Giảm giá siêu chất',
        'Giảm giá siêu đỉnh',
        'Giảm giá ưu đãi',
    ]

    variant_id_to_voucher = {}

    discount_rows = []


    random_product_varriant = product_variant_df[['id', 'price', 'original_price', 'profit']].sample(n=1000, replace=False, random_state=RANDOM_SEED)

    for index, row in tqdm(random_product_varriant.iterrows(), total=random_product_varriant.shape[0], desc='Processing discounts', unit="rows", colour='green'):
        
        random_string = ''.join(random_generator.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))

        product_variant_id = row['id']

        code = f"PTIT-{random_string}"

        discount_name = random_generator.choice(voucher_name)

        discount_type = random_generator.choice(types)

        discount_value = get_discount_value(discount_type, row['original_price'], row['price'])

        start_date = random_date('2024-01-01', '2025-12-30')

        end_date = random_date(start_date, '2025-12-31')

        status = check_status(start_date, end_date)

        id = get_md5_hash(pd.Series({
            'product_variant_id': product_variant_id,
            'code': code,
            'name': discount_name,
            'type': discount_type,
            'value': discount_value,
            'status': status,
            'start_date': start_date,
            'end_date': end_date,
        }))

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

    discount_df.sort_values(by='id', inplace=True)
    discount_df.reset_index(drop=True, inplace=True)

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
            payment_method = random_generator.choice(['COD', 'Credit Card', 'Bank Transfer', 'PayPal'])
        
        if manage_order_status is None:
            statuses = ['Pending', 'Processing', 'Cancelled', 'Completed']
            probabilities = [0.01, 0.01, 0.01, 0.97]
            manage_order_status = random_generator.choices(statuses, weights=probabilities, k=1)[0]

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
    

    product_variant_ids_sample = random_generator.choices(
        product_variant_df['id'].tolist(), k=5000
    )

    product_id_to_variant_id = product_variant_df.groupby('product_id')['id'].apply(list).to_dict()

    def fill_product_variant_id(row):

        if pd.isna(row['product_variant_id']):
            if pd.isna(row['product_id']):
                return random_generator.choice(product_variant_ids_sample)
            
            return random_generator.choice(product_id_to_variant_id[row['product_id']])
        
        return row['product_variant_id']


    customer_order_df['product_variant_id'] = customer_order_df.apply(
        lambda row: fill_product_variant_id(row), axis=1
    )

    customer_order_df.drop(columns=['product_id'], inplace=True)

    customer_order_df['customer_created_at'] = customer_order_df['customer_created_at'].astype(str).str.strip()

    manager_ids = manager_df['id'].tolist()

    def get_order_date(row):
        if pd.isna(row['feedback_created_at']):
            return random_date(row['customer_created_at'])
        
        return (datetime.strptime(row['feedback_created_at'], '%Y-%m-%d %H:%M:%S') - timedelta(days=random_generator.randint(1, 3))).strftime('%Y-%m-%d %H:%M:%S')
    
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

        customer_id = row['customer_id']

        order_date = get_order_date(row)

        shipping_address = row['address']

        status_dict = get_status(row)

        status = status_dict['order_status']

        payment_method = status_dict['payment_method']

        payment_date = get_payment_date(row, payment_method, order_date, status)

        payment_status = status_dict['payment_status']

        product_variant_id = row['product_variant_id']

        quantity = random_generator.randint(1, 5)

        unit_price = product_variant_df.loc[product_variant_df['id'] == product_variant_id, 'price'].values[0]

        voucher = variant_id_to_voucher.get(product_variant_id, None)

        payment_amount = get_payment_amount(unit_price, quantity, order_date, voucher)

        manager_id = random_generator.choice(manager_ids)

        processing_time = random_date(order_date, payment_date) if payment_date else None

        previous_status = random_generator.choice(['Pending', 'Processing', 'Completed', 'Cancelled'])

        new_status = status

        order_id = get_md5_hash(pd.Series({
            'customer_id': customer_id,
            'order_date': order_date,
            'shipping_address': shipping_address,
            'status': status,
            'payment_method': payment_method,
            'payment_date': payment_date,
            'payment_status': payment_status,
            'payment_amount': payment_amount
        }))

        order_rows.append({
            'id': order_id,
            'customer_id': customer_id,
            'order_date': order_date,
            'shipping_address': shipping_address,
            'status': status,
            'payment_method': payment_method,
            'payment_date': payment_date,
            'payment_status': payment_status,
            'payment_amount': payment_amount,
        })

        order_item_id = get_md5_hash(pd.Series({
            'product_variant_id': product_variant_id,
            'order_id': order_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'note': ''
        }))

        order_item_rows.append({
            'id': order_item_id,
            'product_variant_id': product_variant_id,
            'order_id': order_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'note': '',
        })

        order_history_id = get_md5_hash(pd.Series({
            'manager_id': manager_id,
            'order_id': order_id,
            'processing_time': processing_time,
            'previous_status': previous_status,
            'new_status': new_status
        }))

        order_history_rows.append({
            'id': order_history_id,
            'manager_id': manager_id,
            'order_id': order_id,
            'processing_time': processing_time,
            'previous_status': previous_status,
            'new_status': new_status,
        })

    order_df = pd.DataFrame(order_rows)
    order_item_df = pd.DataFrame(order_item_rows)
    order_history_df = pd.DataFrame(order_history_rows)

    order_df.sort_values(by='id', inplace=True)
    order_df.reset_index(drop=True, inplace=True)

    order_item_df.sort_values(by='id', inplace=True)
    order_item_df.reset_index(drop=True, inplace=True)

    order_history_df.sort_values(by='id', inplace=True)
    order_history_df.reset_index(drop=True, inplace=True)

    return {
        'order_df': order_df,
        'order_item_df': order_item_df,
        'order_history_df': order_history_df
    }

@task(name="Save category dataframe to sql server", cache_policy=NO_CACHE)
def save_category_df(category_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_category(
            id NVARCHAR(255) PRIMARY KEY,
            name NVARCHAR(255),
        );       
    """)

    category_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(category_df.itertuples(index=False, name=None), total=category_df.shape[0], desc="Creating new category data", unit="rows", colour="green")
    ]
    
    cursor.executemany("""
        INSERT INTO #new_category(id, name)
        VALUES (?, ?);
    """, category_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_category
        WHERE NOT EXISTS (
            SELECT 1
            FROM category c
            WHERE c.id = #new_category.id
        );
    """)

    new_category_count = cursor.fetchone()[0]
    print(f"Number of new categories added: {new_category_count}")
    

    cursor.execute("""
        INSERT INTO category(id, name)
        SELECT id, name
        FROM #new_category
        WHERE NOT EXISTS (
            SELECT 1
            FROM category c
            WHERE c.id = #new_category.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_category;
    """)
    
    conn.commit()
    conn.close()

    return True

@task(name="Save product dataframe to sql server", cache_policy=NO_CACHE)
def save_product_df(product_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_product(
            id NVARCHAR(255) PRIMARY KEY,
            category_id NVARCHAR(50),
            name NVARCHAR(255),
            description NVARCHAR(MAX),
            specification NVARCHAR(MAX),
            image_url NVARCHAR(MAX),
            brand NVARCHAR(255),
        );
    """)
    product_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(product_df.itertuples(index=False, name=None), total=product_df.shape[0], desc="Creating new product data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_product(id, category_id, name, description, specification, image_url, brand)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """, product_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_product
        WHERE NOT EXISTS (
            SELECT 1
            FROM product p
            WHERE p.id = #new_product.id
        );
    """)

    new_product_count = cursor.fetchone()[0]
    print(f"Number of new products added: {new_product_count}")

    cursor.execute("""
        INSERT INTO product(id, category_id, name, description, specification, image_url, brand)
        SELECT id, category_id, name, description, specification, image_url, brand
        FROM #new_product
        WHERE NOT EXISTS (
            SELECT 1
            FROM product p
            WHERE p.id = #new_product.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_product;
    """)

    conn.commit()
    conn.close()

    return True

@task(name="Save attribute dataframe to sql server", cache_policy=NO_CACHE)
def save_attribute_df(attribute_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_attribute(
            id NVARCHAR(255) PRIMARY KEY,
            name NVARCHAR(255),
        );
    """)

    attribute_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(attribute_df.itertuples(index=False, name=None), total=attribute_df.shape[0], desc="Creating new attribute data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_attribute(id, name)
        VALUES (?, ?);
    """, attribute_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_attribute
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute a
            WHERE a.id = #new_attribute.id
        );
    """)

    new_attribute_count = cursor.fetchone()[0]
    print(f"Number of new attributes added: {new_attribute_count}")

    cursor.execute("""
        INSERT INTO attribute(id, name)
        SELECT id, name
        FROM #new_attribute
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute a
            WHERE a.id = #new_attribute.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_attribute;
    """)

    
    conn.commit()
    conn.close()

    return True

@task(name="Save attribute value dataframe to sql server", cache_policy=NO_CACHE)
def save_attribute_value_df(attribute_value_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_attribute_value(
            id NVARCHAR(255) PRIMARY KEY,
            attribute_id NVARCHAR(50),
            value NVARCHAR(255),
        );                   
    """)

    attribute_value_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(attribute_value_df.itertuples(index=False, name=None), total=attribute_value_df.shape[0], desc="Creating new attribute value data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_attribute_value(id, attribute_id, value)
        VALUES (?, ?, ?);
    """, attribute_value_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_attribute_value
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute_value av
            WHERE av.id = #new_attribute_value.id
        );
    """)

    new_attribute_value_count = cursor.fetchone()[0]
    print(f"Number of new attribute values added: {new_attribute_value_count}")

    cursor.execute("""
        INSERT INTO attribute_value(id, attribute_id, value)
        SELECT id, attribute_id, value
        FROM #new_attribute_value
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute_value av
            WHERE av.id = #new_attribute_value.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_attribute_value;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save product_variant dataframe to sql server", cache_policy=NO_CACHE)
def save_product_variant_df(product_variant_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_product_variant(
            id NVARCHAR(255) PRIMARY KEY,
            product_id NVARCHAR(50),
            price FLOAT,
            original_price FLOAT,
            profit FLOAT,
            sku NVARCHAR(255),
            stock_quantity INT,
            sold_quantity INT,
        );                   
    """)

    product_variant_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(product_variant_df.itertuples(index=False, name=None), total=product_variant_df.shape[0], desc="Creating new product variant data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_product_variant(id, product_id, price, original_price, profit, sku, stock_quantity, sold_quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, product_variant_tuples)


    cursor.execute("""
        SELECT COUNT(*) FROM #new_product_variant
        WHERE NOT EXISTS (
            SELECT 1
            FROM product_variant pv
            WHERE pv.id = #new_product_variant.id
        );
    """)

    new_product_variant_count = cursor.fetchone()[0]
    print(f"Number of new product variants added: {new_product_variant_count}")

    cursor.execute("""
        INSERT INTO product_variant(id, product_id, price, original_price, profit, sku, stock_quantity, sold_quantity)
        SELECT id, product_id, price, original_price, profit, sku, stock_quantity, sold_quantity
        FROM #new_product_variant
        WHERE NOT EXISTS (
            SELECT 1
            FROM product_variant pv
            WHERE pv.id = #new_product_variant.id
        );           
    """)

    cursor.execute("""
        DROP TABLE #new_product_variant;
    """)
    
    conn.commit()
    conn.close()

    return True

@task(name="Save attribute_variant dataframe to sql server", cache_policy=NO_CACHE)
def save_attribute_variant_df(attribute_variant_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_attribute_variant(
            product_variant_id NVARCHAR(50),
            attribute_id NVARCHAR(50),
            attribute_value_id NVARCHAR(50),
            hash NVARCHAR(255)
        );
                   
        CREATE INDEX nidx_attribute_variant_hash ON #new_attribute_variant(hash);
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
        SELECT COUNT(*) FROM #new_attribute_variant
        WHERE NOT EXISTS (
            SELECT 1
            FROM attribute_variant av
            WHERE av.hash = #new_attribute_variant.hash
        );
    """)

    new_attribute_variant_count = cursor.fetchone()[0]
    print(f"Number of new attribute variants added: {new_attribute_variant_count}")
    
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
    
    conn.commit()
    conn.close()

    return True


@task(name="Save feedback dataframe to sql server", cache_policy=NO_CACHE)
def save_feedback_df(feedback_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_feedback(
            id NVARCHAR(255) PRIMARY KEY,
            customer_id INT,
            product_id NVARCHAR(50),
            product_variant_id NVARCHAR(50),
            rating FLOAT,
            comment NVARCHAR(MAX),
            created_at DATETIME,
        );
    """)

    feedback_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(feedback_df.itertuples(index=False, name=None), total=feedback_df.shape[0], desc="Creating new feedback data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_feedback(id, customer_id, product_id, product_variant_id, rating, comment, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """, feedback_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_feedback
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback f
            WHERE f.id = #new_feedback.id
        );
    """)

    new_feedback_count = cursor.fetchone()[0]
    print(f"Number of new feedbacks added: {new_feedback_count}")

    cursor.execute("""
        INSERT INTO feedback(id, customer_id, product_id, product_variant_id, rating, comment, created_at)
        SELECT id, customer_id, product_id, product_variant_id, rating, comment, created_at
        FROM #new_feedback
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback f
            WHERE f.id = #new_feedback.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_feedback;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save feedback response dataframe to sql server", cache_policy=NO_CACHE)
def save_feedback_response_df(feedback_response_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_feedback_response(
            id NVARCHAR(255) PRIMARY KEY,
            manager_id INT,
            feedback_id NVARCHAR(50),
            comment NVARCHAR(MAX),
            created_at DATETIME,
        );                   
    """)

    feedback_response_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(feedback_response_df.itertuples(index=False, name=None), total=feedback_response_df.shape[0], desc="Creating new feedback response data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_feedback_response(id, manager_id, feedback_id, comment, created_at)
        VALUES (?, ?, ?, ?, ?);
    """, feedback_response_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_feedback_response
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback_response fr
            WHERE fr.id = #new_feedback_response.id
        );
    """)

    new_feedback_response_count = cursor.fetchone()[0]
    print(f"Number of new feedback responses added: {new_feedback_response_count}")

    cursor.execute("""
        INSERT INTO feedback_response(id, manager_id, feedback_id, content, created_at)
        SELECT id, manager_id, feedback_id, comment, created_at
        FROM #new_feedback_response
        WHERE NOT EXISTS (
            SELECT 1
            FROM feedback_response fr
            WHERE fr.id = #new_feedback_response.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_feedback_response;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save discount dataframe to sql server", cache_policy=NO_CACHE)
def save_discount_df(discount_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_discount(
            id NVARCHAR(255) PRIMARY KEY,
            product_variant_id NVARCHAR(50),
            code NVARCHAR(255),
            name NVARCHAR(255),
            type NVARCHAR(255),
            value FLOAT,
            status NVARCHAR(255),
            start_date DATETIME,
            end_date DATETIME,
        );
    """)

    discount_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(discount_df.itertuples(index=False, name=None), total=discount_df.shape[0], desc="Creating new discount data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_discount(id, product_variant_id, code, name, type, value, status, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, discount_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_discount
        WHERE NOT EXISTS (
            SELECT 1
            FROM discount d
            WHERE d.id = #new_discount.id
        );
    """)

    new_discount_count = cursor.fetchone()[0]
    print(f"Number of new discounts added: {new_discount_count}")

    cursor.execute("""
        INSERT INTO discount(id, product_variant_id, code, name, type, value, status, start_date, end_date)
        SELECT id, product_variant_id, code, name, type, value, status, start_date, end_date
        FROM #new_discount
        WHERE NOT EXISTS (
            SELECT 1
            FROM discount d
            WHERE d.id = #new_discount.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_discount;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save order dataframe to sql server", cache_policy=NO_CACHE)
def save_order_df(order_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_order(
            id NVARCHAR(255) PRIMARY KEY,
            customer_id INT,
            order_date DATETIME,
            shipping_address NVARCHAR(255),
            status NVARCHAR(255),
            payment_method NVARCHAR(255),
            payment_date DATETIME,
            payment_status NVARCHAR(255),
            payment_amount FLOAT,
        );
    """)
    
    order_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_df.itertuples(index=False, name=None), total=order_df.shape[0], desc="Creating new order data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_order(id, customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, order_tuples)
 
    cursor.execute("""
        SELECT COUNT(*) FROM #new_order
        WHERE NOT EXISTS (
            SELECT 1
            FROM [order] o
            WHERE o.id = #new_order.id
        );
    """)

    new_order_count = cursor.fetchone()[0]
    print(f"Number of new orders added: {new_order_count}")
   
    cursor.execute("""
        INSERT INTO [order](id, customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount)
        SELECT id, customer_id, order_date, shipping_address, status, payment_method, payment_date, payment_status, payment_amount
        FROM #new_order
        WHERE NOT EXISTS (
            SELECT 1
            FROM [order] o
            WHERE o.id = #new_order.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_order;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save order_item dataframe to sql server", cache_policy=NO_CACHE)
def save_order_item_df(order_item_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_order_item(
            id NVARCHAR(255) PRIMARY KEY,
            product_variant_id NVARCHAR(50),
            order_id NVARCHAR(50),
            quantity INT,
            unit_price FLOAT,
            note NVARCHAR(255),
        );
    """)

    order_item_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_item_df.itertuples(index=False, name=None), total=order_item_df.shape[0], desc="Creating new order item data", unit="rows", colour="green")
    ]

    cursor.executemany("""
        INSERT INTO #new_order_item(id, product_variant_id, order_id, quantity, unit_price, note)
        VALUES (?, ?, ?, ?, ?, ?);
    """, order_item_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_order_item
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_item oi
            WHERE oi.id = #new_order_item.id
        );
    """)

    new_order_item_count = cursor.fetchone()[0]
    print(f"Number of new order items added: {new_order_item_count}")

    cursor.execute("""
        INSERT INTO order_item(id, product_variant_id, order_id, quantity, unit_price, note)
        SELECT id, product_variant_id, order_id, quantity, unit_price, note
        FROM #new_order_item
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_item oi
            WHERE oi.id = #new_order_item.id
        );           
    """)

    cursor.execute("""
        DROP TABLE #new_order_item;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Save order_history dataframe to sql server", cache_policy=NO_CACHE)
def save_order_history_df(order_history_df: pd.DataFrame):
    conn = get_db_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE #new_order_history(
            id NVARCHAR(255) PRIMARY KEY,
            manager_id INT,
            order_id NVARCHAR(50),
            processing_time DATETIME,
            previous_status NVARCHAR(255),
            new_status NVARCHAR(255),
        );                   
    """)

    order_history_tuples = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in tqdm(order_history_df.itertuples(index=False, name=None), total=order_history_df.shape[0], desc="Creating new order history data", unit="rows", colour="green")
    ]
    cursor.executemany("""
        INSERT INTO #new_order_history(id, manager_id, order_id, processing_time, previous_status, new_status)
        VALUES (?, ?, ?, ?, ?, ?);
    """, order_history_tuples)

    cursor.execute("""
        SELECT COUNT(*) FROM #new_order_history
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_history oh
            WHERE oh.id = #new_order_history.id
        );
    """)

    new_order_history_count = cursor.fetchone()[0]
    print(f"Number of new order histories added: {new_order_history_count}")

    cursor.execute("""
        INSERT INTO order_history(id, manager_id, order_id, processing_time, previous_status, new_status)
        SELECT id, manager_id, order_id, processing_time, previous_status, new_status
        FROM #new_order_history
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_history oh
            WHERE oh.id = #new_order_history.id
        );
    """)

    cursor.execute("""
        DROP TABLE #new_order_history;
    """)
    
    conn.commit()
    conn.close()

    return True


@task(name="Get connection to database", cache_policy=NO_CACHE)
def get_conn(db_name: str):
    return get_db_connection(db_name)

@task(name="Close connection to database", cache_policy=NO_CACHE)
def close_conn(conn: pyodbc.Connection):
    conn.commit()
    conn.close()

    return True

@flow(name="ETL Pipeline")
def etl_pipeline():
    crawl_id_product.submit(current_dir, time_now).result()
    crawl_product_info_result = crawl_product_info.submit(current_dir, time_now)
    crawl_feedback_users_result = crawl_feedback_users.submit(current_dir, time_now)

    if  not crawl_product_info_result.result() or not crawl_feedback_users_result.result():
        print("Crawling data failed. Please check the logs for more details.")
        return
    
    print("Crawling data completed successfully.")

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
    
    conn = get_conn.submit(DB_NAME).result()
    cursor = conn.cursor()

    customer_df = pd.read_sql_query("""
        SELECT c.id, a.created_at, c.address
        FROM customer as c
        JOIN account as a
        ON c.account_id = a.id
        WHERE a.status != 'banned'
        ORDER BY c.id, a.created_at, c.address;
    """, conn)

    feedbacks = read_feedback_csv_file()
    (
        feedback_df,
        old_feedback_id_to_feedback
    ) = create_feedback_df.submit(feedbacks, customer_df, option_to_variant_id, old_product_id_to_new_product_id).result().values()

    feedback_responses = read_feedback_response_csv_file()

    service_customer_df = pd.read_sql_query("""
        SELECT m.id as id
        FROM manager as m
        JOIN role as r
        ON m.role_id = r.id
        WHERE r.name = 'service_customer'
        ORDER BY m.id;
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
        WHERE r.name = 'product_manager'
        ORDER BY m.id;
    """, conn)
    
    close_conn.submit(conn)
    
    (
        order_df,
        order_item_df,
        order_history_df
    ) = create_df_related_to_order.submit(feedback_df, customer_df, product_variant_df, manager_df, variant_id_to_voucher).result().values()

    save_category_df.submit(category_df).result()
    save_product_df.submit(product_df).result()
    save_attribute_df.submit(attribute_df).result()

    save_attribute_df_result = save_attribute_value_df.submit(attribute_value_df)
    save_product_variant_df_result = save_product_variant_df.submit(product_variant_df)

    if (save_attribute_df_result 
        and save_product_variant_df_result 
        and save_attribute_df_result.result() 
        and save_product_variant_df_result.result()
    ):
        save_attribute_variant_df_result = save_attribute_variant_df.submit(attribute_variant_df)

    if save_attribute_variant_df_result and save_attribute_variant_df_result.result():
        save_feedback_df_result = save_feedback_df.submit(feedback_df)

    if save_feedback_df_result and save_feedback_df_result.result():
        save_feedback_response_df.submit(feedback_response_df.result())

    save_discount_df.submit(discount_df)
    
    save_order_df_result = save_order_df.submit(order_df)
    if save_order_df_result and save_order_df_result.result():
        save_order_item_df.submit(order_item_df)
        save_order_history_df.submit(order_history_df)

    
if __name__ == "__main__":
    # etl_pipeline.serve(
    #     name="ETL Pipeline",
    #     cron="0 */8 * * *",
    # )
    etl_pipeline()