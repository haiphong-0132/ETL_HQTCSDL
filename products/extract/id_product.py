import requests
from bs4 import BeautifulSoup
import pandas
import re
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Referer":"https://tiki.vn",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "x-guest-token": "yVUXcYNkAvt8IZ6mCnD5BQlxF4gKquTf"    
}

payload = [
    {"urlKey": "camera-giam-sat", "category": "4077",},
    {"urlKey": "dien-thoai-smartphone", "category": "1795",},
    {"urlKey": "dien-thoai-ban", "category": "8061",},
    {"urlKey": "dien-thoai-pho-thong", "category": "1796",},
    {"urlKey": "may-lanh-may-dieu-hoa", "category": "3865",},
    {"urlKey": "laptop", "category": "8095",},
    {"urlKey": "may-anh", "category": "28806",},
    {"urlKey": "may-doc-sach", "category": "28856",},
    {"urlKey": "maygiat", "category": "3862",},
    {"urlKey": "may-tinh-bang", "category": "1794",},
    {"urlKey": "pc-may-tinh-bo", "category": "8093",},
    {"urlKey": "tivi", "category": "5015",},
    {"urlKey": "tu-lanh", "category": "2328",},
]

custom_filenames = [
    "camera_id.csv",
    "dienthoai_id.csv",
    "dienthoaiban_id.csv",
    "dienthoaiphothong_id.csv",
    "dieuhoa_id.csv",
    "laptop_id.csv",
    "mayanh_id.csv",
    "maydocsach_id.csv",
    "maygiat_id.csv",
    "maytinhbang_id.csv",
    "maytinhdeban_id.csv",
    "tivi_id.csv",
    "tulanh_id.csv"
]

file_index = 0
All_id_product = []

def remove_html_tags(text):
    text = BeautifulSoup(text, "html.parser").get_text()
    text1 = re.sub(r'\xa0', ' ', text)
    text2 =  re.sub(r"\n+", "\n",text1)
    return re.sub(r"\n +", "\n",text2)

def clean_special_char(s):
    cleaned = re.sub(r"[:()]", "", s)        
    cleaned = re.sub(r"\s+", " ", cleaned)    
    return cleaned.strip()


def get_tiki_products(urlKey, category, page):
    params = {
        "limit": "40",
        "include": "advertisement",
        "aggregations": "2",
        "version": "home-persionalized",
        "trackity_id": "b3f611e1-dd04-c19b-adc4-8637079771cb",
        "urlKey": urlKey,
        "category": category,
        "page": f"{page}"
    }
    response = requests.get("https://tiki.vn/api/personalish/v1/blocks/listings", headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        products = data.get("data", [])
        for product in products:
            id = product.get("id", "Không có id")
            All_id_product.append(id)
    else:
        print(f"Lỗi tại {urlKey} trang {page}")
        return False

#-Main-

for item in payload:
    All_id = {"id": []}
    All_id_product = []
    print(f"Danh mục: {item["urlKey"]} (category: {item["category"]})")   

    # max_pages = get_max_pages(item['urlKey'], item['category'])
    max_pages = 10
    for i in range(1, max_pages +1 ):
        print(f"Trang {i}")
        get_tiki_products(item['urlKey'], item['category'], page=i)
        time.sleep(1)

    All_id["id"] = All_id_product

    filename = custom_filenames[file_index]
    file_index += 1

    df = pandas.DataFrame(All_id)
    file_path = f"../data/id/{filename}"
    df.to_csv(file_path, index=False, sep=',', encoding='utf-8-sig')
    print(f"Đã lưu {len(All_id)} sản phẩm vào '{file_path}'\n")