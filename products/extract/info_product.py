import requests
from bs4 import BeautifulSoup
import pandas 
import re
import time
import random

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Referer":"https://tiki.vn",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "x-guest-token": "yVUXcYNkAvt8IZ6mCnD5BQlxF4gKquTf"    
}

id_product_file = [
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

custom_filenames = [
    "camera.csv",
    "dienthoai.csv",
    "dienthoaiban.csv",
    "dienthoaiphothong.csv",
    "dieuhoa.csv",
    "laptop.csv",
    "mayanh.csv",
    "maydocsach.csv",
    "maygiat.csv",
    "maytinhbang.csv",
    "maytinhdeban.csv",
    "tivi.csv",
    "tulanh.csv"
]

All_id_product = []
All_product_info = []
file_index = 0


def remove_html_tags(text):
    text = BeautifulSoup(text, "html.parser").get_text()
    text1 = re.sub(r'\xa0', ' ', text)
    text2 =  re.sub(r"\n+", "\n",text1)
    return re.sub(r"\n +", "\n",text2)

def clean_special_char(s):
    cleaned = re.sub(r"[:()]", "", s)        
    cleaned = re.sub(r"\s+", " ", cleaned)    
    return cleaned.strip()

def get_product_link(id):
    url = f"https://tiki.vn/api/v2/products/{id}?platform=web&version=3"           ##query ngắn nên viết pẹ link luôn, ko dùng params nữa
    response = requests.get(url, headers=headers)
    time.sleep(random.randint(3, 8)/ 10)
    if response.status_code == 200:
        data = response.json()
        name = data.get("name", "Không có tên").strip()                                    
        description = data.get("description", "Không có mô tả")                  
        brand = data.get("brand", "Không rõ").get("name", "Không rõ")

        specifications = []
        for spec in data.get("specifications", []):
            for attribute in spec.get("attributes", []):
                key = attribute.get("name", "Không rõ").strip()
                value = attribute.get("value", "Không rõ").strip()
                specifications.append(f"{key}: {value}")
        
        options = data.get("configurable_options", [])
        name_option1 = ""
        name_option2 = ""
        for option in options:
            if option.get("code") == "option1":
                name_option1 = clean_special_char(option.get("name", ""))
            elif option.get("code") == "option2":
                name_option2 = clean_special_char(option.get("name", ""))

        variant_details = []
        config = data.get("configurable_products", [])
        if data.get("type","Không rõ") != "simple":
            if config:
                for variant in config:
                    value_option1 = clean_special_char(variant.get("option1",""))
                    value_option2 = clean_special_char(variant.get("option2",""))
                    price = variant.get("price","")
                    if value_option2 != "":
                        variant_details.append(f"{name_option1}: {value_option1} $$ {name_option2}: {value_option2} = {price} VND")
                    else: 
                        variant_details.append(f"{name_option1}: {value_option1} = {price} VND")
            else:
                variant_details.append(f"{data.get("original_price")} VND")
        else: 
            variant_details.append(f"{data.get("original_price")} VND")

        pro_images = []
        for image in data.get("images", []):
            img = image.get("base_url", "Không rõ").strip()
            pro_images.append(img)
        
        pattern = r"\b(bộ sạc|tai nghe|ốp lưng)\b"
        if re.search(pattern, name, flags=re.IGNORECASE):
            cate = "Phụ kiện"
        else: 
            cate = data.get("categories", "Không rõ").get("name","Không có").strip()
            
        product_info = {
            "Id": id,
            "Tên sản phẩm": name,
            "Thương hiệu": brand,
            "Danh mục": cate,
            "Thông số kỹ thuật": remove_html_tags("\n".join(specifications)),
            "Phiên bản": remove_html_tags("\n".join(variant_details)),
            "Mô tả": remove_html_tags(description),
            "Hình ảnh": "\n".join(pro_images)
        }
        All_product_info.append(product_info)
        print(len(All_product_info))

#-Main-

for item in id_product_file:
    print(f"Đang mở file {item}")
    id_file = pandas.read_csv(f"../data/id/{item}")

    All_id_product = []
    for id in id_file["id"]:
        All_id_product.append(id)
    
    All_product_info = []

    for id in All_id_product:
        print(f'Lấy sản phẩm id = {id}')
        get_product_link(id)
        time.sleep(random.randint(5, 15)/10)

    filename = custom_filenames[file_index]
    file_index += 1
    df = pandas.DataFrame(All_product_info)
    file_path = f"../data/product/{filename}"
    df.to_csv(file_path, index=False, sep=',', encoding='utf-8-sig')
    print(f"Đã lưu {len(All_product_info)} sản phẩm vào '{file_path}'\n")
    time.sleep(random.randint(30, 60)/10)