import requests
from bs4 import BeautifulSoup
import pandas
import re
import time
import os
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Referer":"https://tiki.vn",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "x-guest-token": "yVUXcYNkAvt8IZ6mCnD5BQlxF4gKquTf"    
}

id_product_file = [
    # "camera_id.csv",
    # "dienthoai_id.csv",
    # "dienthoaiban_id.csv",
    # "dienthoaiphothong_id.csv",
    # "dieuhoa_id.csv",
    # "laptop_id.csv",
    # "mayanh_id.csv",
    # "maydocsach_id.csv",
    # "maygiat_id.csv",
    # "maytinhbang_id.csv",
    # "maytinhdeban_id.csv",
    "tivi_id.csv",
    "tulanh_id.csv"
]

feedback_file = [
    # "camera_fb.csv",
    # "dienthoai_fb.csv",
    # "dienthoaiban_fb.csv",
    # # "dienthoaiphothong_fb.csv",
    # "dieuhoa_fb.csv",
    # "laptop_fb.csv",
    # "mayanh_fb.csv",
    # "maydocsach_fb.csv",
    # "maygiat_fb.csv",
    # "maytinhbang_fb.csv",
    # "maytinhdeban_fb.csv",
    "tivi_fb.csv",
    "tulanh_fb.csv"
]
feedback_manager_file = [
    # "camera_fb_ma.csv",
    # "dienthoai_fb_ma.csv",
    # "dienthoaiban_fb_ma.csv",
    # "dienthoaiphothong_fb_ma.csv",
    # "dieuhoa_fb_ma.csv",
    # "laptop_fb_ma.csv",
    # "mayanh_fb_ma.csv",
    # "maydocsach_fb_ma.csv",
    # "maygiat_fb_ma.csv",
    # "maytinhbang_fb_ma.csv",
    # "maytinhdeban_fb_ma.csv",
    "tivi_fb_ma.csv",
    "tulanh_fb_ma.csv"
]

All_feedback = []
All_manager_feedback = []
file_index = 0

def remove_html_tags(text):
    text = BeautifulSoup(text, "html.parser").get_text()
    text1 = re.sub(r'\xa0', ' ', text)
    text2 =  re.sub(r"\n+", "\n",text1)
    return re.sub(r"\n +", "\n",text2)

def clean_special_char(s):
    cleaned = re.sub(r"[()]", "", s)        
    cleaned = re.sub(r"\s+", " ", cleaned)    
    return cleaned.strip()

def get_total_feedback_pages(product_id):
    url = "https://tiki.vn/api/v2/reviews"
    params = {
        "limit": 5,
        "include": "comments,contribute_info,attribute_vote_summary",
        "sort": "score|desc,id|desc,stars|all",
        "page": 1,
        "product_id": product_id,
        "seller_id": 1
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        last_page = data.get("paging", {}).get("last_page", 0)
        return last_page
    else:
        print(f"Không lấy được số trang feedback cho sản phẩm {product_id}")
        return 0

def get_feedbacks(product_id, pages):
    for page in range(1, pages+1):
        params = {
            "limit": 5,
            "include": "comments,contribute_info,attribute_vote_summary",
            "sort": "score|desc,id|desc,stars|all",
            "page": page,
            "product_id": product_id,
            "seller_id": "1"
        }
        response = requests.get("https://tiki.vn/api/v2/reviews", headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            for review in data.get("data", []):
                product_attributes = []
                for attr in review.get("product_attributes",[]):
                    product_attributes.append(clean_special_char(attr))
                
                image_url = []
                for imgage in review.get("images", []):
                    image_url.append(imgage.get("full_path", "Không rõ"))

                feedback = {
                    "feedback_id": review.get("id", "Không rõ"),
                    "product_id": product_id,
                    "customer_id": review.get("customer_id", "Không rõ"),
                    "rating": review.get("rating", "Không rõ"),
                    "content": remove_html_tags(review.get("content", "")).strip(),
                    'time': review.get("timeline", {}).get("review_created_date", ""),
                    "variant": "$$".join(product_attributes),
                    "image_url":"\n".join(image_url),
                }
                
                for com in review.get("comments",[]):
                    if com.get("commentator",[]) != "customer":
                        manager_feedback = {
                            "id": com.get("id","Không rõ"),
                            "feedback_id": com.get("review_id", "Không rõ"),
                            "manager_id": com.get("customer_id", "Không rõ"),
                            "manager_name": com.get("fullname", "Không rõ"),
                            "content": remove_html_tags(com.get("content", "")).strip(),
                            'time': review.get("timeline", {}).get("review_created_date", ""),

                        }
                        All_manager_feedback.append(manager_feedback)

                All_feedback.append(feedback)
        else:
            print(f" Không lấy được feedback cho sản phẩm {product_id} trang {page}")
            break

#-Main-

for item in id_product_file:
    print(f"Đang mở file {item}")
    id_file = pandas.read_csv(f"../data/id/{item}")
    
    All_id_product = []
    for id in id_file["id"]:
        All_id_product.append(id)

    All_feedback = []
    All_manager_feedback = []

    for id in All_id_product:
        print(f'Lấy sản phẩm id = {id}')
        max_page = 3 
        get_feedbacks(id,max_page)
        time.sleep(0.5)

    filename = feedback_file[file_index]
    filename_2 = feedback_manager_file[file_index]

    file_index += 1
    df = pandas.DataFrame(All_feedback)
    df2 = pandas.DataFrame(All_manager_feedback)

    file_path = f"../data/feedback/{filename}"
    file_path2 = f"../data/feedback_manager/{filename_2}"
    df.to_csv(file_path, index=False, sep=',', encoding='utf-8-sig')
    df2.to_csv(file_path2, index=False, sep=',', encoding='utf-8-sig')
    print(f"Đã lưu {len(All_feedback)} feedback vào '{file_path}'\n")
    print(f"Đã lưu {len(All_manager_feedback)} feedback vào '{file_path2}'\n")

