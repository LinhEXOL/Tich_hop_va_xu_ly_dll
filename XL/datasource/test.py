import argparse
import json
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

response = requests.get("https://www.careerlink.vn/tim-viec-lam/nhan-vien-ho-tro-ky-thuat-dtdd-laptop-chau-thanh-an-giang/2737347?source=site")
soup = BeautifulSoup(response.content, "html.parser")
tmp_data = soup.find("div", class_="d-flex align-items-start mb-2")
text= tmp_data.get_text()
normalize_text= ' '.join(text.split())
print(normalize_text)

# In mảng nội dung văn bản


