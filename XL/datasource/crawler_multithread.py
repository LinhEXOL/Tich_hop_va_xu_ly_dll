import argparse
import json
import time

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


def get_list_link(start, end):
    links = []
    for i in range(start, end + 1):
        links.append(f"https://www.careerlink.vn/vieclam/list?category_ids=130%2C19&page={i}")
    return links


def get_titles(link):
    response = requests.get(link)
    soup = BeautifulSoup(response.content, "html.parser")
    titles = soup.find_all('a', class_="job-link clickable-outside")
    return titles


def crawl_content(link):
    data = {}
    news = requests.get(f"https://www.careerlink.vn{link}")
    soup = BeautifulSoup(news.content, "html.parser")

    names_obj = soup.find('h1', class_="job-title mb-0")
    if names_obj is None:
        return None

    job_names = names_obj.text
    data['tên công việc'] = job_names

    company_name = soup.find('p', class_="org-name mb-2")
    data['tên công ty'] = company_name.get_text()

    # tmp_data = soup.find("div", class_="d-flex align-items-start mb-2")
    # tmp_data = tmp_data.get_text()
    # data['Địa điểm công việc'] = ' '.join(tmp_data.split())

    location_div = soup.find("div", id="job-location")  # ưu tiên id vì ổn định hơn class
    if location_div:
    # Lấy toàn bộ text bên trong, bao gồm <span> và <a>
        location_text = location_div.get_text(separator=" ", strip=True)
        data['Địa điểm công việc'] = location_text
    else:
        data['Địa điểm công việc'] = None

    # tmp_data = soup.find_all("div", class_="d-flex align-items-center mb-2")
    # luong = tmp_data[0].find('span', class_='text-primary')
    # data["Mức lương"] = luong.get_text()

    salary_div = soup.find("div", id="job-salary")
    if salary_div:
        salary_text = salary_div.find("span", class_="text-primary")
        data["Mức lương"] = salary_text.get_text(strip=True) if salary_text else None
    else:
        data["Mức lương"] = None

    # kinh_nghiem = tmp_data[1].find('span')
    # data["Kinh nghiệm"] = kinh_nghiem.get_text()

    icon = soup.find("i", class_="cli-suitcase-simple")
    if icon:
        span = icon.find_next_sibling("span")
        if span:
            data["Kinh nghiệm"] = span.get_text(strip=True)
        else:
            data["Kinh nghiệm"] = None
    else:
        data["Kinh nghiệm"] = None

    job_description = soup.find(id="section-job-description")
    job_des = job_description.find("div", class_="rich-text-content")
    data['mô tả công việc'] = job_des.get_text(strip=True)

    job_skill = soup.find(id="section-job-skills")
    skill = job_skill.find("div", class_="rich-text-content")
    data['kĩ năng yêu cầu'] = skill.get_text(strip=True)

    job_contact = soup.find(id="section-job-contact-information")
    content = job_contact.find("ul", class_="list-unstyled contact-person rounded-lg p-3 m-0")
    li_elements = content.select("ul.list-unstyled li")
    contact_array = []
    for li in li_elements:
        text_content = li.get_text(strip=True)
        contact_array.append(text_content)
    data['thông tin liên hệ'] = contact_array

    summarize = soup.find('div', class_="row job-summary d-flex")
    labels = summarize.find_all('div', class_='my-0 summary-label')
    for indx, label in enumerate(labels):
        if 'Loại công việc' in label.get_text():
            loai_con_viec = label.find_next_sibling('div').get_text()
            data['loại công việc'] = loai_con_viec

        elif 'Cấp bậc' in label.get_text():
            cap_bac = label.find_next_sibling('div').get_text()
            data['cấp bậc'] = cap_bac

        elif 'Học vấn' in label.get_text():
            hoc_van = label.find_next_sibling("div").get_text()
            data['học vấn'] = hoc_van

        elif 'Giới tính' in label.get_text():
            gioi_tinh = label.find_next_sibling('div').get_text()
            data['giới tính'] = gioi_tinh

        elif 'Tuổi' in label.get_text():
            tuoi = label.find_next_sibling('div').get_text()
            data['tuổi'] = tuoi
        elif 'Ngành nghề' in label.get_text():
            nganh_nghe = label.find_next_sibling('div').get_text()
            data['ngành nghề'] = nganh_nghe
    for title, value in data.items():
        if "\n" in value:
            data[title] = value.replace("\n", "")

    return data


def crawl_contents(filename, links_company):
    job_info = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(crawl_content, links_company))

    for result in results:
        if result is not None:
            job_info.append(result)

    with open(filename, 'w', encoding="utf-8") as f:
        json.dump(job_info, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    print("Parsing Args")
    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end")
    args = parser.parse_args()
    start_time = time.time()

    print("Start crawling from", args.start, "to", args.end)
    links = get_list_link(int(args.start), int(args.end))
    print("get list link", links)

    with ThreadPoolExecutor(max_workers=5) as executor:
        titles = list(executor.map(get_titles, links))
        
    

    links_company = [link['href'] for title in titles for link in title]
    print("get all link of company")

    

    filename = f"result/recruit_{args.start}_{args.end}.json"
    crawl_contents(filename, links_company)
    print(f'crawl data successful in {time.time()- start_time}')