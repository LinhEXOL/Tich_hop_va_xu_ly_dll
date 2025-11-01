#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gộp từ crawl_link.py và extract_data.py thành 1 file:
- Thực hiện crawl các trang danh sách (links + titles)
- Sau đó extract chi tiết từng việc và lưu ra JSON

Sử dụng: python crawl_and_extract.py
Tùy chỉnh các hằng số bên dưới để thay đổi phạm vi / delay / limit.
"""
import re, time, json, csv
from pathlib import Path
from typing import Tuple, List, Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup, NavigableString, Tag
import concurrent.futures
import random

# --- Configuration (tweak as needed) ---
BASE_DOMAIN = "https://vn.joboko.com"
BASE_LIST_URL = "https://vn.joboko.com/jobs?ind=124&jt=1"

# Crawl settings
START_PAGE = 1
MAX_PAGES = 1000
CRAWL_DELAY = 0.75
MAX_CONSECUTIVE_NO_NEW = 10  # nếu gặp N trang liên tiếp không có item mới -> dừng
START_LOCATION = 1
MAX_LOCATION = 1000  # safety cap; crawl locations from START_LOCATION..MAX_LOCATION

# Output files for the crawl step
OUT_JSON = "joboko_links_titles.json"
OUT_CSV = "joboko_links_titles.csv"

# Extraction settings
INPUT_JSON = OUT_JSON
OUTPUT_JSON = "joboko_job_details_SPEC.json"
LIMIT = 0       # 0 = all; >0 = only first N items
DELAY = 0.5     # seconds between requests when extracting details (used as jitter factor per worker)
TIMEOUT = 30.0  # HTTP timeout
MAX_WORKERS = 5  # number of threads for parallel extraction
RESULT_DIR = "result"  # directory to save per-location detail JSON files
OUTPUT_DIR = "output"  # directory to save crawl outputs (json/csv) and other non-detail files

INDUSTRY_CONST = "IT / Phần mềm / IOT / Điện tử viễn thông"

SECTION_STOP_TITLES = [
    "yêu cầu", "quyền lợi", "quyền lợi được hưởng", "thông tin khác",
    "địa điểm", "cách thức ứng tuyển", "thông tin"
]


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return s


def build_url_with_page(base_url: str, page: int) -> str:
    u = urlparse(base_url)
    qs = dict(parse_qsl(u.query, keep_blank_values=True))
    qs["p"] = str(page)
    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def build_list_url_with_location(base_url: str, location: int) -> str:
    """Return base_url with query param l=location (replace or add)."""
    u = urlparse(base_url)
    qs = dict(parse_qsl(u.query, keep_blank_values=True))
    qs["l"] = str(location)
    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s


def extract_link_title_pairs(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("div.nw-job-list__list .item .item-head .item-title a")
    if not anchors:
        anchors = soup.select('a[href*="/viec-lam-"][href*="-xvi"]')
    items = []
    pat = re.compile(r"/viec-lam-[\w\-]+-xvi\d+", re.IGNORECASE)
    for a in anchors:
        href = (a.get("href") or "").strip()
        title = (a.get_text(strip=True) or "").strip()
        if not href or not pat.search(href):
            continue
        if href.startswith("/"):
            href = BASE_DOMAIN + href
        elif not href.startswith("http"):
            href = BASE_DOMAIN.rstrip("/") + "/" + href.lstrip("/")
        items.append((href, title))
    # de-dup theo link, vẫn giữ thứ tự
    seen, uniq = set(), []
    for link, title in items:
        if link not in seen:
            seen.add(link); uniq.append((link, title))
    return uniq


def parse_total_jobs(html: str) -> int:
    m = re.search(r"\(([\d\.,]+)\s*việc\)", html, flags=re.IGNORECASE)
    if not m: return -1
    raw = m.group(1).replace(".", "").replace(",", "")
    try: return int(raw)
    except: return -1


# ---------- Extraction helpers (from extract_data.py) ----------
def get_company_and_location(soup: BeautifulSoup) -> Tuple[str, str]:
    cong_ty = ""
    el_company = soup.select_one(".nw-company-hero__info h2 a")
    if el_company:
        cong_ty = norm_text(el_company.get_text(strip=True))

    dia_diem = ""
    el_addr = soup.select_one(".nw-company-hero__Address" ) if False else soup.select_one(".nw-company-hero__address")
    if el_addr:
        a = el_addr.find("a")
        if a and a.get_text(strip=True):
            dia_diem = norm_text(a.get_text(strip=True))
        else:
            dia_diem = norm_text(el_addr.get_text(" ", strip=True))
        dia_diem = re.sub(r"(?i)^địa điểm làm việc:\s*", "", dia_diem).strip(" .")
    return cong_ty, dia_diem


def extract_label_value(soup: BeautifulSoup, label_keywords: List[str]) -> str:
    for item in soup.select(".nw-job-list__main .block-entry .item"):
        text_container = item.select_one(".item-content")
        if not text_container:
            continue
        label = norm_text(text_container.get_text(" ", strip=True))
        strong = item.select_one(".fw-bold")
        value = norm_text(strong.get_text(strip=True)) if strong else ""

        for kw in label_keywords:
            if kw.lower() in label.lower():
                return value or label.replace(kw + ":", "").strip()
    return ""


def get_income(soup: BeautifulSoup) -> str:
    return extract_label_value(soup, ["Thu nhập", "Mức lương", "Lương"])


def get_experience(soup: BeautifulSoup) -> str:
    return extract_label_value(soup, ["Kinh nghiệm"])


def get_job_type(soup: BeautifulSoup) -> str:
    return extract_label_value(soup, ["Loại hình"])


def get_job_level(soup: BeautifulSoup) -> str:
    return extract_label_value(soup, ["Chức vụ", "Cấp bậc"])


def is_heading_like(el: Tag) -> bool:
    return el.name in ("h1","h2","h3","h4","h5","strong","b")


def text_contains(hay: str, needle: str) -> bool:
    return needle.lower() in hay.lower()


def collect_following_text(start: Tag) -> str:
    pieces: List[str] = []
    node = start.next_sibling
    while node:
        if isinstance(node, NavigableString):
            node = node.next_sibling
            continue
        if isinstance(node, Tag):
            if is_heading_like(node):
                t = norm_text(node.get_text(" ", strip=True))
                if any(text_contains(t, stop) for stop in SECTION_STOP_TITLES):
                    break
            if node.name in ("p", "div", "ul", "ol"):
                text = norm_text(node.get_text(" ", strip=True))
                if text:
                    pieces.append(text)
        node = node.next_sibling
    return "\n".join(pieces).strip()


def extract_desc(soup: BeautifulSoup) -> str:
    scope = soup.select_one(".nw-job-list__main") or soup
    for h in scope.select("h1, h2, h3, h4, h5, strong, b"):
        title = norm_text(h.get_text(" ", strip=True))
        if text_contains(title, "Mô tả công việc") or title.lower() == "mô tả":
            body = collect_following_text(h)
            if body:
                return body
    block = scope.select_one(".nw-wysiwyg")
    if block:
        return norm_text(block.get_text(" ", strip=True))
    return ""


def extract_requirements(soup: BeautifulSoup) -> str:
    scope = soup.select_one(".nw-job-list__main") or soup
    h3_req = None
    for h in scope.select("h3"):
        if text_contains(norm_text(h.get_text(' ', strip=True)), "Yêu cầu"):
            h3_req = h
            break
    if h3_req:
        sib = h3_req.find_next_sibling(lambda t: isinstance(t, Tag) and "job-requirement" in (t.get("class") or []))
        if sib:
            return norm_text(sib.get_text(" ", strip=True))
        body = collect_following_text(h3_req)
        if body:
            return body
    for h in scope.select("h1, h2, h3, h4, h5, strong, b"):
        title = norm_text(h.get_text(" ", strip=True))
        if text_contains(title, "Yêu cầu"):
            body = collect_following_text(h)
            if body:
                return body
    return ""


def extract_company_size(soup: BeautifulSoup) -> str:
    for head in soup.select(".nw-job-detail__heading, h2, h3, h4, strong, b, span"):
        title = norm_text(head.get_text(" ", strip=True))
        if text_contains(title, "Quy mô công ty"):
            for sib in (head.next_sibling, head.parent.next_sibling if head.parent else None):
                if isinstance(sib, Tag):
                    target = sib if "nw-job-detail__text" in (sib.get("class") or []) else sib.select_one(".nw-job-detail__text")
                    if target:
                        val = norm_text(target.get_text(" ", strip=True))
                        if val:
                            return val
            container = head.find_parent()
            if container:
                target = container.find(class_=lambda c: c and "nw-job-detail__text" in c)
                if target:
                    val = norm_text(target.get_text(" ", strip=True))
                    if val:
                        return val
    t = norm_text(soup.get_text(" ", strip=True))
    m = re.search(r"Quy mô công ty\s*:?\s*([^\n]+)", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def extract_deadline(soup: BeautifulSoup) -> str:
    for el in soup.select("div.mt-1.fz-16"):
        txt = norm_text(el.get_text(" ", strip=True))
        if "Hạn nộp" in txt:
            return txt.split(":", 1)[-1].strip()
    for txt_node in soup.find_all(string=re.compile(r"Hạn nộp", flags=re.IGNORECASE)):
        full = norm_text(txt_node.parent.get_text(" ", strip=True))
        if ":" in full:
            return full.split(":", 1)[-1].strip()
        else:
            sib = txt_node.parent.find_next_sibling()
            if isinstance(sib, Tag):
                stext = norm_text(sib.get_text(" ", strip=True))
                if stext:
                    return stext
    return ""


def crawl_list_pages(base_url: str, start_page: int, max_pages: int, delay: float,
                     out_json: Optional[str] = None, out_csv: Optional[str] = None) -> list[dict]:
    sess = make_session()
    all_rows = []
    seen_links = set()
    total_jobs_seen = -1
    page_urls = []
    stt = 0
    consecutive_no_new = 0
    for p in range(start_page, max_pages + 1):
        url = build_url_with_page(base_url, p)
        page_urls.append(url)
        try:
            resp = sess.get(url, timeout=30)
        except Exception as e:
            print(f"[WARN] Page {p} request failed: {e!r}, continue.")
            time.sleep(delay); continue

        if resp.status_code != 200:
            print(f"[WARN] Page {p} -> HTTP {resp.status_code}, skip.")
            time.sleep(delay); continue

        html = resp.text
        if p == start_page:
            total_jobs_seen = parse_total_jobs(html)
            if total_jobs_seen > 0:
                print(f"[INFO] Reported total jobs: {total_jobs_seen:,}")

        pairs = extract_link_title_pairs(html)
        if not pairs:
            print(f"[INFO] Page {p} has 0 items.")
            new_count = 0
        else:
            new_count = 0
            for link, title in pairs:
                if link in seen_links:
                    continue
                seen_links.add(link)
                stt += 1
                row = {"stt": stt, "ten": title, "link": link}
                all_rows.append(row)
                new_count += 1
                print(f"{stt}. {title}  |  {link}")
            print(f"[OK] Page {p}: {len(pairs)} items (new {new_count}). Total so far: {len(all_rows)}")

        # track consecutive pages that added zero new items
        if new_count == 0:
            consecutive_no_new += 1
            print(f"[INFO] Consecutive pages with no new items: {consecutive_no_new}")
        else:
            consecutive_no_new = 0

        # If we exceed the configured threshold, stop early.
        # Default MAX_CONSECUTIVE_NO_NEW = 10. This triggers when consecutive_no_new > MAX_CONSECUTIVE_NO_NEW
        if consecutive_no_new > MAX_CONSECUTIVE_NO_NEW:
            print(f"[INFO] Reached {consecutive_no_new} consecutive pages with no new items -> early stop.")
            break

        if total_jobs_seen > 0 and len(all_rows) >= total_jobs_seen:
            print("[INFO] Reached/Exceeded reported total jobs. Early stop.")
            break

        time.sleep(delay)

    # save outputs (allow per-call filenames)
    out_json = out_json or OUT_JSON
    out_csv = out_csv or OUT_CSV
    # ensure parent dir exists
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)
    print(f"[DONE] Saved {len(all_rows)} items -> {out_json}")
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["stt", "tên việc", "link"])
        for r in all_rows:
            writer.writerow([r["stt"], r["ten"], r["link"]])
    print(f"[DONE] Saved CSV -> {out_csv}")

    return all_rows


def extract_details(rows: list[dict], limit: int = 0, max_workers: int = 5, delay: float = 0.5, timeout: float = 30.0,
                    output_json: Optional[str] = None) -> list[dict]:
    """
    Parallel extraction of job detail pages using a thread pool.

    - `rows` should be a list of dicts with keys: 'stt', 'ten', 'link'
    - Results are returned as a list of dicts (one per job) and also written to OUTPUT_JSON.
    """
    out_rows = []
    items = rows if not limit else rows[:limit]

    def worker(item: dict) -> dict:
        # small random jitter before each request to avoid exact bursts
        time.sleep(delay * random.random())
        sess = make_session()
        stt = item.get("stt")
        ten = item.get("ten", "")
        link = item.get("link", "")
        cong_ty = dia_diem = muc_luong = kinh_nghiem = mo_ta = ky_nang = loai_cong_viec = cap_bac = quy_mo = han_nop = ""
        if not link:
            print(f"[SKIP] Missing link for stt={stt}")
            return {
                "stt": stt,
                "ten_cong_viec": ten,
                "ten_cong_ty": cong_ty,
                "dia_diem_lam_viec": dia_diem,
                "muc_luong": muc_luong,
                "kinh_nghiem": kinh_nghiem,
                "mo_ta_chi_tiet": mo_ta,
                "ki_nang_yeu_cau": ky_nang,
                "loai_cong_viec": loai_cong_viec,
                "cap_bac": cap_bac,
                "quy_mo": quy_mo,
                "han_nop": han_nop,
                "nganh_nghe": INDUSTRY_CONST,
                "link": link
            }

        try:
            resp = sess.get(link, timeout=timeout)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                cong_ty, dia_diem = get_company_and_location(soup)
                muc_luong = get_income(soup)
                kinh_nghiem = get_experience(soup)
                mo_ta = extract_desc(soup)
                ky_nang = extract_requirements(soup)
                loai_cong_viec = get_job_type(soup)
                cap_bac = get_job_level(soup)
                quy_mo = extract_company_size(soup)
                han_nop = extract_deadline(soup)
            else:
                print(f"[WARN] HTTP {resp.status_code} -> {link}")
        except Exception as e:
            print(f"[ERR] {e!r} -> {link}")

        return {
            "stt": stt,
            "ten_cong_viec": ten,
            "ten_cong_ty": cong_ty,
            "dia_diem_lam_viec": dia_diem,
            "muc_luong": muc_luong,
            "kinh_nghiem": kinh_nghiem,
            "mo_ta_chi_tiet": mo_ta,
            "ki_nang_yeu_cau": ky_nang,
            "loai_cong_viec": loai_cong_viec,
            "cap_bac": cap_bac,
            "quy_mo": quy_mo,
            "han_nop": han_nop,
            "nganh_nghe": INDUSTRY_CONST,
            "link": link
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, it): it for it in items}
        for fut in concurrent.futures.as_completed(futures):
            try:
                row = fut.result()
            except Exception as e:
                print(f"[ERR] Worker raised: {e!r}")
                continue
            out_rows.append(row)
            # keep user informed
            print(f"{row.get('stt')}. {row.get('ten_cong_viec')}")

    # maintain original ordering by 'stt'
    out_rows.sort(key=lambda x: (x.get('stt') or 0))

    out_path = output_json or OUTPUT_JSON
    Path(out_path).write_text(json.dumps(out_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] Saved {len(out_rows)} items -> {out_path}")
    return out_rows


def main():
    print("Starting combined crawl + extract by location (l)")
    # ensure result dir exists
    Path(RESULT_DIR).mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    for loc in range(START_LOCATION, MAX_LOCATION + 1):
        list_url = build_list_url_with_location(BASE_LIST_URL, loc)
        print(f"\n=== Crawling location l={loc} -> {list_url} ===")
        out_json = str(Path(OUTPUT_DIR) / f"joboko_links_titles_l{loc}.json")
        out_csv = str(Path(OUTPUT_DIR) / f"joboko_links_titles_l{loc}.csv")
        crawled = crawl_list_pages(list_url, START_PAGE, MAX_PAGES, CRAWL_DELAY, out_json=out_json, out_csv=out_csv)

        if not crawled:
            print(f"[INFO] No items crawled for location l={loc}. Stopping location loop.")
            break

        # after each location crawl, run extraction for that location and save per-location detail file under RESULT_DIR
        detail_out = str(Path(RESULT_DIR) / f"joboko_job_details_l{loc}.json")
        print(f"[INFO] Extracting details for l={loc} ({len(crawled)} items) -> {detail_out}")
        extract_details(crawled, limit=LIMIT, max_workers=MAX_WORKERS, delay=DELAY, timeout=TIMEOUT, output_json=detail_out)
        # small pause between locations
        time.sleep(1.0)


if __name__ == '__main__':
    main()
