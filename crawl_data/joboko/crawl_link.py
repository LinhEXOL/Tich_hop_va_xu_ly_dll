# -*- coding: utf-8 -*-
import re, time, json, csv
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

BASE_DOMAIN = "https://vn.joboko.com"

def build_url_with_page(base_url: str, page: int) -> str:
    u = urlparse(base_url)
    qs = dict(parse_qsl(u.query, keep_blank_values=True))
    qs["p"] = str(page)
    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def extract_links(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("div.nw-job-list__list .item .item-head .item-title a")
    if not anchors:
        anchors = soup.select('a[href*="/viec-lam-"][href*="-xvi"]')
    links = []
    pat = re.compile(r"/viec-lam-[\w\-]+-xvi\d+", re.IGNORECASE)
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href or not pat.search(href):
            continue
        if href.startswith("/"):
            href = BASE_DOMAIN + href
        elif not href.startswith("http"):
            href = BASE_DOMAIN.rstrip("/") + "/" + href.lstrip("/")
        links.append(href)
    # de-dup giữ thứ tự
    seen, uniq = set(), []
    for x in links:
        if x not in seen:
            seen.add(x); uniq.append(x)
    return uniq

def parse_total_jobs(html: str) -> int:
    m = re.search(r"\(([\d\.,]+)\s*việc\)", html, flags=re.IGNORECASE)
    if not m: return -1
    raw = m.group(1).replace(".", "").replace(",", "")
    try: return int(raw)
    except: return -1

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

def extract_link_title_pairs(html: str) -> list[tuple[str, str]]:
    """
    Trả về list các tuple (full_link, job_title_text)
    """
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

base_url   = "https://vn.joboko.com/jobs?ind=124&jt=1"
start_page = 1
max_pages  = 2
delay      = 0.75
out_json   = "joboko_links_titles.json"
out_csv    = "joboko_links_titles.csv"
save_page_urls = ""  


if start_page < 1:
    raise SystemExit("--start-page must be >= 1")

sess = make_session()
all_rows = []      
seen_links = set()
total_jobs_seen = -1
page_urls = []

stt = 0
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

    if total_jobs_seen > 0 and len(all_rows) >= total_jobs_seen:
        print("[INFO] Reached/Exceeded reported total jobs. Early stop.")
        break

    time.sleep(delay)


with open(out_json, "w", encoding="utf-8") as f:
    json.dump(all_rows, f, ensure_ascii=False, indent=2)
print(f"[DONE] Saved {len(all_rows)} items -> {out_json}")

with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["stt", "tên việc", "link"])
    for r in all_rows:
        writer.writerow([r["stt"], r["ten"], r["link"]])
print(f"[DONE] Saved CSV -> {out_csv}")

if save_page_urls:
    with open(save_page_urls, "w", encoding="utf-8") as f:
        f.write("\n".join(page_urls))
    print(f"[DONE] Saved visited page URLs -> {save_page_urls}")
