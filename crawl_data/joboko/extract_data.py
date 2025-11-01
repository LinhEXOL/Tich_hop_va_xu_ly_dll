import re, json, time
from pathlib import Path
from typing import Tuple, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup, NavigableString, Tag


INPUT_JSON   = "joboko_links_titles.json"
OUTPUT_JSON  = "joboko_job_details_SPEC.json"
LIMIT        = 0       # 0 = all; >0 = only first N items
DELAY        = 0.5     # seconds between requests
TIMEOUT      = 30.0    # HTTP timeout

INDUSTRY_CONST = "IT / Phần mềm / IOT / Điện tử viễn thông"

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return s

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

def get_company_and_location(soup: BeautifulSoup) -> Tuple[str, str]:
    # Company
    cong_ty = ""
    el_company = soup.select_one(".nw-company-hero__info h2 a")
    if el_company:
        cong_ty = norm_text(el_company.get_text(strip=True))

    # Location
    dia_diem = ""
    el_addr = soup.select_one(".nw-company-hero__address")
    if el_addr:
        a = el_addr.find("a")
        if a and a.get_text(strip=True):
            dia_diem = norm_text(a.get_text(strip=True))
        else:
            dia_diem = norm_text(el_addr.get_text(" ", strip=True))
        dia_diem = re.sub(r"(?i)^địa điểm làm việc:\s*", "", dia_diem).strip(" .")
    return cong_ty, dia_diem

def extract_label_value(soup: BeautifulSoup, label_keywords: List[str]) -> str:
    """
    Find a block like: <div class='item-content'> {Label}: <span class='fw-bold'>Value</span>
    Return Value. Search within '.nw-job-list__main .block-entry .item'.
    """
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

# -------- Section extraction (Mô tả công việc / Yêu cầu) --------
SECTION_STOP_TITLES = [
    "yêu cầu", "quyền lợi", "quyền lợi được hưởng", "thông tin khác",
    "địa điểm", "cách thức ứng tuyển", "thông tin"
]

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
    # Prefer specific heading "Mô tả công việc"
    for h in scope.select("h1, h2, h3, h4, h5, strong, b"):
        title = norm_text(h.get_text(" ", strip=True))
        if text_contains(title, "Mô tả công việc") or title.lower() == "mô tả":
            body = collect_following_text(h)
            if body:
                return body
    # Fallback: large content block
    block = scope.select_one(".nw-wysiwyg")
    if block:
        return norm_text(block.get_text(" ", strip=True))
    return ""

def extract_requirements(soup: BeautifulSoup) -> str:
    scope = soup.select_one(".nw-job-list__main") or soup
    # SPEC: <h3>Yêu cầu</h3><div class='text-left job-requirement'>...</div>
    h3_req = None
    for h in scope.select("h3"):
        if text_contains(norm_text(h.get_text(' ', strip=True)), "Yêu cầu"):
            h3_req = h
            break
    if h3_req:
        # direct sibling div.job-requirement
        sib = h3_req.find_next_sibling(lambda t: isinstance(t, Tag) and "job-requirement" in (t.get("class") or []))
        if sib:
            return norm_text(sib.get_text(" ", strip=True))
        # else, collect until next heading
        body = collect_following_text(h3_req)
        if body:
            return body
    # Fallback: heading-like 'Yêu cầu'
    for h in scope.select("h1, h2, h3, h4, h5, strong, b"):
        title = norm_text(h.get_text(" ", strip=True))
        if text_contains(title, "Yêu cầu"):
            body = collect_following_text(h)
            if body:
                return body
    return ""

# -------- Company size ("Quy mô công ty") --------
def extract_company_size(soup: BeautifulSoup) -> str:
    # Find heading "Quy mô công ty" then next sibling with class .nw-job-detail__text
    for head in soup.select(".nw-job-detail__heading, h2, h3, h4, strong, b, span"):
        title = norm_text(head.get_text(" ", strip=True))
        if text_contains(title, "Quy mô công ty"):
            # Walk next siblings to find div.nw-job-detail__text (or .nw-wysiwyg .nw-job-detail__text)
            node = head.parent.next_sibling if head.name != "div" else head.next_sibling
            # generalized walk
            cur = head
            # Try immediate next blocks first
            for sib in (head.next_sibling, head.parent.next_sibling if head.parent else None):
                if isinstance(sib, Tag):
                    target = sib if "nw-job-detail__text" in (sib.get("class") or []) else sib.select_one(".nw-job-detail__text")
                    if target:
                        val = norm_text(target.get_text(" ", strip=True))
                        if val:
                            return val
            # Broader search in vicinity
            container = head.find_parent()
            if container:
                target = container.find(class_=lambda c: c and "nw-job-detail__text" in c)
                if target:
                    val = norm_text(target.get_text(" ", strip=True))
                    if val:
                        return val
    # Fallback: regex scan
    t = norm_text(soup.get_text(" ", strip=True))
    m = re.search(r"Quy mô công ty\s*:?\s*([^\n]+)", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""

# -------- Deadline ("Hạn nộp") --------
def extract_deadline(soup: BeautifulSoup) -> str:
    # Prefer exact div.mt-1.fz-16 containing 'Hạn nộp:'
    for el in soup.select("div.mt-1.fz-16"):
        txt = norm_text(el.get_text(" ", strip=True))
        if "Hạn nộp" in txt:
            return txt.split(":", 1)[-1].strip()
    # Fallback: any text node with 'Hạn nộp'
    for txt_node in soup.find_all(string=re.compile(r"Hạn nộp", flags=re.IGNORECASE)):
        full = norm_text(txt_node.parent.get_text(" ", strip=True))
        if ":" in full:
            return full.split(":", 1)[-1].strip()
        else:
            # maybe sibling holds date
            sib = txt_node.parent.find_next_sibling()
            if isinstance(sib, Tag):
                stext = norm_text(sib.get_text(" ", strip=True))
                if stext:
                    return stext
    return ""

data = json.loads(Path(INPUT_JSON).read_text(encoding="utf-8"))
if not isinstance(data, list):
    raise SystemExit("Input JSON should be a list of objects.")

sess = make_session()

out_rows = []
for idx, item in enumerate(data, start=1):
    if LIMIT and idx > LIMIT:
        break

    stt = item.get("stt")
    ten = item.get("ten", "")
    link = item.get("link", "")
    if not link:
        print(f"[SKIP] Missing link for stt={stt}")
        continue

    soup = None
    try:
        resp = sess.get(link, timeout=TIMEOUT)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
        else:
            print(f"[WARN] HTTP {resp.status_code} -> {link}")
    except Exception as e:
        print(f"[ERR] {e!r} -> {link}")

    cong_ty = dia_diem = muc_luong = kinh_nghiem = mo_ta = ky_nang = loai_cong_viec = cap_bac = quy_mo = han_nop = ""
    if soup:
        cong_ty, dia_diem = get_company_and_location(soup)
        muc_luong = get_income(soup)
        kinh_nghiem = get_experience(soup)
        mo_ta = extract_desc(soup)
        ky_nang = extract_requirements(soup)
        loai_cong_viec = get_job_type(soup)
        cap_bac = get_job_level(soup)
        quy_mo = extract_company_size(soup)
        han_nop = extract_deadline(soup)

    row = {
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
    out_rows.append(row)

    print(f"{stt}. {ten}")
    time.sleep(DELAY)

Path(OUTPUT_JSON).write_text(json.dumps(out_rows, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[DONE] Saved {len(out_rows)} items -> {OUTPUT_JSON}")
