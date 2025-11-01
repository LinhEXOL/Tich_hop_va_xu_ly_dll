import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

# Import the crawler utilities from the uploaded module
from scrape_topcv_company import crawl_to_dataframe

def make_chunks(start_page: int, end_page: int, pages_per_thread: int) -> List[Tuple[int, int]]:
    chunks = []
    p = start_page
    while p <= end_page:
        q = min(p + pages_per_thread - 1, end_page)
        chunks.append((p, q))
        p = q + 1
    return chunks

def export_json(df, out_path: str):
    # Same column mapping as the original script
    RENAME_MAP = {
        "stt": "stt",
        "detail_title": "tên công việc",
        "company_name_full": "công ty",
        "detail_location": "địa điểm làm việc",
        "detail_salary": "lương",
        "detail_experience": "yêu cầu kinh nghiệm",
        "desc_mota": "mô tả chi tiết",
        "desc_yeucau": "kĩ năng yêu cầu",
        "desc_quyenloi": "quyền lợi",
        "working_times": "thời gian làm việc",     # (fulltime, parttime, ...)
        "job_level": "cấp bậc",
        "education": "học vấn",
        "deadline": "hạn nộp",
    }
    src_cols = [c for c in RENAME_MAP.keys() if c in df.columns]
    out_df = df[src_cols].rename(columns=RENAME_MAP)
    records = out_df.to_dict(orient="records")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return len(records)

def process_chunk(qtpl: str, start: int, end: int, delay_min: float, delay_max: float, out_prefix: str):
    df = crawl_to_dataframe(qtpl, start_page=start, end_page=end, delay_between_pages=(delay_min, delay_max))
    out_path = f"{out_prefix}{start}_{end}.json"
    n = export_json(df, out_path)
    return (start, end, out_path, n)

def main():
    parser = argparse.ArgumentParser(description="Multithread TopCV crawl by page chunks.")
    parser.add_argument("--start", type=int, required=True, help="Start page, e.g., 1")
    parser.add_argument("--end", type=int, required=True, help="End page, e.g., 70")
    parser.add_argument("--pages-per-thread", type=int, default=10, help="How many pages per thread (chunk).")
    parser.add_argument("--workers", type=int, default=None, help="Number of worker threads. Default = number of chunks")
    parser.add_argument("--delay-min", type=float, default=0.5, help="Min delay between pages within a thread.")
    parser.add_argument("--delay-max", type=float, default=1.0, help="Max delay between pages within a thread.")
    parser.add_argument("--out-prefix", type=str, default="data", help="Output prefix, e.g., 'data' -> data1_10.json")
    parser.add_argument("--qtpl", type=str, default="https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?category_family=r257&page={page}",
                        help="Query URL template with {page} placeholder.")
    args = parser.parse_args()

    chunks = make_chunks(args.start, args.end, args.pages_per_thread)
    workers = args.workers or len(chunks)
    print(f"[INFO] Total chunks: {len(chunks)}; using workers={workers}")
    print(f"[INFO] Chunks: {chunks}")

    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [
            ex.submit(process_chunk, args.qtpl, s, e, args.delay_min, args.delay_max, args.out_prefix)
            for (s, e) in chunks
        ]
        for fut in as_completed(futs):
            results.append(fut.result())

    # Sort results by start page for a clean summary
    results.sort(key=lambda x: x[0])
    total = sum(n for _, _, _, n in results)
    print("\\n=== SUMMARY ===")
    for (s, e, path, n) in results:
        print(f" - {path}: pages {s}-{e}, {n} rows")
    print(f"TOTAL rows: {total}")

if __name__ == "__main__":
    main()