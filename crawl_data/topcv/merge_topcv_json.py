#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_topcv_json.py — Gộp các file JSON chunk thành 1 file duy nhất (bản không trace)
----------------------------------------------------------------------------------
Tính năng:
  • Gộp nhiều file JSON theo glob pattern
  • Loại trùng theo khóa ưu tiên: 'Liên_kết' → 'url' → bộ 3 ('tên công việc','công ty','địa điểm làm việc')
  • Tùy chọn đánh lại STT 1..N sau khi gộp
  • Tùy chọn xuất thêm CSV

Ví dụ:
  python merge_topcv_json.py --pattern "data*.json" --out all_jobs.json --renumber-stt --stt-field stt
  python merge_topcv_json.py --pattern "data*_*.json" --out all_jobs.json --to-csv --csv-out all_jobs.csv
"""
import argparse
import glob
import json
import os
import re
import unicodedata
from typing import Dict, List, Optional

def _norm_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFC", str(s)).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def composite_key(row: Dict) -> str:
    t = _norm_text(row.get("tên công việc"))
    c = _norm_text(row.get("công ty"))
    d = _norm_text(row.get("địa điểm làm việc", row.get("địa điểm")))
    return f"{t}__{c}__{d}"

def load_json_file(path: str) -> List[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[WARN] Bỏ qua {path}: {e}")
        return []

def write_json(rows: List[Dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def pick_dedup_key(rows: List[Dict], prefer_key: Optional[str]) -> Optional[str]:
    if prefer_key:
        return prefer_key
    if not rows or not isinstance(rows[0], dict):
        return None
    if "Liên_kết" in rows[0]:
        return "Liên_kết"
    if "url" in rows[0]:
        return "url"
    return None

def merge_files(
    pattern: str,
    out_path: str,
    dedup_key: Optional[str] = None,
    renumber_stt: bool = False,
    stt_field: str = "stt",
    to_csv: bool = False,
    csv_path: str = "all_jobs.csv",
) -> int:
    paths = sorted(glob.glob(pattern))
    if not paths:
        print(f"[ERR] Không tìm thấy file nào khớp pattern: {pattern}")
        return 0

    paths = [p for p in paths if os.path.abspath(p) != os.path.abspath(out_path)]
    print(f"[INFO] Tìm thấy {len(paths)} file: {', '.join(paths)}")

    all_rows: List[Dict] = []
    for p in paths:
        rows = load_json_file(p)
        print(f"[INFO] {p}: {len(rows)} dòng")
        all_rows.extend(rows)

    if not all_rows:
        write_json([], out_path)
        print(f"[DONE] Không có dữ liệu. Đã tạo rỗng: {out_path}")
        return 0

    key_field = pick_dedup_key(all_rows, dedup_key)
    print(f"[INFO] Khóa dedup: {key_field or 'tên công việc + công ty + địa điểm (composite)'}")

    seen = set()
    deduped: List[Dict] = []
    for row in all_rows:
        if key_field:
            k = row.get(key_field)
            if not k:
                k = "_fallback_" + composite_key(row)
        else:
            k = composite_key(row)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(row)

    if renumber_stt:
        for i, r in enumerate(deduped, start=1):
            if isinstance(r, dict):
                r[stt_field] = i

    write_json(deduped, out_path)
    print(f"[DONE] Gộp xong → {out_path} | Trước: {len(all_rows)} | Sau dedup: {len(deduped)}")

    if to_csv and deduped:
        import csv
        header = list(deduped[0].keys())
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerows(deduped)
        print(f"[DONE] Xuất CSV → {csv_path} (cột theo bản ghi đầu tiên)")

    return len(deduped)

def main():
    ap = argparse.ArgumentParser(description="Gộp JSON chunk thành 1 file, loại trùng, đánh lại STT, xuất CSV.")
    ap.add_argument("--pattern", required=True, help='Glob pattern, ví dụ: "data*.json" hoặc "data*_*.json"')
    ap.add_argument("--out", default="all_jobs.json", help="Đường dẫn file JSON đầu ra")
    ap.add_argument("--dedup-key", default=None, help="Khóa dedup (Liên_kết hoặc url). Bỏ trống để tự dò.")
    ap.add_argument("--renumber-stt", action="store_true", help="Đánh lại STT 1..N trong file gộp")
    ap.add_argument("--stt-field", default="stt", help="Tên cột STT trong dữ liệu (mặc định: stt)")
    ap.add_argument("--to-csv", action="store_true", help="Xuất thêm CSV")
    ap.add_argument("--csv-out", default="all_jobs.csv", help="Tên file CSV đầu ra")
    args = ap.parse_args()

    merge_files(
        pattern=args.pattern,
        out_path=args.out,
        dedup_key=args.dedup_key,
        renumber_stt=args.renumber_stt,
        stt_field=args.stt_field,
        to_csv=args.to_csv,
        csv_path=args.csv_out,
    )

if __name__ == "__main__":
    main()