#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON Export Pipeline (Clean Version)
------------------------------------
- Load dữ liệu từ các nguồn (Careerlink, Joboko, TopCV, ...)
- Làm sạch và chuẩn hóa
- Gộp tất cả nguồn lại
- Xuất ra JSON chuẩn:
    + NaN → null
    + 7000000.0 → 7000000
"""

import sys
import os
import pandas as pd
import json
import logging
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from etl.data_loader import DataLoader
from etl.data_cleaner import DataCleaner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JSONExporter:
    """Xuất dữ liệu đã xử lý ra file JSON."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")

    def export_processed_data(self, data: pd.DataFrame, filename: str = None) -> str:
        """Xuất dữ liệu đã xử lý ra file JSON sạch (NaN→null, số nguyên không có .0)."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processed_jobs_{timestamp}.json"

        filepath = os.path.join(self.output_dir, filename)

        try:
            import numpy as np
            import math

            # Làm sạch NaN / NA / NaT → None
            data = data.replace({pd.NA: None, pd.NaT: None, np.nan: None})
            data = data.where(pd.notna(data), None)

            # Ép các cột lương về kiểu int nếu là số nguyên
            for col in ["salary_min", "salary_max", "avg_salary"]:
                if col in data.columns:
                    def safe_int(x):
                        if isinstance(x, (float, int)) and x is not None:
                            try:
                                if float(x).is_integer():
                                    return int(x)
                            except Exception:
                                pass
                        return x
                    data[col] = data[col].apply(safe_int)

            # Convert DataFrame to dict
            data_dict = data.to_dict("records")

            # Làm sạch NaN / Inf còn sót lại
            def clean_nan(obj):
                if isinstance(obj, list):
                    return [clean_nan(v) for v in obj]
                elif isinstance(obj, dict):
                    return {k: clean_nan(v) for k, v in obj.items()}
                elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                    return None
                return obj

            data_dict = clean_nan(data_dict)

            # Xuất JSON chuẩn (NaN không hợp lệ)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(
                    data_dict,
                    f,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                    allow_nan=False
                )

            logger.info(f"Exported {len(data_dict)} records to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise


def main():
    """Pipeline rút gọn: chỉ load, clean và export JSON."""
    print("=" * 60)
    print("JSON Export Pipeline (Clean Version)")
    print("=" * 60)
    print()

    try:
        print("1. Đang tải dữ liệu từ các nguồn...")
        loader = DataLoader()
        raw_data = loader.load_all_sources()

        total_raw_records = sum(len(df) for df in raw_data.values())
        print(f"   Đã tải {total_raw_records:,} records\n")

        print("2. Đang làm sạch dữ liệu...")
        cleaner = DataCleaner()
        cleaned_data = cleaner.clean_all_data(raw_data)
        standardized_data = cleaner.standardize_columns(cleaned_data)
        print("   Đã chuẩn hóa dữ liệu\n")

        print("3. Đang gộp dữ liệu...")
        all_data = []
        for source, df in standardized_data.items():
            if not df.empty:
                df_copy = df.copy()
                df_copy["source"] = source
                all_data.append(df_copy)

        if not all_data:
            print("Không có dữ liệu để xử lý.")
            return

        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"   Tổng cộng {len(combined_df):,} records\n")

        print("4. Xuất dữ liệu ra JSON...")
        exporter = JSONExporter()
        filepath = exporter.export_processed_data(combined_df)
        print(f"   File JSON đã lưu tại: {filepath}")

        print("\nHoàn tất pipeline.")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\nPipeline thất bại: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
