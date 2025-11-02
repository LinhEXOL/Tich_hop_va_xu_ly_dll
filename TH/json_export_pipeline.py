#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON Export Pipeline
====================

Pipeline để trích xuất, schema matching, data matching, làm sạch và xuất dữ liệu ra file JSON.
"""

import sys
import os
import pandas as pd
import json
import logging
from datetime import datetime
from typing import Dict, Any

# Add src to path - ensure absolute path for linter resolution
_project_root = os.path.dirname(os.path.abspath(__file__))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

# Import ETL modules
from etl.data_loader import DataLoader  # type: ignore
from etl.data_cleaner import DataCleaner  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JSONExporter:
    """Xuất dữ liệu đã xử lý ra file JSON."""
    
    def __init__(self, output_dir: str = "output"):
        """
        Khởi tạo JSONExporter.
        
        Args:
            output_dir: Thư mục để lưu file JSON
        """
        self.output_dir = output_dir
        
        # Tạo thư mục output nếu chưa tồn tại
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
    
    def export_processed_data(self, data: pd.DataFrame, filename: str = None) -> str:
        """
        Xuất dữ liệu đã xử lý ra file JSON.
        
        Args:
            data: DataFrame chứa dữ liệu đã xử lý
            filename: Tên file (optional)
            
        Returns:
            str: Đường dẫn đến file đã lưu
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export-{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            # Convert DataFrame to dict
            data_dict = data.to_dict('records')
            
            # Write to JSON file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data_dict, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Exported {len(data_dict)} records to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise
    
    def export_matching_report(self, report: Dict[str, Any], filename: str = None) -> str:
        """
        Xuất schema matching và data matching report ra file JSON.
        
        Args:
            report: Dictionary chứa kết quả matching
            filename: Tên file (optional)
            
        Returns:
            str: Đường dẫn đến file đã lưu
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"matching_report_{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Exported matching report to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to export matching report: {e}")
            raise
    
    def export_summary(self, summary: Dict[str, Any], filename: str = "pipeline_summary.json") -> str:
        """
        Xuất tóm tắt pipeline ra file JSON.
        
        Args:
            summary: Dictionary chứa tóm tắt
            filename: Tên file
            
        Returns:
            str: Đường dẫn đến file đã lưu
        """
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Exported pipeline summary to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to export summary: {e}")
            raise


def main():
    """Hàm main để chạy pipeline."""
    print("=" * 60)
    print("JSON Export Pipeline - Job Market Analytics")
    print("=" * 60)
    print()
    
    try:
        # 1. Load dữ liệu
        print("1. Đang tải dữ liệu từ các nguồn...")
        loader = DataLoader()
        raw_data = loader.load_all_sources()
        
        total_raw_records = sum(len(df) for df in raw_data.values())
        print(f"   ✓ Đã tải {total_raw_records:,} records từ {len(raw_data)} nguồn")
        for source, df in raw_data.items():
            print(f"     - {source}: {len(df):,} records")
        print()
        
        # Initialize DataCleaner
        cleaner = DataCleaner()
        
        # 2. Clean & Standardize - LÀM TRƯỚC
        print("2. Đang làm sạch và chuẩn hóa dữ liệu...")
        cleaned_data = cleaner.clean_all_data(raw_data)
        standardized_data = cleaner.standardize_columns(cleaned_data)
        
        total_cleaned_records = sum(len(df) for df in standardized_data.values())
        print(f"   ✓ Đã xử lý {total_cleaned_records:,} records")
        print()
        
        # 3. Schema Matching - Phân tích tương thích schema trên cleaned data
        print("3. Đang phân tích tương thích schema...")
        schema_analysis = cleaner.analyze_schema_compatibility(standardized_data)
        
        # Hiển thị kết quả schema matching
        if 'compatibility' in schema_analysis:
            compat = schema_analysis['compatibility']
            overall_compat = compat.get('overall_compatibility', 0) * 100
            print(f"   ✓ Schema Compatibility: {overall_compat:.1f}%")
            
            # Hiển thị field compatibility
            if 'field_compatibility' in compat:
                compatible_fields = sum(1 for f, info in compat['field_compatibility'].items() 
                                      if info.get('is_compatible', False))
                total_fields = len(compat['field_compatibility'])
                if total_fields > 0:
                    print(f"   ✓ Compatible Fields: {compatible_fields}/{total_fields}")
        
        # Hiển thị unified schema
        if 'unified_schema' in schema_analysis:
            unified = schema_analysis['unified_schema']
            standard_fields = len(unified.get('standard_fields', []))
            print(f"   ✓ Standard Fields: {standard_fields} fields defined")
        
        print()
        
        # 4. Data Matching - Tìm duplicates và entity resolution trên cleaned data
        print("4. Đang thực hiện data matching...")
        matching_analysis = cleaner.perform_data_matching(standardized_data)
        
        # Hiển thị kết quả data matching
        if 'matching_report' in matching_analysis:
            report = matching_analysis['matching_report']
            
            # Duplicate analysis
            if 'duplicate_analysis' in report:
                dup = report['duplicate_analysis']
                dup_count = dup.get('duplicate_count', 0)
                dup_rate = dup.get('duplicate_rate', 0) * 100
                print(f"   ✓ Duplicate Records: {dup_count} ({dup_rate:.1f}%)")
                print(f"   ✓ Duplicate Groups: {dup.get('duplicate_groups', 0)} groups")
            
            # Similarity analysis
            if 'similarity_analysis' in report:
                sim = report['similarity_analysis']
                sim_groups = sim.get('similar_groups', 0)
                sim_rate = sim.get('similarity_rate', 0) * 100
                print(f"   ✓ Similar Groups: {sim_groups} groups ({sim_rate:.1f}% similarity rate)")
            
            # Entity resolution
            if 'entity_resolution' in matching_analysis:
                entity_res = matching_analysis['entity_resolution']
                if 'entity_statistics' in entity_res:
                    stats = entity_res['entity_statistics']
                    unique_companies = stats.get('unique_companies', 0)
                    unique_titles = stats.get('unique_job_titles', 0)
                    total_companies = stats.get('total_companies', 0)
                    total_titles = stats.get('total_job_titles', 0)
                    if total_companies > 0 or total_titles > 0:
                        print(f"   ✓ Entity Resolution:")
                        if total_companies > 0:
                            print(f"      - Companies: {unique_companies}/{total_companies} unique")
                        if total_titles > 0:
                            print(f"      - Job Titles: {unique_titles}/{total_titles} unique")
        
        print()
        
        # 5. Kết hợp dữ liệu từ tất cả nguồn
        print("5. Đang kết hợp dữ liệu...")
        all_data = []
        for source, df in standardized_data.items():
            if not df.empty:
                df_copy = df.copy()
                df_copy['source'] = source
                all_data.append(df_copy)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"   ✓ Đã kết hợp thành {len(combined_df):,} records")
            
            # Xóa các cột không cần thiết
            columns_to_remove = ['original_id', 'sector', 'sentiment_score']
            existing_columns_to_remove = [col for col in columns_to_remove if col in combined_df.columns]
            if existing_columns_to_remove:
                combined_df = combined_df.drop(columns=existing_columns_to_remove)
                print(f"   ✓ Đã xóa các cột: {', '.join(existing_columns_to_remove)}")
        else:
            print("   ✗ Không có dữ liệu để xử lý")
            return
        print()
        
        # 6. Xuất dữ liệu ra file JSON
        print("6. Đang xuất dữ liệu ra file JSON...")
        exporter = JSONExporter(output_dir="output")
        
        # Xuất dữ liệu đã xử lý với tên export-{timestamp}.json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_file = exporter.export_processed_data(
            combined_df,
            filename=f"export-{timestamp}.json"
        )
        print(f"   ✓ Dữ liệu đã xử lý: {export_file}")
        
        # Xuất schema matching và data matching report
        matching_file = exporter.export_matching_report(
            {
                'schema_analysis': schema_analysis,
                'data_matching': matching_analysis
            },
            filename=f"matching_report_{timestamp}.json"
        )
        print(f"   ✓ Schema & Data Matching Report: {matching_file}")
        
        # Tạo và xuất tóm tắt pipeline
        pipeline_summary = {
            'pipeline_info': {
                'run_date': datetime.now().isoformat(),
                'pipeline_version': '3.1 (Load → Clean → Schema Matching → Data Matching → Export)',
                'output_directory': 'output/'
            },
            'data_sources': {
                source: {
                    'raw_records': len(raw_data[source]),
                    'processed_records': len(standardized_data[source])
                }
                for source in raw_data.keys()
            },
            'statistics': {
                'total_raw_records': total_raw_records,
                'total_processed_records': len(combined_df),
                'unique_companies': combined_df['company_name'].nunique() if 'company_name' in combined_df.columns else 0,
                'unique_locations': combined_df['city'].nunique() if 'city' in combined_df.columns else 0,
            },
            'files_generated': {
                'export_file': export_file,
                'matching_report': matching_file
            },
            'schema_matching': {
                'overall_compatibility': schema_analysis.get('compatibility', {}).get('overall_compatibility', 0) * 100 if 'compatibility' in schema_analysis else 0,
                'standard_fields_count': len(schema_analysis.get('unified_schema', {}).get('standard_fields', [])) if 'unified_schema' in schema_analysis else 0
            },
            'data_matching': {
                'duplicate_count': matching_analysis.get('matching_report', {}).get('duplicate_analysis', {}).get('duplicate_count', 0) if 'matching_report' in matching_analysis else 0,
                'duplicate_rate': matching_analysis.get('matching_report', {}).get('duplicate_analysis', {}).get('duplicate_rate', 0) * 100 if 'matching_report' in matching_analysis else 0,
                'similar_groups': matching_analysis.get('matching_report', {}).get('similarity_analysis', {}).get('similar_groups', 0) if 'matching_report' in matching_analysis else 0
            }
        }
        
        summary_file = exporter.export_summary(pipeline_summary)
        print(f"   ✓ Tóm tắt pipeline: {summary_file}")
        print()
        
        # 7. Hiển thị thống kê
        print("=" * 60)
        print("Tóm tắt Pipeline")
        print("=" * 60)
        print(f"Tổng số records gốc:      {total_raw_records:,}")
        print(f"Tổng số records đã xử lý:  {len(combined_df):,}")
        print(f"Số lượng công ty:          {pipeline_summary['statistics']['unique_companies']:,}")
        print(f"Số lượng địa điểm:         {pipeline_summary['statistics']['unique_locations']:,}")
        print()
        
        # Hiển thị schema matching và data matching summary
        print("Schema Matching & Data Matching:")
        print("-" * 40)
        if 'schema_matching' in pipeline_summary:
            sm = pipeline_summary['schema_matching']
            print(f"  • Schema Compatibility: {sm.get('overall_compatibility', 0):.1f}%")
            print(f"  • Standard Fields: {sm.get('standard_fields_count', 0)} fields")
        if 'data_matching' in pipeline_summary:
            dm = pipeline_summary['data_matching']
            print(f"  • Duplicate Records: {dm.get('duplicate_count', 0):,} ({dm.get('duplicate_rate', 0):.1f}%)")
            print(f"  • Similar Groups: {dm.get('similar_groups', 0)} groups")
        print()
        
        print("=" * 60)
        print("✓ Pipeline hoàn tất thành công!")
        print(f"✓ Các file đã được lưu trong thư mục: {exporter.output_dir}/")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Pipeline thất bại: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())