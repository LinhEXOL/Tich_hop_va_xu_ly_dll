"""
Data Cleaner Module
==================

Handles data cleaning, transformation, and standardization
for job market analytics data.
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import unicodedata
from .schema_matcher import SchemaMatcher, DataMatcher

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    A class to handle data cleaning and transformation.
    """

    def __init__(self):
        """
        Initialize the DataCleaner.
        """
        self.salary_patterns = {
            'range': r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*[-–—]\s*\$?(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'single': r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'k_suffix': r'(\d+(?:,\d{3})*)\s*[Kk]',
            'hourly': r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*/\s*hr',
            'yearly': r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*/\s*yr'
        }

        # Experience extraction patterns
        self.experience_patterns = [
            r'(\d+)\+?\s*to\s*(\d+)\s*years?',
            r'(\d+)\+?\s*-\s*(\d+)\s*years?',
            r'(\d+)\+?\s*years?\s*(?:of\s*)?experience',
            r'(\d+)\+?\s*years?\s*exp',
            r'(\d+)\+?\s*yrs?\s*exp',
            r'(\d+)\+?\s*years?',
            r'(\d+)\+?\s*yr',
            r'(\d+)\+?\s*y\b'
        ]

        # Skills keywords for extraction
        self.skills_keywords = [
            # Programming Languages
            'python', 'java', 'javascript', 'typescript', 'sql', 'r', 'scala', 'go', 'c++', 'c#', 'php', 'ruby',
            'swift', 'kotlin', 'rust', 'dart', 'perl', 'matlab', 'sas', 'stata',

            # Data Science & ML
            'machine learning', 'ml', 'deep learning', 'data science', 'data analysis', 'statistics', 'analytics',
            'pandas', 'numpy', 'scipy', 'scikit-learn', 'sklearn', 'tensorflow', 'pytorch', 'keras',
            'xgboost', 'lightgbm', 'catboost', 'spark', 'hadoop', 'kafka', 'elasticsearch',

            # Visualization & BI
            'tableau', 'power bi', 'excel', 'spss', 'sas', 'matplotlib', 'seaborn', 'plotly', 'd3.js',
            'looker', 'qlik', 'microstrategy', 'cognos',

            # Cloud & DevOps
            'aws', 'amazon web services', 'azure', 'gcp', 'google cloud', 'docker', 'kubernetes', 'jenkins',
            'git', 'github', 'gitlab', 'bitbucket', 'ci/cd', 'terraform', 'ansible',

            # Databases
            'mysql', 'postgresql', 'postgres', 'mongodb', 'redis', 'cassandra', 'elasticsearch',
            'oracle', 'sql server', 'sqlite', 'dynamodb', 'bigquery', 'redshift',

            # Web Technologies
            'html', 'css', 'react', 'angular', 'vue', 'node.js', 'express', 'django', 'flask',
            'spring', 'laravel', 'rails', 'fastapi', 'streamlit', 'dash',

            # Business & Soft Skills
            'agile', 'scrum', 'jira', 'confluence', 'project management', 'leadership',
            'communication', 'teamwork', 'problem solving', 'critical thinking',

            # Domain Specific
            'finance', 'banking', 'healthcare', 'e-commerce', 'retail', 'manufacturing',
            'telecommunications', 'automotive', 'aerospace', 'energy', 'pharmaceuticals'
        ]

        # Initialize schema and data matchers
        self.schema_matcher = SchemaMatcher()
        self.data_matcher = DataMatcher()

    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text data.

        Args:
            text: Input text

        Returns:
            str: Cleaned text
        """
        if pd.isna(text) or text == '':
            return ''

        # Convert to string and strip whitespace
        text = str(text).strip()

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\-.,()&/]', '', text)

        # Normalize unicode
        text = unicodedata.normalize('NFKD', text)

        return text

    def extract_salary_range(self, salary_text: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract salary range from text.

        Args:
            salary_text: Salary text (e.g., "$50K-$70K", "50000-70000")

        Returns:
            Tuple[Optional[float], Optional[float]]: (min_salary, max_salary)
        """
        if pd.isna(salary_text) or salary_text == '':
            return None, None

        salary_text = str(salary_text).strip()

        # Handle K suffix (e.g., "50K-70K")
        if 'K' in salary_text.upper():
            salary_text = re.sub(
                r'(\d+(?:,\d{3})*)\s*[Kk]', r'\g<1>000', salary_text)

        # Extract range pattern
        range_match = re.search(self.salary_patterns['range'], salary_text)
        if range_match:
            min_sal = float(range_match.group(1).replace(',', ''))
            max_sal = float(range_match.group(2).replace(',', ''))
            return min_sal, max_sal

        # Extract single value
        single_match = re.search(self.salary_patterns['single'], salary_text)
        if single_match:
            salary = float(single_match.group(1).replace(',', ''))
            return salary, salary

        return None, None

    def extract_experience(self, text: str) -> Optional[int]:
        """
        Extract years of experience from text.

        Args:
            text: Input text (job description, experience field, etc.)

        Returns:
            Optional[int]: Years of experience or None if not found
        """
        if pd.isna(text) or text == '':
            return None

        text = str(text).lower()

        # Try each pattern
        for pattern in self.experience_patterns:
            match = re.search(pattern, text)
            if match:
                # Handle range patterns (e.g., "3-5 years")
                if len(match.groups()) == 2:
                    try:
                        min_exp = int(match.group(1))
                        max_exp = int(match.group(2))
                        return (min_exp + max_exp) // 2  # Return average
                    except ValueError:
                        continue
                else:
                    try:
                        return int(match.group(1))
                    except ValueError:
                        continue

        return None

    def extract_skills(self, text: str) -> List[str]:
        """
        Extract skills from text.

        Args:
            text: Input text (job description, skills field, etc.)

        Returns:
            List[str]: List of found skills
        """
        if pd.isna(text) or text == '':
            return []

        text_lower = str(text).lower()
        found_skills = []

        for skill in self.skills_keywords:
            if skill in text_lower:
                found_skills.append(skill)

        return list(set(found_skills))  # Remove duplicates

    def validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate data quality and return validation results.

        Args:
            df: DataFrame to validate

        Returns:
            Dict[str, Any]: Validation results
        """
        validation_results = {
            'total_rows': len(df),
            'missing_values': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict(),
            'validation_errors': [],
            'warnings': []
        }

        # Validate salary range logic
        if 'salary_min' in df.columns and 'salary_max' in df.columns:
            # Convert to numeric, coercing errors to NaN
            salary_min_numeric = pd.to_numeric(
                df['salary_min'], errors='coerce')
            salary_max_numeric = pd.to_numeric(
                df['salary_max'], errors='coerce')

            # Check for invalid salary ranges (min > max)
            invalid_salary = df[(salary_min_numeric.notna()) & (salary_max_numeric.notna()) &
                                (salary_min_numeric > salary_max_numeric)]
            if len(invalid_salary) > 0:
                validation_results['validation_errors'].append(
                    f"Found {len(invalid_salary)} records with salary_min > salary_max"
                )

        # Validate experience range
        if 'experience' in df.columns:
            numeric_exp = pd.to_numeric(df['experience'], errors='coerce')
            invalid_exp = numeric_exp[(numeric_exp < 0) | (numeric_exp > 50)]
            if len(invalid_exp) > 0:
                validation_results['warnings'].append(
                    f"Found {len(invalid_exp)} records with unrealistic experience values"
                )

        # Validate required fields completeness
        required_fields = ['job_title_clean', 'company_name', 'source']
        for field in required_fields:
            if field in df.columns:
                missing_count = df[field].isnull().sum()
                if missing_count > 0:
                    validation_results['warnings'].append(
                        f"Field '{field}' has {missing_count} missing values"
                    )

        # Validate data types
        if 'salary_min' in df.columns:
            non_numeric_salary = pd.to_numeric(
                df['salary_min'], errors='coerce').isnull().sum()
            if non_numeric_salary > 0:
                validation_results['validation_errors'].append(
                    f"Found {non_numeric_salary} non-numeric values in salary_min"
                )

        return validation_results

    def get_data_quality_score(self, df: pd.DataFrame) -> float:
        """
        Calculate data quality score (0-100).

        Args:
            df: DataFrame to score

        Returns:
            float: Quality score from 0 to 100
        """
        if df.empty:
            return 0.0

        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()

        # Base score from completeness
        completeness_score = (
            (total_cells - missing_cells) / total_cells) * 100

        # Deduct points for validation errors
        validation_results = self.validate_data(df)
        error_penalty = len(validation_results['validation_errors']) * 5
        warning_penalty = len(validation_results['warnings']) * 2

        final_score = max(0, completeness_score -
                          error_penalty - warning_penalty)
        return round(final_score, 2)

    def clean_careerlink_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean CareerLink data.

        Args:
            df: Raw CareerLink DataFrame

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            return df

        logger.info("Cleaning CareerLink data...")
        cleaned_df = df.copy()

        # Add source identifier
        cleaned_df['source'] = 'careerlink'

        # Clean job titles
        if 'tên công việc' in cleaned_df.columns:
            cleaned_df['job_title_clean'] = cleaned_df['tên công việc'].apply(
                self.clean_text)

        # Clean company names
        if 'tên công ty' in cleaned_df.columns:
            cleaned_df['company_name'] = cleaned_df['tên công ty'].apply(
                self.clean_text)

        # Clean locations
        if 'Địa điểm công việc' in cleaned_df.columns:
            cleaned_df['location_clean'] = cleaned_df['Địa điểm công việc'].apply(
                self.clean_text)
            # Extract city and state for Vietnamese locations
            cleaned_df['city'] = cleaned_df['Địa điểm công việc'].apply(
                self._extract_vietnamese_city)
            cleaned_df['state'] = cleaned_df['Địa điểm công việc'].apply(
                self._extract_vietnamese_province)
            cleaned_df['country'] = 'Vietnam'

        # Clean salary
        if 'Mức lương' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['salary_text'] = cleaned_df['Mức lương']
            salary_data = cleaned_df['Mức lương'].apply(
                self.extract_vietnamese_salary_range)
            cleaned_df['salary_min'] = [x[0] for x in salary_data]
            cleaned_df['salary_max'] = [x[1] for x in salary_data]

        # Clean industry
        if 'ngành nghề' in cleaned_df.columns:
            cleaned_df['industry'] = cleaned_df['ngành nghề'].apply(
                self.clean_text)

        # Clean job descriptions
        if 'mô tả công việc' in cleaned_df.columns:
            cleaned_df['job_description'] = cleaned_df['mô tả công việc'].apply(
                self.clean_text)

        # Clean skills
        if 'kĩ năng yêu cầu' in cleaned_df.columns:
            cleaned_df['skills'] = cleaned_df['kĩ năng yêu cầu'].apply(
                self.clean_text)
        else:
            cleaned_df['skills'] = ''

        # Clean experience
        if 'Kinh nghiệm' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['experience_text'] = cleaned_df['Kinh nghiệm']
            cleaned_df['experience'] = cleaned_df['Kinh nghiệm'].apply(
                self.extract_vietnamese_experience)
        else:
            cleaned_df['experience'] = None
            cleaned_df['experience_text'] = None

        # Clean job type
        if 'loại công việc' in cleaned_df.columns:
            cleaned_df['job_type'] = cleaned_df['loại công việc'].apply(
                self.clean_text)

        # Clean job level
        if 'cấp bậc' in cleaned_df.columns:
            cleaned_df['job_level'] = cleaned_df['cấp bậc'].apply(
                self.clean_text)

        # Clean education
        if 'học vấn' in cleaned_df.columns:
            cleaned_df['education'] = cleaned_df['học vấn'].apply(
                self.clean_text)

        # Add missing columns
        cleaned_df['rating'] = None
        cleaned_df['company_size'] = None
        cleaned_df['benefits'] = ''

        logger.info(f"Cleaned CareerLink data: {len(cleaned_df)} rows")
        return cleaned_df

    def clean_joboko_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Joboko data.

        Args:
            df: Raw Joboko DataFrame

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            return df

        logger.info("Cleaning Joboko data...")
        cleaned_df = df.copy()

        # Normalize Joboko column names to shared Vietnamese keys used by cleaner
        alias_map = {
            'ten_cong_viec': 'tên công việc',
            'ten_cong_ty': 'tên công ty',
            'dia_diem_lam_viec': 'địa điểm',
            'muc_luong': 'mức lương',
            'kinh_nghiem': 'kinh nghiệm',
            'mo_ta_chi_tiet': 'mô tả công việc',
            'ki_nang_yeu_cau': 'kĩ năng yêu cầu',
            'loai_cong_viec': 'loại công việc',
            'cap_bac': 'cấp bậc',
            'nganh_nghe': 'ngành nghề'
        }
        for old_col, new_col in alias_map.items():
            if old_col in cleaned_df.columns and new_col not in cleaned_df.columns:
                cleaned_df[new_col] = cleaned_df[old_col]

        # Add source identifier
        cleaned_df['source'] = 'joboko'

        # Clean job titles
        if 'tên công việc' in cleaned_df.columns:
            cleaned_df['job_title_clean'] = cleaned_df['tên công việc'].apply(
                self.clean_text)

        # Clean company names
        if 'tên công ty' in cleaned_df.columns:
            cleaned_df['company_name'] = cleaned_df['tên công ty'].apply(
                self.clean_text)

        # Clean locations
        if 'địa điểm' in cleaned_df.columns:
            cleaned_df['location_clean'] = cleaned_df['địa điểm'].apply(
                self.clean_text)
            cleaned_df['city'] = cleaned_df['địa điểm'].apply(
                self._extract_vietnamese_city)
            cleaned_df['state'] = cleaned_df['địa điểm'].apply(
                self._extract_vietnamese_province)
            cleaned_df['country'] = 'Vietnam'

        # Clean salary
        if 'mức lương' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['salary_text'] = cleaned_df['mức lương']
            salary_data = cleaned_df['mức lương'].apply(
                self.extract_vietnamese_salary_range)
            cleaned_df['salary_min'] = [x[0] for x in salary_data]
            cleaned_df['salary_max'] = [x[1] for x in salary_data]
        else:
            cleaned_df['salary_min'] = None
            cleaned_df['salary_max'] = None
            cleaned_df['salary_text'] = None

        # Clean industry
        if 'ngành nghề' in cleaned_df.columns:
            cleaned_df['industry'] = cleaned_df['ngành nghề'].apply(
                self.clean_text)

        # Clean job descriptions
        if 'mô tả công việc' in cleaned_df.columns:
            cleaned_df['job_description'] = cleaned_df['mô tả công việc'].apply(
                self.clean_text)

        # Clean skills
        if 'kĩ năng yêu cầu' in cleaned_df.columns:
            cleaned_df['skills'] = cleaned_df['kĩ năng yêu cầu'].apply(
                self.clean_text)
        else:
            cleaned_df['skills'] = ''

        # Clean experience
        if 'kinh nghiệm' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['experience_text'] = cleaned_df['kinh nghiệm']
            cleaned_df['experience'] = cleaned_df['kinh nghiệm'].apply(
                self.extract_vietnamese_experience)
        else:
            cleaned_df['experience'] = None
            cleaned_df['experience_text'] = None

        # Clean job type
        if 'loại công việc' in cleaned_df.columns:
            cleaned_df['job_type'] = cleaned_df['loại công việc'].apply(
                self.clean_text)

        # Clean job level
        if 'cấp bậc' in cleaned_df.columns:
            cleaned_df['job_level'] = cleaned_df['cấp bậc'].apply(
                self.clean_text)

        # Clean company size
        if 'quy mô công ty' in cleaned_df.columns:
            cleaned_df['company_size'] = cleaned_df['quy mô công ty'].apply(
                self.clean_text)

        # Add missing columns
        cleaned_df['rating'] = None
        cleaned_df['education'] = None
        cleaned_df['benefits'] = ''

        logger.info(f"Cleaned Joboko data: {len(cleaned_df)} rows")
        return cleaned_df

    def clean_topcv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean TopCV data.

        Args:
            df: Raw TopCV DataFrame

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            return df

        logger.info("Cleaning TopCV data...")
        cleaned_df = df.copy()

        # Normalize TopCV column names to shared Vietnamese keys used by cleaner
        alias_map = {
            'công ty': 'tên công ty',
            'địa điểm làm việc': 'địa điểm',
            'lương': 'mức lương',
            'mô tả chi tiết': 'mô tả công việc'
        }
        for old_col, new_col in alias_map.items():
            if old_col in cleaned_df.columns and new_col not in cleaned_df.columns:
                cleaned_df[new_col] = cleaned_df[old_col]

        # Add source identifier
        cleaned_df['source'] = 'topcv'

        # Clean job titles
        if 'tên công việc' in cleaned_df.columns:
            cleaned_df['job_title_clean'] = cleaned_df['tên công việc'].apply(
                self.clean_text)

        # Clean company names
        if 'tên công ty' in cleaned_df.columns:
            cleaned_df['company_name'] = cleaned_df['tên công ty'].apply(
                self.clean_text)

        # Clean locations
        if 'địa điểm' in cleaned_df.columns:
            cleaned_df['location_clean'] = cleaned_df['địa điểm'].apply(
                self.clean_text)
            cleaned_df['city'] = cleaned_df['địa điểm'].apply(
                self._extract_vietnamese_city)
            cleaned_df['state'] = cleaned_df['địa điểm'].apply(
                self._extract_vietnamese_province)
            cleaned_df['country'] = 'Vietnam'

        # Clean salary
        if 'mức lương' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['salary_text'] = cleaned_df['mức lương']
            salary_data = cleaned_df['mức lương'].apply(
                self.extract_vietnamese_salary_range)
            cleaned_df['salary_min'] = [x[0] for x in salary_data]
            cleaned_df['salary_max'] = [x[1] for x in salary_data]
        else:
            cleaned_df['salary_min'] = None
            cleaned_df['salary_max'] = None
            cleaned_df['salary_text'] = None

        # Clean job descriptions
        if 'mô tả công việc' in cleaned_df.columns:
            cleaned_df['job_description'] = cleaned_df['mô tả công việc'].apply(
                self.clean_text)

        # Clean skills
        if 'kĩ năng yêu cầu' in cleaned_df.columns:
            cleaned_df['skills'] = cleaned_df['kĩ năng yêu cầu'].apply(
                self.clean_text)
        else:
            cleaned_df['skills'] = ''

        # Clean experience
        if 'kinh nghiệm' in cleaned_df.columns:
            # Preserve original text
            cleaned_df['experience_text'] = cleaned_df['kinh nghiệm']
            cleaned_df['experience'] = cleaned_df['kinh nghiệm'].apply(
                self.extract_vietnamese_experience)
        else:
            cleaned_df['experience'] = None
            cleaned_df['experience_text'] = None

        # Clean benefits
        if 'quyền lợi' in cleaned_df.columns:
            cleaned_df['benefits'] = cleaned_df['quyền lợi'].apply(
                self.clean_text)
        else:
            cleaned_df['benefits'] = ''

        # Clean work time
        if 'thời gian làm việc' in cleaned_df.columns:
            cleaned_df['work_time'] = cleaned_df['thời gian làm việc'].apply(
                self.clean_text)
        else:
            cleaned_df['work_time'] = None

        # Add missing columns
        cleaned_df['rating'] = None
        cleaned_df['company_size'] = None
        cleaned_df['industry'] = None
        cleaned_df['job_type'] = None
        cleaned_df['job_level'] = None
        cleaned_df['education'] = None

        logger.info(f"Cleaned TopCV data: {len(cleaned_df)} rows")
        return cleaned_df

    def _extract_city(self, location: str) -> str:
        """Extract city from location string."""
        if pd.isna(location):
            return ''

        location = str(location).strip()
        # Split by comma and take first part
        parts = location.split(',')
        if parts:
            return parts[0].strip()
        return location

    def _extract_state(self, location: str) -> str:
        """Extract state from location string."""
        if pd.isna(location):
            return ''

        location = str(location).strip()
        # Split by comma and take second part
        parts = location.split(',')
        if len(parts) > 1:
            return parts[1].strip()
        return ''

    def _extract_vietnamese_city(self, location: str) -> str:
        """Extract city from Vietnamese location string."""
        if pd.isna(location):
            return ''

        location = str(location).strip()
        # Common Vietnamese city patterns
        vietnamese_cities = [
            'Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ',
            'An Giang', 'Bà Rịa - Vũng Tàu', 'Bắc Giang', 'Bắc Kạn', 'Bạc Liêu',
            'Bắc Ninh', 'Bến Tre', 'Bình Định', 'Bình Dương', 'Bình Phước',
            'Bình Thuận', 'Cà Mau', 'Cao Bằng', 'Đắk Lắk', 'Đắk Nông',
            'Điện Biên', 'Đồng Nai', 'Đồng Tháp', 'Gia Lai', 'Hà Giang',
            'Hà Nam', 'Hà Tĩnh', 'Hải Dương', 'Hậu Giang', 'Hòa Bình',
            'Hưng Yên', 'Khánh Hòa', 'Kiên Giang', 'Kon Tum', 'Lai Châu',
            'Lâm Đồng', 'Lạng Sơn', 'Lào Cai', 'Long An', 'Nam Định',
            'Nghệ An', 'Ninh Bình', 'Ninh Thuận', 'Phú Thọ', 'Phú Yên',
            'Quảng Bình', 'Quảng Nam', 'Quảng Ngãi', 'Quảng Ninh', 'Quảng Trị',
            'Sóc Trăng', 'Sơn La', 'Tây Ninh', 'Thái Bình', 'Thái Nguyên',
            'Thanh Hóa', 'Thừa Thiên Huế', 'Tiền Giang', 'Trà Vinh', 'Tuyên Quang',
            'Vĩnh Long', 'Vĩnh Phúc', 'Yên Bái'
        ]

        # Check for exact city matches
        for city in vietnamese_cities:
            if city in location:
                return city

        # If no exact match, split by comma and take first part
        parts = location.split(',')
        if parts:
            return parts[0].strip()
        return location

    def _extract_vietnamese_province(self, location: str) -> str:
        """Extract province from Vietnamese location string."""
        if pd.isna(location):
            return ''

        location = str(location).strip()
        # Split by comma and take last part (usually province)
        parts = location.split(',')
        if len(parts) > 1:
            return parts[-1].strip()
        return ''

    # def extract_vietnamese_salary_range(self, salary_text: str) -> Tuple[Optional[float], Optional[float]]:
    #     """
    #     Extract salary range from Vietnamese salary text.

    #     Args:
    #         salary_text: Salary text (e.g., "8 - 15 triệu", "Thương lượng")

    #     Returns:
    #         Tuple[Optional[float], Optional[float]]: (min_salary, max_salary) in VND
    #     """
    #     if pd.isna(salary_text) or salary_text == '':
    #         return None, None

    #     salary_text = str(salary_text).strip()

    #     # Handle "Thương lượng" or "Thỏa thuận"
    #     if any(term in salary_text.lower() for term in ['thương lượng', 'thỏa thuận', 'negotiable']):
    #         return None, None

    #     # Handle "USD" currency
    #     if 'usd' in salary_text.lower():
    #         # Extract USD amounts
    #         usd_pattern = r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*-\s*(\d+(?:,\d{3})*(?:\.\d{2})?)\s*usd'
    #         usd_match = re.search(usd_pattern, salary_text.lower())
    #         if usd_match:
    #             min_usd = float(usd_match.group(1).replace(',', ''))
    #             max_usd = float(usd_match.group(2).replace(',', ''))
    #             # Convert USD to VND (approximate rate: 1 USD = 24,000 VND)
    #             return min_usd * 24000, max_usd * 24000

    #         # Single USD amount
    #         single_usd_pattern = r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*usd'
    #         single_usd_match = re.search(single_usd_pattern, salary_text.lower())
    #         if single_usd_match:
    #             usd_amount = float(single_usd_match.group(1).replace(',', ''))
    #             vnd_amount = usd_amount * 24000
    #             return vnd_amount, vnd_amount

    #     # Handle VND amounts (triệu, nghìn)
    #     # Convert "triệu" to actual numbers
    #     salary_text = re.sub(r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*triệu', r'\g<1>000000', salary_text)
    #     salary_text = re.sub(r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*nghìn', r'\g<1>000', salary_text)

    #     # Extract range pattern
    #     range_pattern = r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*[-–—]\s*(\d+(?:,\d{3})*(?:\.\d{2})?)'
    #     range_match = re.search(range_pattern, salary_text)
    #     if range_match:
    #         min_sal = float(range_match.group(1).replace(',', ''))
    #         max_sal = float(range_match.group(2).replace(',', ''))
    #         return min_sal, max_sal

    #     # Extract single value
    #     single_pattern = r'(\d+(?:,\d{3})*(?:\.\d{2})?)'
    #     single_match = re.search(single_pattern, salary_text)
    #     if single_match:
    #         salary = float(single_match.group(1).replace(',', ''))
    #         return salary, salary

    #     return None, None

    def extract_vietnamese_salary_range(self, salary_text: str,
                                        usd_to_vnd: float = 24000.0
                                        ) -> Tuple[Optional[float], Optional[float]]:
        if salary_text is None or (isinstance(salary_text, float) and pd.isna(salary_text)):
            return None, None

        s = str(salary_text).strip()
        if s == "":
            return None, None

        # Negotiable
        if any(term in s.lower() for term in ["thương lượng", "thoả thuận", "thỏa thuận", "negotiable"]):
            return None, None

        # Chuẩn hoá dash & "to"
        s_norm = re.sub(r"[–—−]", "-", s)
        s_norm = re.sub(r"\s*to\s*", "-", s_norm, flags=re.IGNORECASE)

        # ===== USD =====
        if ("usd" in s_norm.lower()) or ("$" in s_norm):
            usd_range_pat = re.compile(
                r"(?i)(\d[\d\.,]*)\s*(?:usd|\$)?\s*-\s*(\d[\d\.,]*)\s*(?:usd|\$)?")
            m = usd_range_pat.search(s_norm)
            if m:
                # bỏ , .
                a = float(re.sub(r"[^\d.]", "", m.group(1)).replace(".", ""))
                b = float(re.sub(r"[^\d.]", "", m.group(2)).replace(".", ""))
                return a * usd_to_vnd, b * usd_to_vnd

            usd_single_pat = re.compile(
                r"(?i)(?:usd|\$)\s*(\d[\d\.,]*)|(\d[\d\.,]*)\s*(?:usd|\$)")
            m = usd_single_pat.search(s_norm)
            if m:
                v = m.group(1) or m.group(2)
                val = float(re.sub(r"[^\d.]", "", v).replace(".", ""))
                val_vnd = val * usd_to_vnd
                return val_vnd, val_vnd

        # Helpers cho VND
        def _unit_multiplier(u: Optional[str]) -> float:
            if not u:
                return 1.0
            u = u.strip().lower()
            if u in ["triệu", "tr", "trđ", "trd", "million"]:
                return 1_000_000.0
            if u in ["nghìn", "ngàn", "ngan", "k"]:
                return 1_000.0
            return 1.0

        def _parse_with_unit(num_str: str, unit: Optional[str]) -> float:
            ns = num_str.replace(",", ".")
            try:
                base = float(ns)
            except ValueError:
                base = float(re.sub(r"[^\d\.]", "", ns).replace(".", ""))
            return base * _unit_multiplier(unit)

        def _parse_plain_number(num_str: str) -> float:
            # Không scale; chỉ bỏ mọi ký tự không phải số
            digits = re.sub(r"[^\d]", "", num_str)
            return float(digits) if digits else 0.0

        # ===== RANGE có/không đơn vị =====
        vnd_unit_range = re.compile(
            r"(?i)\b(\d[\d\.,]*)\s*(triệu|tr|trđ|trd|million|nghìn|ngàn|ngan|k)?\s*-\s*"
            r"(\d[\d\.,]*)\s*(triệu|tr|trđ|trd|million|nghìn|ngàn|ngan|k)?\b"
        )
        m = vnd_unit_range.search(s_norm)
        if m:
            v1, u1, v2, u2 = m.groups()
            u1_eff = u1 or u2
            u2_eff = u2 or u1
            if u1_eff or u2_eff:
                # Có ít nhất một bên có đơn vị -> dùng đơn vị đó cho bên còn lại nếu thiếu
                a = _parse_with_unit(v1, u1_eff)
                b = _parse_with_unit(v2, u2_eff)
                return a, b
            else:
                # CẢ HAI BÊN KHÔNG CÓ ĐƠN VỊ -> Áp dụng quy tắc USD/VND theo ngưỡng 1e6
                a_raw = _parse_plain_number(v1)
                b_raw = _parse_plain_number(v2)
                if a_raw < 1_000_000 and b_raw < 1_000_000:
                    return a_raw * usd_to_vnd, b_raw * usd_to_vnd  # hiểu là USD
                else:
                    return a_raw, b_raw  # hiểu là VND

        # ===== RANGE VND thuần (fallback) =====
        pure_range = re.compile(r"\b(\d[\d\.,]*)\s*-\s*(\d[\d\.,]*)\b")
        m = pure_range.search(s_norm)
        if m:
            a_raw = _parse_plain_number(m.group(1))
            b_raw = _parse_plain_number(m.group(2))
            if a_raw < 1_000_000 and b_raw < 1_000_000:
                return a_raw * usd_to_vnd, b_raw * usd_to_vnd  # không có đơn vị -> USD
            else:
                return a_raw, b_raw  # VND

        # ===== SINGLE có đơn vị =====
        vnd_unit_single = re.compile(
            r"(?i)\b(\d[\d\.,]*)\s*(triệu|tr|trđ|trd|million|nghìn|ngàn|ngan|k)\b")
        m = vnd_unit_single.search(s_norm)
        if m:
            v, u = m.groups()
            val = _parse_with_unit(v, u)
            return val, val

        # ===== SINGLE thuần (không đơn vị) =====
        vnd_single = re.compile(r"\b(\d[\d\.,]{1,})\b")
        m = vnd_single.search(s_norm)
        if m:
            raw = _parse_plain_number(m.group(1))
            if raw < 1_000_000:
                val = raw * usd_to_vnd  # USD
            else:
                val = raw              # VND
            return val, val

        return None, None

    def extract_vietnamese_experience(self, text: str) -> Optional[int]:
        """
        Extract years of experience from Vietnamese text.

        Args:
            text: Input text (job description, experience field, etc.)

        Returns:
            Optional[int]: Years of experience or None if not found
        """
        if pd.isna(text) or text == '':
            return None

        text = str(text).lower()

        # Vietnamese experience patterns
        vietnamese_patterns = [
            r'(\d+)\+?\s*-\s*(\d+)\s*năm',
            r'(\d+)\+?\s*đến\s*(\d+)\s*năm',
            r'(\d+)\+?\s*năm\s*kinh\s*nghiệm',
            r'(\d+)\+?\s*năm\s*exp',
            r'(\d+)\+?\s*năm',
            r'(\d+)\+?\s*n\b',
            r'không\s*yêu\s*cầu',
            r'không\s*cần\s*kinh\s*nghiệm'
        ]

        # Check for "no experience required"
        if any(term in text for term in ['không yêu cầu', 'không cần kinh nghiệm', 'fresh graduate', 'sinh viên mới ra trường']):
            return 0

        # Try each pattern
        for pattern in vietnamese_patterns:
            match = re.search(pattern, text)
            if match:
                # Handle range patterns (e.g., "3-5 năm")
                if len(match.groups()) == 2:
                    try:
                        min_exp = int(match.group(1))
                        max_exp = int(match.group(2))
                        return (min_exp + max_exp) // 2  # Return average
                    except ValueError:
                        continue
                else:
                    try:
                        return int(match.group(1))
                    except ValueError:
                        continue

        return None

    def clean_all_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Clean all data sources.

        Args:
            data: Dictionary of raw DataFrames

        Returns:
            Dict[str, pd.DataFrame]: Dictionary of cleaned DataFrames
        """
        cleaned_data = {}

        for source, df in data.items():
            if source == 'careerlink':
                cleaned_data[source] = self.clean_careerlink_data(df)
            elif source == 'joboko':
                cleaned_data[source] = self.clean_joboko_data(df)
            elif source == 'topcv':
                cleaned_data[source] = self.clean_topcv_data(df)
            else:
                logger.warning(f"Unknown data source: {source}")
                cleaned_data[source] = df

        return cleaned_data

    def standardize_columns(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Standardize column names across all data sources.

        Args:
            data: Dictionary of DataFrames

        Returns:
            Dict[str, pd.DataFrame]: DataFrames with standardized columns
        """
        standardized_data = {}

        # # Define standard columns
        # standard_columns = [
        #     'source', 'job_title_clean', 'company_name', 'location_clean',
        #     'city', 'state', 'country', 'salary_min', 'salary_max',
        #     'industry', 'job_description', 'rating', 'company_size',
        #     'skills', 'experience', 'job_type', 'job_level', 'education',
        #     'benefits', 'work_time',
        #     # Preserve raw text fields for downstream export/DB
        #     'salary_text', 'experience_text'
        # ]
        # Define standard columns
        standard_columns = [
            'source', 'job_title_clean', 'company_name', 'location_clean',
            # 'salary_min', 'salary_max', 'experience', 
            'industry', 'job_description',
            'skills', 'job_type', 'job_level', 'education',
            # Preserve raw text fields for downstream export/DB
            'salary_text', 'experience_text'
        ]

        for source, df in data.items():
            if df.empty:
                standardized_data[source] = df
                continue

            # Create standardized DataFrame
            std_df = pd.DataFrame()

            for col in standard_columns:
                if col in df.columns:
                    std_df[col] = df[col]
                else:
                    std_df[col] = None

            standardized_data[source] = std_df
            logger.info(f"Standardized {source} data: {len(std_df)} rows")

        return standardized_data

    def get_cleaning_summary(self, original_data: Dict[str, pd.DataFrame],
                             cleaned_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Get comprehensive summary of cleaning process.

        Args:
            original_data: Original data before cleaning
            cleaned_data: Data after cleaning

        Returns:
            Dict[str, Dict]: Comprehensive cleaning summary
        """
        summary = {}

        for source in original_data.keys():
            orig_df = original_data[source]
            clean_df = cleaned_data[source]

            # Basic cleaning stats
            basic_stats = {
                'original_rows': len(orig_df),
                'cleaned_rows': len(clean_df),
                'rows_removed': len(orig_df) - len(clean_df),
                'columns_standardized': len(clean_df.columns),
                'missing_values': clean_df.isnull().sum().to_dict()
            }

            # Data quality assessment
            quality_score = self.get_data_quality_score(clean_df)
            validation_results = self.validate_data(clean_df)

            # Skills and experience extraction stats
            skills_stats = {}
            if 'skills' in clean_df.columns:
                non_empty_skills = clean_df['skills'].apply(
                    lambda x: len(str(x).strip()) > 0).sum()
                skills_stats = {
                    'records_with_skills': int(non_empty_skills),
                    'skills_extraction_rate': round(non_empty_skills / len(clean_df) * 100, 2) if len(clean_df) > 0 else 0
                }

            experience_stats = {}
            if 'experience' in clean_df.columns:
                non_null_experience = clean_df['experience'].notna().sum()
                experience_stats = {
                    'records_with_experience': int(non_null_experience),
                    'experience_extraction_rate': round(non_null_experience / len(clean_df) * 100, 2) if len(clean_df) > 0 else 0,
                    'avg_experience': round(clean_df['experience'].mean(), 2) if non_null_experience > 0 else None
                }

            summary[source] = {
                **basic_stats,
                'data_quality_score': quality_score,
                'validation_errors': len(validation_results['validation_errors']),
                'validation_warnings': len(validation_results['warnings']),
                'skills_extraction': skills_stats,
                'experience_extraction': experience_stats,
                'validation_details': validation_results
            }

        return summary

    def analyze_schema_compatibility(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyze schema compatibility across data sources.

        Args:
            data: Dictionary of DataFrames from different sources

        Returns:
            Dict[str, Any]: Schema compatibility analysis
        """
        logger.info("Analyzing schema compatibility...")

        # Detect schemas for each source
        schemas = {}
        for source, df in data.items():
            if not df.empty:
                schemas[source] = self.schema_matcher.detect_schema(df, source)

        # Create unified schema
        unified_schema = self.schema_matcher.create_unified_schema(schemas)

        # Validate compatibility
        compatibility = self.schema_matcher.validate_schema_compatibility(
            schemas)

        return {
            'schemas': schemas,
            'unified_schema': unified_schema,
            'compatibility': compatibility
        }

    def _map_raw_columns_for_matching(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Map raw data columns to standard columns for matching.
        
        Args:
            data: Dictionary of raw DataFrames
            
        Returns:
            Dict[str, pd.DataFrame]: DataFrames with mapped columns
        """
        mapped_data = {}
        
        for source, df in data.items():
            if df.empty:
                mapped_data[source] = df
                continue
                
            df_mapped = df.copy()
            
            # Map columns based on source
            column_mappings = {
                'careerlink': {
                    'job_title_clean': 'tên công việc',
                    'company_name': 'tên công ty',
                    'location_clean': 'Địa điểm công việc'
                },
                'joboko': {
                    'job_title_clean': ['ten_cong_viec', 'tên công việc'],
                    'company_name': ['ten_cong_ty', 'tên công ty'],
                    'location_clean': ['dia_diem_lam_viec', 'địa điểm', 'Địa điểm công việc']
                },
                'topcv': {
                    'job_title_clean': 'tên công việc',
                    'company_name': ['công ty', 'tên công ty'],
                    'location_clean': ['địa điểm làm việc', 'địa điểm', 'Địa điểm công việc']
                }
            }
            
            # Get mapping for this source
            source_mapping = column_mappings.get(source, {})
            
            # Map columns
            for target_col, source_cols in source_mapping.items():
                if isinstance(source_cols, list):
                    # Try each possible source column
                    for src_col in source_cols:
                        if src_col in df_mapped.columns:
                            df_mapped[target_col] = df_mapped[src_col]
                            break
                else:
                    if source_cols in df_mapped.columns:
                        df_mapped[target_col] = df_mapped[source_cols]
            
            mapped_data[source] = df_mapped
        
        return mapped_data

    def perform_data_matching(self, data: Dict[str, pd.DataFrame], is_raw_data: bool = False) -> Dict[str, Any]:
        """
        Perform data matching and entity resolution.

        Args:
            data: Dictionary of DataFrames (cleaned or raw)
            is_raw_data: If True, map raw columns to standard columns first

        Returns:
            Dict[str, Any]: Data matching results
        """
        logger.info("Performing data matching...")

        # Map raw columns if needed
        if is_raw_data:
            data = self._map_raw_columns_for_matching(data)

        # Combine all data for matching analysis
        all_data = []
        for source, df in data.items():
            if not df.empty:
                df_copy = df.copy()
                df_copy['source'] = source
                all_data.append(df_copy)

        if not all_data:
            return {'error': 'No data available for matching'}

        # Filter out empty DataFrames before concatenation
        non_empty_data = [
            df for df in all_data if not df.empty and len(df) > 0]
        if non_empty_data:
            # Ensure all DataFrames have the same columns before concatenation
            all_columns = set()
            for df in non_empty_data:
                all_columns.update(df.columns)

            # Add missing columns to each DataFrame
            for df in non_empty_data:
                for col in all_columns:
                    if col not in df.columns:
                        df[col] = None

            combined_df = pd.concat(non_empty_data, ignore_index=True)
        else:
            combined_df = pd.DataFrame()

        # Check if required columns exist, if not use available columns
        required_cols = ['job_title_clean', 'company_name', 'location_clean']
        available_cols = [col for col in required_cols if col in combined_df.columns]
        
        if not available_cols:
            # Fallback: use any columns that might work
            fallback_cols = []
            for col in combined_df.columns:
                if any(keyword in col.lower() for keyword in ['title', 'job', 'công việc', 'ten']):
                    fallback_cols.append(col)
                    break
            for col in combined_df.columns:
                if any(keyword in col.lower() for keyword in ['company', 'cong ty', 'công ty']):
                    fallback_cols.append(col)
                    break
            for col in combined_df.columns:
                if any(keyword in col.lower() for keyword in ['location', 'dia diem', 'địa điểm']):
                    fallback_cols.append(col)
                    break
            
            if fallback_cols:
                available_cols = fallback_cols

        # Create a temporary DataMatcher with adjusted fields
        if available_cols != required_cols:
            # Temporarily adjust DataMatcher fields
            original_fields = self.data_matcher.fuzzy_match_fields.copy()
            self.data_matcher.fuzzy_match_fields = available_cols if available_cols else ['source']
            
            try:
                # Perform matching analysis with adjusted fields
                matching_report = self.data_matcher.generate_matching_report(
                    combined_df)
                
                # Find duplicates with available columns
                df_with_duplicates = self.data_matcher.find_duplicates(
                    combined_df, key_fields=available_cols + ['source'])
                similar_groups = self.data_matcher.find_similar_records(combined_df)
                
                # Entity resolution (will use available columns)
                entity_resolution = self.data_matcher.resolve_entities(combined_df)
            finally:
                # Restore original fields
                self.data_matcher.fuzzy_match_fields = original_fields
        else:
            # Perform matching analysis
            matching_report = self.data_matcher.generate_matching_report(
                combined_df)

            # Find duplicates and similar records
            df_with_duplicates = self.data_matcher.find_duplicates(combined_df)
            similar_groups = self.data_matcher.find_similar_records(combined_df)

            # Entity resolution
            entity_resolution = self.data_matcher.resolve_entities(combined_df)

        return {
            'matching_report': matching_report,
            'duplicates': df_with_duplicates,
            'similar_groups': similar_groups,
            'entity_resolution': entity_resolution,
            'combined_data': combined_df
        }

    def get_comprehensive_analysis(self, original_data: Dict[str, pd.DataFrame],
                                   cleaned_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Get comprehensive analysis including schema matching and data matching.

        Args:
            original_data: Original data before cleaning
            cleaned_data: Data after cleaning

        Returns:
            Dict[str, Any]: Comprehensive analysis results
        """
        logger.info("Performing comprehensive analysis...")

        # Basic cleaning summary
        cleaning_summary = self.get_cleaning_summary(
            original_data, cleaned_data)

        # Schema compatibility analysis
        schema_analysis = self.analyze_schema_compatibility(cleaned_data)

        # Data matching analysis
        matching_analysis = self.perform_data_matching(cleaned_data)

        # Combine all results
        comprehensive_analysis = {
            'cleaning_summary': cleaning_summary,
            'schema_analysis': schema_analysis,
            'matching_analysis': matching_analysis,
            'overall_quality_score': self._calculate_overall_quality_score(
                cleaning_summary, schema_analysis, matching_analysis
            )
        }

        return comprehensive_analysis

    def _calculate_overall_quality_score(self, cleaning_summary: Dict,
                                         schema_analysis: Dict,
                                         matching_analysis: Dict) -> float:
        """
        Calculate overall data quality score.

        Args:
            cleaning_summary: Cleaning summary results
            schema_analysis: Schema analysis results
            matching_analysis: Matching analysis results

        Returns:
            float: Overall quality score (0-100)
        """
        scores = []

        # Data cleaning quality scores
        for source, stats in cleaning_summary.items():
            if 'data_quality_score' in stats:
                scores.append(stats['data_quality_score'])

        # Schema compatibility score
        if 'compatibility' in schema_analysis:
            compatibility_score = schema_analysis['compatibility']['overall_compatibility'] * 100
            scores.append(compatibility_score)

        # Data matching quality (inverse of duplicate/similarity rates)
        if 'matching_report' in matching_analysis:
            matching_report = matching_analysis['matching_report']
            duplicate_rate = matching_report['duplicate_analysis']['duplicate_rate']
            similarity_rate = matching_report['similarity_analysis']['similarity_rate']

            # Lower duplicate/similarity rates = higher quality
            matching_quality = (
                1 - (duplicate_rate + similarity_rate) / 2) * 100
            scores.append(matching_quality)

        return round(np.mean(scores), 2) if scores else 0.0


if __name__ == "__main__":
    # Example usage
    from data_loader import DataLoader

    # Load data
    loader = DataLoader()
    raw_data = loader.load_all_sources()

    # Clean data
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_all_data(raw_data)
    standardized_data = cleaner.standardize_columns(cleaned_data)

    # Print summary
    summary = cleaner.get_cleaning_summary(raw_data, standardized_data)
    for source, stats in summary.items():
        print(f"\n{source.upper()} Cleaning Summary:")
        print(f"  Original rows: {stats['original_rows']}")
        print(f"  Cleaned rows: {stats['cleaned_rows']}")
        print(f"  Rows removed: {stats['rows_removed']}")
        print(f"  Columns: {stats['columns_standardized']}")