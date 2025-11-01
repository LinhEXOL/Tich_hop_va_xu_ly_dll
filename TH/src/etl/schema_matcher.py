"""
Schema Matching and Data Matching Module
========================================

Handles automatic schema matching across different data sources
and data matching for entity resolution.
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from difflib import SequenceMatcher
from collections import defaultdict
import Levenshtein

logger = logging.getLogger(__name__)


class SchemaMatcher:
    """
    A class to handle schema matching across different data sources.
    """
    
    def __init__(self):
        """Initialize the SchemaMatcher."""
        
        # Define schema mapping rules
        self.schema_mappings = {
            # Job Title mappings
            'job_title': {
                'careerlink': ['tên công việc'],
                'joboko': ['tên công việc'],
                'topcv': ['tên công việc']
            },
            
            # Company Name mappings
            'company_name': {
                'careerlink': ['tên công ty'],
                'joboko': ['tên công ty'],
                'topcv': ['tên công ty']
            },
            
            # Location mappings
            'location': {
                'careerlink': ['Địa điểm công việc'],
                'joboko': ['địa điểm'],
                'topcv': ['địa điểm']
            },
            
            # Salary mappings
            'salary': {
                'careerlink': ['Mức lương'],
                'joboko': ['mức lương'],
                'topcv': ['mức lương']
            },
            
            # Industry mappings
            'industry': {
                'careerlink': ['ngành nghề'],
                'joboko': ['ngành nghề'],
                'topcv': ['ngành nghề']
            },
            
            # Job Description mappings
            'job_description': {
                'careerlink': ['mô tả công việc'],
                'joboko': ['mô tả công việc'],
                'topcv': ['mô tả công việc']
            },
            
            # Skills mappings
            'skills': {
                'careerlink': ['kĩ năng yêu cầu'],
                'joboko': ['kĩ năng yêu cầu'],
                'topcv': ['kĩ năng yêu cầu']
            },
            
            # Experience mappings
            'experience': {
                'careerlink': ['Kinh nghiệm'],
                'joboko': ['kinh nghiệm'],
                'topcv': ['kinh nghiệm']
            },
            
            # Additional fields for Vietnamese job data
            'job_type': {
                'careerlink': ['loại công việc'],
                'joboko': ['loại công việc'],
                'topcv': ['loại công việc']
            },
            
            'job_level': {
                'careerlink': ['cấp bậc'],
                'joboko': ['cấp bậc'],
                'topcv': ['cấp bậc']
            },
            
            'education': {
                'careerlink': ['học vấn'],
                'joboko': ['học vấn'],
                'topcv': ['học vấn']
            },
            
            'benefits': {
                'careerlink': ['quyền lợi'],
                'joboko': ['quyền lợi'],
                'topcv': ['quyền lợi']
            }
        }
        
        # Define data type mappings
        self.data_type_mappings = {
            'job_title': 'string',
            'company_name': 'string',
            'location': 'string',
            'salary_min': 'numeric',
            'salary_max': 'numeric',
            'industry': 'string',
            'job_description': 'string',
            'skills': 'string',
            'experience': 'numeric',
            'rating': 'numeric',
            'company_size': 'string'
        }
    
    def detect_schema(self, df: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """
        Detect schema for a given DataFrame.
        
        Args:
            df: DataFrame to analyze
            source_name: Name of the data source
            
        Returns:
            Dict[str, Any]: Detected schema information
        """
        schema_info = {
            'source': source_name,
            'columns': list(df.columns),
            'data_types': df.dtypes.to_dict(),
            'sample_values': {},
            'mapped_fields': {}
        }
        
        # Get sample values for each column
        for col in df.columns:
            sample_values = df[col].dropna().head(3).tolist()
            schema_info['sample_values'][col] = sample_values
        
        # Map columns to standard fields
        schema_info['mapped_fields'] = self._map_columns_to_standard_fields(df.columns, source_name)
        
        return schema_info
    
    def _map_columns_to_standard_fields(self, columns: List[str], source_name: str) -> Dict[str, str]:
        """
        Map actual columns to standard field names.
        
        Args:
            columns: List of column names
            source_name: Name of the data source
            
        Returns:
            Dict[str, str]: Mapping from standard field to actual column
        """
        mapping = {}
        
        for standard_field, source_mappings in self.schema_mappings.items():
            if source_name in source_mappings:
                possible_columns = source_mappings[source_name]
                
                # Find matching column
                for col in columns:
                    if col in possible_columns:
                        mapping[standard_field] = col
                        break
                
                # If no exact match, try fuzzy matching
                if standard_field not in mapping:
                    best_match = self._fuzzy_match_column(columns, possible_columns)
                    if best_match:
                        mapping[standard_field] = best_match
        
        return mapping
    
    def _fuzzy_match_column(self, columns: List[str], possible_columns: List[str]) -> Optional[str]:
        """
        Perform fuzzy matching between columns and possible column names.
        
        Args:
            columns: Available columns
            possible_columns: Possible column names to match
            
        Returns:
            Optional[str]: Best matching column name
        """
        best_match = None
        best_score = 0.0
        
        for col in columns:
            for possible_col in possible_columns:
                # Use Levenshtein distance for fuzzy matching
                similarity = Levenshtein.ratio(col.lower(), possible_col.lower())
                
                if similarity > best_score and similarity > 0.8:  # Threshold for matching
                    best_score = similarity
                    best_match = col
        
        return best_match
    
    def create_unified_schema(self, schemas: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Create unified schema from multiple data sources.
        
        Args:
            schemas: Dictionary of schema information from different sources
            
        Returns:
            Dict[str, Any]: Unified schema definition
        """
        unified_schema = {
            'standard_fields': list(self.schema_mappings.keys()),
            'data_types': self.data_type_mappings,
            'source_mappings': {},
            'coverage_analysis': {}
        }
        
        # Analyze field coverage across sources
        field_coverage = defaultdict(list)
        
        for source, schema in schemas.items():
            unified_schema['source_mappings'][source] = schema['mapped_fields']
            
            for field in schema['mapped_fields']:
                field_coverage[field].append(source)
        
        # Calculate coverage statistics
        for field, sources in field_coverage.items():
            unified_schema['coverage_analysis'][field] = {
                'sources': sources,
                'coverage_rate': len(sources) / len(schemas),
                'is_common': len(sources) == len(schemas)
            }
        
        return unified_schema
    
    def validate_schema_compatibility(self, schemas: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Validate schema compatibility across sources.
        
        Args:
            schemas: Dictionary of schema information
            
        Returns:
            Dict[str, Any]: Compatibility analysis
        """
        compatibility_report = {
            'overall_compatibility': 0.0,
            'field_compatibility': {},
            'data_type_consistency': {},
            'recommendations': []
        }
        
        # Analyze field compatibility
        all_fields = set()
        for schema in schemas.values():
            all_fields.update(schema['mapped_fields'].keys())
        
        compatible_fields = 0
        for field in all_fields:
            sources_with_field = [s for s, schema in schemas.items() 
                                if field in schema['mapped_fields']]
            
            compatibility_score = len(sources_with_field) / len(schemas)
            compatibility_report['field_compatibility'][field] = {
                'coverage': compatibility_score,
                'sources': sources_with_field,
                'is_compatible': compatibility_score >= 0.5
            }
            
            if compatibility_score >= 0.5:
                compatible_fields += 1
        
        compatibility_report['overall_compatibility'] = compatible_fields / len(all_fields) if len(all_fields) > 0 else 0.0
        
        # Data type consistency analysis
        for field in all_fields:
            data_types = []
            for schema in schemas.values():
                if field in schema['mapped_fields']:
                    mapped_col = schema['mapped_fields'][field]
                    if mapped_col in schema['data_types']:
                        data_types.append(str(schema['data_types'][mapped_col]))
            
            if data_types:
                most_common_type = max(set(data_types), key=data_types.count)
                consistency_score = data_types.count(most_common_type) / len(data_types)
                
                compatibility_report['data_type_consistency'][field] = {
                    'most_common_type': most_common_type,
                    'consistency_score': consistency_score,
                    'all_types': list(set(data_types))
                }
        
        # Generate recommendations
        if compatibility_report['overall_compatibility'] < 0.7:
            compatibility_report['recommendations'].append(
                "Low schema compatibility detected. Consider data preprocessing."
            )
        
        return compatibility_report


class DataMatcher:
    """
    A class to handle data matching and entity resolution.
    """
    
    def __init__(self):
        """Initialize the DataMatcher."""
        self.similarity_threshold = 0.8
        self.exact_match_fields = ['source']  # Fields that must match exactly
        self.fuzzy_match_fields = ['job_title_clean', 'company_name', 'location_clean']
    
    def find_duplicates(self, df: pd.DataFrame, 
                       key_fields: List[str] = None) -> pd.DataFrame:
        """
        Find duplicate records based on key fields.
        
        Args:
            df: DataFrame to analyze
            key_fields: Fields to use for duplicate detection
            
        Returns:
            pd.DataFrame: DataFrame with duplicate indicators
        """
        if key_fields is None:
            key_fields = ['job_title_clean', 'company_name', 'location_clean']
        
        # Create a copy with duplicate indicators
        df_with_duplicates = df.copy()
        df_with_duplicates['is_duplicate'] = False
        df_with_duplicates['duplicate_group'] = None
        
        # Group by key fields to find exact duplicates
        exact_duplicates = df.groupby(key_fields).size()
        duplicate_groups = exact_duplicates[exact_duplicates > 1].index
        
        group_id = 0
        for group in duplicate_groups:
            mask = True
            for i, field in enumerate(key_fields):
                mask = mask & (df[field] == group[i])
            
            df_with_duplicates.loc[mask, 'is_duplicate'] = True
            df_with_duplicates.loc[mask, 'duplicate_group'] = group_id
            group_id += 1
        
        return df_with_duplicates
    
    def find_similar_records(self, df: pd.DataFrame, 
                           similarity_threshold: float = None,
                           max_records: int = 1000) -> List[List[int]]:
        """
        Find similar records using fuzzy matching.
        
        Args:
            df: DataFrame to analyze
            similarity_threshold: Threshold for similarity matching
            max_records: Maximum number of records to process (for performance)
            
        Returns:
            List[List[int]]: Groups of similar record indices
        """
        if similarity_threshold is None:
            similarity_threshold = self.similarity_threshold
        
        # Limit records for performance
        if len(df) > max_records:
            logger.warning(f"Limiting similarity analysis to {max_records} records for performance")
            df = df.head(max_records)
        
        similar_groups = []
        processed = set()
        
        # Use sampling for very large datasets
        if len(df) > 500:
            logger.info("Using sampling for similarity analysis")
            sample_size = min(500, len(df))
            df = df.sample(n=sample_size, random_state=42)
        
        for i in range(len(df)):
            if i in processed:
                continue
            
            current_group = [i]
            processed.add(i)
            
            # Limit comparisons per record for performance
            max_comparisons = min(100, len(df) - i - 1)
            comparisons_made = 0
            
            for j in range(i + 1, len(df)):
                if j in processed or comparisons_made >= max_comparisons:
                    continue
                
                similarity = self._calculate_record_similarity(df.iloc[i], df.iloc[j])
                comparisons_made += 1
                
                if similarity >= similarity_threshold:
                    current_group.append(j)
                    processed.add(j)
            
            if len(current_group) > 1:
                similar_groups.append(current_group)
        
        return similar_groups
    
    def _calculate_record_similarity(self, record1: pd.Series, record2: pd.Series) -> float:
        """
        Calculate similarity between two records.
        
        Args:
            record1: First record
            record2: Second record
            
        Returns:
            float: Similarity score (0-1)
        """
        similarities = []
        
        for field in self.fuzzy_match_fields:
            if field in record1.index and field in record2.index:
                val1 = str(record1[field]) if pd.notna(record1[field]) else ""
                val2 = str(record2[field]) if pd.notna(record2[field]) else ""
                
                if val1 and val2:
                    similarity = Levenshtein.ratio(val1.lower(), val2.lower())
                    similarities.append(similarity)
        
        return np.mean(similarities) if similarities else 0.0
    
    def resolve_entities(self, df: pd.DataFrame, 
                        entity_fields: List[str] = None) -> Dict[str, Any]:
        """
        Resolve entities and create entity mappings.
        
        Args:
            df: DataFrame to analyze
            entity_fields: Fields to use for entity resolution
            
        Returns:
            Dict[str, Any]: Entity resolution results
        """
        if entity_fields is None:
            entity_fields = ['company_name', 'job_title_clean']
        
        entity_resolution = {
            'company_mapping': {},
            'job_title_mapping': {},
            'entity_statistics': {}
        }
        
        # Company entity resolution
        if 'company_name' in df.columns:
            companies = df['company_name'].dropna().unique()
            company_mapping = self._create_entity_mapping(companies, 'company')
            entity_resolution['company_mapping'] = company_mapping
        
        # Job title entity resolution
        if 'job_title_clean' in df.columns:
            job_titles = df['job_title_clean'].dropna().unique()
            job_title_mapping = self._create_entity_mapping(job_titles, 'job_title')
            entity_resolution['job_title_mapping'] = job_title_mapping
        
        # Calculate statistics
        entity_resolution['entity_statistics'] = {
            'total_companies': len(df['company_name'].dropna().unique()) if 'company_name' in df.columns else 0,
            'total_job_titles': len(df['job_title_clean'].dropna().unique()) if 'job_title_clean' in df.columns else 0,
            'unique_companies': len(entity_resolution['company_mapping']),
            'unique_job_titles': len(entity_resolution['job_title_mapping'])
        }
        
        return entity_resolution
    
    def _create_entity_mapping(self, entities: List[str], entity_type: str) -> Dict[str, str]:
        """
        Create entity mapping for deduplication.
        
        Args:
            entities: List of entity names
            entity_type: Type of entity (company, job_title, etc.)
            
        Returns:
            Dict[str, str]: Mapping from original to canonical entity name
        """
        entity_mapping = {}
        canonical_entities = []
        
        for entity in entities:
            entity_lower = entity.lower().strip()
            
            # Find similar canonical entity
            best_match = None
            best_similarity = 0.0
            
            for canonical in canonical_entities:
                similarity = Levenshtein.ratio(entity_lower, canonical.lower())
                if similarity > best_similarity and similarity >= self.similarity_threshold:
                    best_similarity = similarity
                    best_match = canonical
            
            if best_match:
                entity_mapping[entity] = best_match
            else:
                # Create new canonical entity
                canonical_entities.append(entity)
                entity_mapping[entity] = entity
        
        return entity_mapping
    
    def generate_matching_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate comprehensive data matching report.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dict[str, Any]: Matching report
        """
        report = {
            'total_records': len(df),
            'duplicate_analysis': {},
            'similarity_analysis': {},
            'entity_resolution': {},
            'recommendations': []
        }
        
        # Duplicate analysis
        df_with_duplicates = self.find_duplicates(df)
        duplicate_count = df_with_duplicates['is_duplicate'].sum()
        
        report['duplicate_analysis'] = {
            'duplicate_count': int(duplicate_count),
            'duplicate_rate': duplicate_count / len(df),
            'duplicate_groups': int(df_with_duplicates['duplicate_group'].nunique())
        }
        
        # Similarity analysis
        similar_groups = self.find_similar_records(df)
        similar_count = sum(len(group) for group in similar_groups)
        
        report['similarity_analysis'] = {
            'similar_groups': len(similar_groups),
            'similar_records': similar_count,
            'similarity_rate': similar_count / len(df)
        }
        
        # Entity resolution
        entity_resolution = self.resolve_entities(df)
        report['entity_resolution'] = entity_resolution['entity_statistics']
        
        # Generate recommendations
        if report['duplicate_analysis']['duplicate_rate'] > 0.1:
            report['recommendations'].append(
                "High duplicate rate detected. Consider implementing deduplication."
            )
        
        if report['similarity_analysis']['similarity_rate'] > 0.2:
            report['recommendations'].append(
                "High similarity rate detected. Consider entity resolution."
            )
        
        return report


if __name__ == "__main__":
    # Example usage
    import pandas as pd
    
    # Create sample data
    sample_data = {
        'glassdoor': pd.DataFrame({
            'Job Title': ['Data Scientist', 'Software Engineer', 'Data Analyst'],
            'Company Name': ['Google', 'Microsoft', 'Amazon'],
            'Location': ['San Francisco', 'Seattle', 'New York']
        }),
        'monster': pd.DataFrame({
            'job_title': ['Data Scientist', 'Software Developer', 'Data Analyst'],
            'organization': ['Google Inc.', 'Microsoft Corp', 'Amazon Web Services'],
            'location': ['SF', 'Seattle, WA', 'NYC']
        })
    }
    
    # Test Schema Matching
    schema_matcher = SchemaMatcher()
    schemas = {}
    
    for source, df in sample_data.items():
        schemas[source] = schema_matcher.detect_schema(df, source)
    
    unified_schema = schema_matcher.create_unified_schema(schemas)
    compatibility = schema_matcher.validate_schema_compatibility(schemas)
    
    print("Schema Matching Results:")
    print(f"Overall Compatibility: {compatibility['overall_compatibility']:.2%}")
    print(f"Field Coverage: {len(unified_schema['coverage_analysis'])} fields")
    
    # Test Data Matching
    data_matcher = DataMatcher()
    combined_df = pd.concat(sample_data.values(), ignore_index=True)
    matching_report = data_matcher.generate_matching_report(combined_df)
    
    print("\nData Matching Results:")
    print(f"Total Records: {matching_report['total_records']}")
    print(f"Duplicate Rate: {matching_report['duplicate_analysis']['duplicate_rate']:.2%}")
    print(f"Similarity Rate: {matching_report['similarity_analysis']['similarity_rate']:.2%}")
