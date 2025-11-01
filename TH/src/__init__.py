"""
Job Analytics - Big Data Integration Project
==========================================

A comprehensive big data integration project for job market analytics.

This package provides tools for:
- Data ingestion from multiple sources
- Data cleaning and transformation
- Machine learning models
- Analytics and visualization
- API and dashboard services

Author: [Your Name]
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "[Your Name]"
__email__ = "[your.email@example.com]"

# Import main modules
from .etl import data_loader, data_cleaner, data_transformer
from .analytics import trend_analyzer, salary_analyzer, skills_analyzer
from .models import salary_predictor, skills_clusterer, sentiment_analyzer
from .utils import config_manager, database_manager, logger

__all__ = [
    "data_loader",
    "data_cleaner", 
    "data_transformer",
    "trend_analyzer",
    "salary_analyzer",
    "skills_analyzer",
    "salary_predictor",
    "skills_clusterer",
    "sentiment_analyzer",
    "config_manager",
    "database_manager",
    "logger"
]
