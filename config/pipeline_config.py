"""
BTS Aviation Delay Intelligence System
=======================================
File    : pipeline_config.py
Purpose : Central configuration for all pipeline scripts.
          All constants defined here — never hardcoded
          in individual pipeline scripts.

Author  : Narsing Shiva Kumar
Version : 1.0 | August 2026

Usage   : from config.pipeline_config import RAW_PATH, BRONZE_PATH
"""

import os

# ── Environment ───────────────────────────────────────────────
JAVA_HOME   = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
HADOOP_HOME = r"C:\hadoop"
VENV_PYTHON = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"

# ── Data Paths ────────────────────────────────────────────────
RAW_PATH    = "data/raw/"
BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"
GOLD_PATH   = "data/gold/"

# ── Validation Thresholds ─────────────────────────────────────
MIN_ROWS_PER_FILE   = 400_000
MIN_TOTAL_ROWS      = 18_000_000
MAX_TOTAL_ROWS      = 22_000_000
EXPECTED_FILE_COUNT = 36
EXPECTED_COL_COUNT  = 37

# ── Bronze Settings ───────────────────────────────────────────
BRONZE_COMPRESSION  = "snappy"
BRONZE_PARTITION_BY = ["YEAR", "MONTH"]

# ── Silver Settings ───────────────────────────────────────────
SILVER_PARTITION_BY = ["flight_year", "flight_month"]
# Columns dropped in Silver (ADR-010)
SILVER_DROP_COLS    = ["FLIGHTS"]

# ── Gold Settings ─────────────────────────────────────────────
GOLD_DB_HOST        = "localhost"
GOLD_DB_NAME        = "bts_gold"
GOLD_DB_PORT        = 3306

# ── Confirmed Data Statistics (August 2026) ───────────────────
# These are real confirmed numbers from health check
CONFIRMED_TOTAL_ROWS     = 20_928_599
CONFIRMED_TAIL_NUM_NULLS = 48_139
CONFIRMED_DELAY_NULL_PCT = 79.11
CONFIRMED_ARR_DEL15_VIOLATIONS = 0
CONFIRMED_DUPLICATES     = 0
CONFIRMED_CARRIERS       = 15
