import os
import json
import pandas as pd
import numpy as np
import requests
import traceback
from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
from collections import Counter
import hashlib
from functools import lru_cache


load_dotenv()
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3:8b"
CSV_PATH = "events.csv"

app = Flask(__name__)

QUERY_CACHE = {}
CACHE_MAX_SIZE = 100
DEFAULT_LANGUAGE = 'en'

TRANSLATIONS = {
    'en': {
        'total_events': 'Total Events',
        'found_events': 'Found {count} events',
        'equipment': 'Equipment',
        'category': 'Category',
        'area': 'Area',
        'date_range': 'Date Range',
        'average': 'Average',
        'trend': 'Trend',
        'anomaly_detected': 'Anomaly Detected',
        'comparison': 'Comparison',
        'insights': 'Key Insights',
        'recommendation': 'Recommendation',
        'executive_summary': 'Executive Summary',
        'detailed_breakdown': 'Detailed Breakdown',
        'top_equipment': 'Top Equipment',
        'specific_equipment': 'Specific Equipment Names',
        'pi_sensors': 'PI Tag Sensors',
        'root_cause': 'Root Cause Analysis',
        'corrective_actions': 'Recommended Actions',
        'highest': 'Highest',
        'lowest': 'Lowest',
        'normal': 'Normal',
        'critical': 'Critical',
        'events_on': 'Events on',
        'most_events': 'Most events occurred in',
        'least_events': 'Least events occurred in',
    },
    'id': {
        'total_events': 'Total Event',
        'found_events': 'Ditemukan {count} event',
        'equipment': 'Equipment',
        'category': 'Kategori',
        'area': 'Area',
        'date_range': 'Rentang Tanggal',
        'average': 'Rata-rata',
        'trend': 'Trend',
        'anomaly_detected': 'Anomali Terdeteksi',
        'comparison': 'Perbandingan',
        'insights': 'Insight Utama',
        'recommendation': 'Rekomendasi',
        'executive_summary': 'Ringkasan Eksekutif',
        'detailed_breakdown': 'Rincian Detail',
        'top_equipment': 'Equipment Teratas',
        'specific_equipment': 'Nama Equipment Spesifik',
        'pi_sensors': 'Sensor PI Tag',
        'root_cause': 'Analisis Akar Masalah',
        'corrective_actions': 'Tindakan yang Disarankan',
        'highest': 'Tertinggi',
        'lowest': 'Terendah',
        'normal': 'Normal',
        'critical': 'Kritis',
        'events_on': 'Event pada',
        'most_events': 'Event terbanyak terjadi di',
        'least_events': 'Event tersedikit terjadi di',
    }
}

def detect_language(query):
    """Auto-detect language from query"""
    indonesian_keywords = ['apa', 'berapa', 'yang', 'di', 'pada', 'bulan', 'tanggal', 'dan', 'atau', 'dengan', 'untuk', 'bandingkan', 'analisis', 'ringkasan', 'rekomendasi', 'perbandingan', 'tertinggi', 'terendah', 'kritis', 'normal']
    q = query.lower()
    id_count = sum(1 for kw in indonesian_keywords if kw in q.split())
    return 'id' if id_count >= 2 else 'en'

def t(key, lang='en', **kwargs):
    """Translate key with optional formatting"""
    text = TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)
    return text.format(**kwargs) if kwargs else text
# ====================================================
# LOAD CSV
# ====================================================
def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File {path} tidak ditemukan.")
    
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except:
        df = pd.read_csv(path, encoding="latin1", low_memory=False)

    df.columns = [c.strip() for c in df.columns]
    
    for col in df.columns:
        if any(kw in col.lower() for kw in ["time", "date", "timestamp"]):
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    
    print(f"✅ Loaded: {len(df):,} rows × {len(df.columns)} cols")
    return df

df = load_csv(CSV_PATH)

# ====================================================
# ENHANCED COLUMN MAPPING WITH HIERARCHY
# ====================================================
def identify_key_columns(df):
    """Identify key columns with 3-level hierarchy support"""
    
    mapping = {
        'identifier': None,        # Level 1: Equipment code (GB-651)
        'equipment_name': None,    # Level 2: Specific name (AT-651)
        'pi_tag': None,           # Level 3: Sensor tag
        'category': None,
        'area': None,
        'status': None,
        'severity': None,
        'date': None,
        'description': None,
        'limit_type': None
    }
    
    # Level 1: Equipment identifier
    for col in ['Equipment', 'Asset', 'Equipment Code', 'Asset Code']:
        if col in df.columns:
            mapping['identifier'] = col
            break
    
    # Level 2: Equipment name (more specific)
    for col in ['Equipment Name', 'Asset Name', 'Name', 'Equipment_Name', 'Specific Equipment']:
        if col in df.columns and col != mapping['identifier']:
            mapping['equipment_name'] = col
            break
    
    # Level 3: PI Tag
    for col in ['TagNamePI', 'PI Tag', 'Tag Name', 'PI_Tag', 'Tag', 'Sensor']:
        if col in df.columns:
            mapping['pi_tag'] = col
            break
    
    # Category
    for col in ['Asset Category', 'Type', 'Category', 'Event Type', 'Alarm Type']:
        if col in df.columns:
            mapping['category'] = col
            break
    
    # Area
    for col in ['Plant Area', 'Area Authority', 'Area', 'Location']:
        if col in df.columns:
            mapping['area'] = col
            break
    
    # Status
    for col in ['Status', 'Event Status', 'Alarm Status']:
        if col in df.columns:
            mapping['status'] = col
            break
    
    # Severity
    if 'Severity' in df.columns:
        mapping['severity'] = 'Severity'
    
    # Limit type (High/Low)
    for col in df.columns:
        if any(kw in col.lower() for kw in ['limit', 'alarm']) and col not in [mapping['category']]:
            sample_values = df[col].dropna().astype(str).str.lower().unique()
            if any('high' in str(v) or 'low' in str(v) for v in sample_values):
                mapping['limit_type'] = col
                break
    
    # Date
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            mapping['date'] = col
            break
    
    # Description
    for col in ['Description', 'TextMessage', 'Reason', 'Message', 'Details']:
        if col in df.columns:
            mapping['description'] = col
            break
    
    return mapping

KEY_COLUMNS = identify_key_columns(df)

# ====================================================
# ENHANCED PROFILER
# ====================================================
def get_enhanced_profile(df, key_columns):
    """Build comprehensive profile"""
    
    profile = {
        "total_rows": len(df),
        "total_events": len(df),
        "structure": {},
        "key_values": {}
    }
    
    # Date range
    if key_columns['date']:
        date_col = key_columns['date']
        valid_dates = df[date_col].dropna()
        if len(valid_dates) > 0:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            
            profile["date_range"] = {
                "start": str(min_date.date()),
                "end": str(max_date.date()),
                "total_days": (max_date - min_date).days + 1,
                "span_months": sorted(list(valid_dates.dt.month.unique()))
            }
            
            # Daily stats
            df_temp = df[df[date_col].notna()].copy()
            df_temp['date_only'] = df_temp[date_col].dt.date
            events_per_day = df_temp.groupby('date_only').size()
            
            profile["daily_stats"] = {
                "avg_events_per_day": round(events_per_day.mean(), 2),
                "max_events_in_day": int(events_per_day.max()),
                "min_events_in_day": int(events_per_day.min()),
                "total_active_days": len(events_per_day)
            }
    
    # Profile key columns
    for role, col_name in key_columns.items():
        if col_name and col_name in df.columns:
            profile["structure"][role] = col_name
            
            if role in ['identifier', 'category', 'area', 'status', 'severity']:
                value_counts = df[col_name].value_counts()
                total = len(df)
                
                top_values = {}
                for val, count in value_counts.head(20).items():
                    top_values[str(val)] = {
                        "count": int(count),
                        "percentage": round(count / total * 100, 2)
                    }
                
                profile["key_values"][role] = {
                    "total_unique": len(value_counts),
                    "top_values": top_values
                }
    
    return profile

PROFILE = get_enhanced_profile(df, KEY_COLUMNS)

# ====================================================
# CACHING SYSTEM
# ====================================================
def get_cache_key(query, filters):
    """Generate cache key"""
    key_str = f"{query.lower().strip()}_{json.dumps(filters, sort_keys=True)}"
    return hashlib.md5(key_str.encode()).hexdigest()

def get_from_cache(cache_key):
    """Get from cache"""
    if cache_key in QUERY_CACHE:
        cached = QUERY_CACHE[cache_key]
        # Check if expired (5 minutes)
        if (datetime.now() - cached['timestamp']).seconds < 300:
            return cached['data']
    return None

def save_to_cache(cache_key, data):
    """Save to cache with size limit"""
    global QUERY_CACHE
    
    if len(QUERY_CACHE) >= CACHE_MAX_SIZE:
        # Remove oldest
        oldest = min(QUERY_CACHE.items(), key=lambda x: x[1]['timestamp'])
        del QUERY_CACHE[oldest[0]]
    
    QUERY_CACHE[cache_key] = {
        'data': data,
        'timestamp': datetime.now()
    }

# ====================================================
# CONFIDENCE CALCULATOR
# ====================================================
def calculate_confidence(df_filtered, filters, query, method):
    """Calculate answer confidence (0-100)"""
    
    confidence = 100.0
    reasons = []
    
    # 1. Result count factor
    result_count = len(df_filtered)
    if result_count == 0:
        confidence = 0
        reasons.append("Tidak ada data yang match")
        return confidence, reasons
    elif result_count < 3:
        confidence -= 20
        reasons.append(f"Sangat sedikit data ({result_count} events)")
    elif result_count < 10:
        confidence -= 10
        reasons.append(f"Data terbatas ({result_count} events)")
    
    # 2. Filter clarity
    if not filters:
        confidence -= 15
        reasons.append("Tidak ada filter spesifik")
    
    # 3. Query ambiguity
    ambiguous_terms = ['itu', 'ini', 'nya', 'tersebut', 'yang tadi']
    if any(term in query.lower() for term in ambiguous_terms):
        confidence -= 10
        reasons.append("Query agak ambigu")
    
    # 4. Method factor
    if method == 'identifier_search':
        confidence = min(confidence + 10, 100)
        reasons.append("Pencarian identifier spesifik")
    elif method == 'llm_analysis':
        confidence -= 5
        reasons.append("Analisis LLM")
    
    # 5. Data quality
    if result_count > 0:
        null_ratio = df_filtered.isnull().sum().sum() / (len(df_filtered) * len(df_filtered.columns))
        if null_ratio > 0.3:
            confidence -= 15
            reasons.append("Banyak nilai kosong dalam data")
    
    # 6. Date range coverage
    if filters.get('months') or filters.get('day'):
        confidence = min(confidence + 5, 100)
        reasons.append("Filter tanggal spesifik")
    
    return max(confidence, 0), reasons

# ====================================================
# FILTER EXTRACTOR - ENHANCED
# ====================================================
def extract_filters_from_query(query, df, key_columns):
    """Extract filters from query - ENHANCED untuk support equipment + date + cross-month"""
    
    q = query.lower()
    filters = {}
    df_filtered = df.copy()
    filter_descriptions = []
    
    date_col = key_columns['date']
    
    # Month filters
    months_map = {
        'januari': 1, 'january': 1, 'jan': 1,
        'februari': 2, 'february': 2, 'feb': 2,
        'maret': 3, 'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'mei': 5, 'may': 5,
        'juni': 6, 'june': 6, 'jun': 6,
        'juli': 7, 'july': 7, 'jul': 7,
        'agustus': 8, 'august': 8, 'aug': 8, 'ags': 8,
        'september': 9, 'sept': 9, 'sep': 9,
        'oktober': 10, 'october': 10, 'oct': 10, 'okt': 10,
        'november': 11, 'nov': 11,
        'desember': 12, 'december': 12, 'des': 12, 'dec': 12
    }
    
    month_names = {1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April', 
                   5: 'Mei', 6: 'Juni', 7: 'Juli', 8: 'Agustus',
                   9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'}
    
    found_months = []
    for month_str, month_num in months_map.items():
        if month_str in q:
            found_months.append(month_num)
    
    if found_months and date_col:
        found_months = list(set(found_months))
        df_filtered = df_filtered[df_filtered[date_col].dt.month.isin(found_months)]
        filters['months'] = found_months
        
        if len(found_months) == 1:
            filter_descriptions.append(f"bulan {month_names[found_months[0]]}")
        else:
            month_strs = [month_names[m] for m in sorted(found_months)]
            filter_descriptions.append(f"bulan {' dan '.join(month_strs)}")
    
    # Date range (tanggal X - Y atau tanggal X sd Y) - ENHANCED: Support cross-month
    # Pattern 1: "28 august - 16 september" or "28 agustus sd 16 september"
    cross_month_pattern = r'(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)\s*[-–to/sd]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
    match = re.search(cross_month_pattern, q, re.IGNORECASE)
    
    if match and date_col:
        start_day = int(match.group(1))
        start_month_str = match.group(2).lower()
        end_day = int(match.group(3))
        end_month_str = match.group(4).lower()
        
        start_month = months_map.get(start_month_str)
        end_month = months_map.get(end_month_str)
        
        if start_month and end_month:
            # Get year from data
            year = df_filtered[date_col].dt.year.mode()[0] if len(df_filtered) > 0 else datetime.now().year
            
            try:
                from datetime import datetime
                start_date = datetime(year, start_month, start_day).date()
                end_date = datetime(year, end_month, end_day).date()
                
                df_filtered = df_filtered[
                    (df_filtered[date_col].dt.date >= start_date) & 
                    (df_filtered[date_col].dt.date <= end_date)
                ]
                filters['date_range_full'] = (str(start_date), str(end_date))
                filter_descriptions.append(f"{start_day} {month_names[start_month]} - {end_day} {month_names[end_month]}")
                match = None  # Prevent further date processing
            except:
                pass
    
    # Pattern 2: Same month date range "tanggal X - Y"
    if not match:
        date_range_pattern = r'tanggal\s*(\d{1,2})\s*[-–sd/]\s*(\d{1,2})'
        match = re.search(date_range_pattern, q)
        if match and date_col:
            start_day = int(match.group(1))
            end_day = int(match.group(2))
            df_filtered = df_filtered[
                (df_filtered[date_col].dt.day >= start_day) & 
                (df_filtered[date_col].dt.day <= end_day)
            ]
            filters['date_range'] = (start_day, end_day)
            filter_descriptions.append(f"tanggal {start_day}-{end_day}")
    
    # Single date
    if not match:
        single_date_pattern = r'tanggal\s*(\d{1,2})\b'
        match = re.search(single_date_pattern, q)
        if match and date_col:
            day = int(match.group(1))
            df_filtered = df_filtered[df_filtered[date_col].dt.day == day]
            filters['day'] = day
            filter_descriptions.append(f"tanggal {day}")
    
    # Equipment/Identifier filter (detect patterns like GB-651, EA-119, etc)
    # ENHANCED: Support multiple equipment dengan "dan", "vs", "versus"
    identifier_patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    
    found_identifiers = []
    for pattern in identifier_patterns:
        matches = re.findall(pattern, query.upper())
        found_identifiers.extend(matches)
    
    # Remove duplicates while preserving order
    found_identifiers = list(dict.fromkeys(found_identifiers))
    
    if found_identifiers and key_columns.get('identifier'):
        id_col = key_columns['identifier']
        if id_col in df_filtered.columns:
            # Check if it's a comparison query (multiple equipment)
            comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'dan']
            is_comparison = any(kw in q for kw in comparison_keywords) and len(found_identifiers) >= 2
            
            if is_comparison:
                # Don't filter - let comparison handler handle it
                print(f"  🔄 Detected comparison query with: {found_identifiers}")
                filters['comparison_identifiers'] = found_identifiers
                # Don't add to filter_descriptions yet - comparison will handle
            elif len(found_identifiers) == 1:
                # Single equipment - apply filter
                identifier = found_identifiers[0]
                df_filtered = df_filtered[
                    df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                ]
                filters['identifier'] = identifier
                filter_descriptions.append(f"equipment {identifier}")
            else:
                # Multiple equipment mentioned but no comparison keyword - filter to first one
                identifier = found_identifiers[0]
                df_filtered = df_filtered[
                    df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                ]
                filters['identifier'] = identifier
                filter_descriptions.append(f"equipment {identifier}")
    
    # Value filters (category, area, status, severity)
    # FIXED: Jangan auto-detect area "OM" dari kata-kata umum!
    for role, col_name in key_columns.items():
        if not col_name or col_name not in df.columns:
            continue
        
        if role in ['category', 'area', 'status', 'severity'] and role not in filters:
            unique_values = df[col_name].dropna().unique()
            
            for value in unique_values:
                value_str = str(value).lower()
                words = value_str.split()
                
                # CRITICAL FIX: Jangan match kata pendek atau umum!
                # Skip jika value terlalu pendek (< 4 karakter) kecuali exact match
                if len(value_str) < 4:
                    # Only exact match untuk value pendek
                    if value_str == q or f" {value_str} " in f" {q} ":
                        df_filtered = df_filtered[df_filtered[col_name] == value]
                        filters[col_name] = value
                        filter_descriptions.append(f"{role}: {value}")
                        break
                else:
                    # Untuk value panjang (>= 4 chars), check word match
                    # Tapi pastikan bukan substring dari kata lain!
                    matched = False
                    if value_str in q:
                        # Check if it's a complete word, not part of another word
                        pattern = r'\b' + re.escape(value_str) + r'\b'
                        if re.search(pattern, q):
                            matched = True
                    elif any(word in q for word in words if len(word) > 3):
                        # Check word boundaries for multi-word values
                        for word in words:
                            if len(word) > 3:
                                pattern = r'\b' + re.escape(word) + r'\b'
                                if re.search(pattern, q):
                                    matched = True
                                    break
                    
                    if matched:
                        df_filtered = df_filtered[df_filtered[col_name] == value]
                        filters[col_name] = value
                        filter_descriptions.append(f"{role}: {value}")
                        break
    
    return df_filtered, filters, filter_descriptions

# ====================================================
# ENHANCED COMPARISON WITH FULL HIERARCHY
# ====================================================
def handle_comparison_query(query, df, key_columns):
    """Handle ALL comparison types - UNIVERSAL HANDLER"""
    
    q = query.lower()
    lang = detect_language(query)
    
    comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'comparison']
    if not any(kw in q for kw in comparison_keywords):
        return None
    
    print("  🔄 UNIVERSAL COMPARISON MODE")
    
    date_col = key_columns.get('date')
    identifier_col = key_columns.get('identifier')
    category_col = key_columns.get('category')
    area_col = key_columns.get('area')
    
    # ========================================
    # STEP 1: DETECT WHAT TO COMPARE
    # ========================================
    
    # Extract equipment codes
    identifier_patterns = [r'\b([A-Z]{2,}-\d+[A-Z]*)\b', r'\b([A-Z]{2,}\d+[A-Z]*)\b']
    found_equipments = []
    for pattern in identifier_patterns:
        found_equipments.extend(re.findall(pattern, query.upper()))
    found_equipments = list(dict.fromkeys(found_equipments))
    
    # Extract months
    months_map = {
        'january': 1, 'januari': 1, 'february': 2, 'februari': 2,
        'march': 3, 'maret': 3, 'april': 4, 'may': 5, 'mei': 5,
        'june': 6, 'juni': 6, 'july': 7, 'juli': 7,
        'august': 8, 'agustus': 8, 'ags': 8, 'aug': 8,
        'september': 9, 'sept': 9, 'sep': 9,
        'october': 10, 'oktober': 10, 'november': 11, 'december': 12, 'desember': 12
    }
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 
                   5: 'May', 6: 'June', 7: 'July', 8: 'August',
                   9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    
    # Detect date range FIRST (08-17 september)
    date_with_month_pattern = r'(\d{1,2})\s*[-–to/sd]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
    date_range_match = re.search(date_with_month_pattern, q, re.IGNORECASE)
    
    date_range = None
    found_months = []
    
    if date_range_match:
        start_day = int(date_range_match.group(1))
        end_day = int(date_range_match.group(2))
        month_str = date_range_match.group(3).lower()
        month_num = months_map.get(month_str)
        if month_num:
            date_range = (start_day, end_day)
            found_months = [month_num]
    else:
        for month_str, month_num in months_map.items():
            if month_str in q and len(month_str) > 3:
                found_months.append(month_num)
        found_months = list(set(found_months))
    
    # Extract areas
    found_areas = []
    if area_col and area_col in df.columns:
        unique_areas = df[area_col].dropna().unique()
        for area in unique_areas:
            area_str = str(area).lower()
            if len(area_str) >= 4 and area_str in q:
                pattern = r'\b' + re.escape(area_str) + r'\b'
                if re.search(pattern, q):
                    found_areas.append(area)
    
    # Extract categories
    found_categories = []
    if category_col and category_col in df.columns:
        unique_cats = df[category_col].dropna().unique()
        for cat in unique_cats:
            cat_str = str(cat).lower()
            if len(cat_str) >= 4 and cat_str in q:
                pattern = r'\b' + re.escape(cat_str) + r'\b'
                if re.search(pattern, q):
                    found_categories.append(cat)
    
    # Detect "top N"
    top_pattern = r'top\s*(\d+)'
    top_match = re.search(top_pattern, q, re.IGNORECASE)
    top_n = int(top_match.group(1)) if top_match else None
    
    print(f"  📊 Detected:")
    print(f"    - Equipments: {found_equipments}")
    print(f"    - Months: {found_months}")
    print(f"    - Date range: {date_range}")
    print(f"    - Areas: {found_areas}")
    print(f"    - Categories: {found_categories}")
    print(f"    - Top N: {top_n}")
    
    # ========================================
    # STEP 2: DETERMINE COMPARISON TYPE
    # ========================================
    
    comparison_entities = []
    comparison_type = None
    
    if top_n and identifier_col:
        top_equipments = df[identifier_col].value_counts().head(top_n).index.tolist()
        comparison_entities = top_equipments
        comparison_type = 'equipment'
        print(f"  🎯 Mode: Top {top_n} equipment")
    elif len(found_equipments) >= 2:
        comparison_entities = found_equipments
        comparison_type = 'equipment'
        print(f"  🎯 Mode: Equipment comparison")
    elif len(found_areas) >= 2:
        comparison_entities = found_areas
        comparison_type = 'area'
        print(f"  🎯 Mode: Area comparison")
    elif len(found_categories) >= 2:
        comparison_entities = found_categories
        comparison_type = 'category'
        print(f"  🎯 Mode: Category comparison")
    elif len(found_months) >= 2:
        comparison_entities = found_months
        comparison_type = 'month'
        if len(found_equipments) == 1:
            comparison_type = 'equipment_month'
            comparison_entities = {'equipment': found_equipments[0], 'months': found_months}
        print(f"  🎯 Mode: Month comparison")
    elif len(found_equipments) == 1 and len(found_months) >= 2:
        comparison_type = 'equipment_month'
        comparison_entities = {'equipment': found_equipments[0], 'months': found_months}
        print(f"  🎯 Mode: Single equipment, multi-month")
    else:
        print(f"  ⚠️ Could not determine comparison type")
        return None
    
    # ========================================
    # STEP 3: BUILD RESULTS
    # ========================================
    
    results = {}
    
    if comparison_type == 'equipment':
        for eq in comparison_entities:
            df_eq = df[df[identifier_col].astype(str).str.contains(eq, case=False, na=False)]
            
            if date_col and date_col in df_eq.columns:
                if found_months:
                    df_eq = df_eq[df_eq[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_eq = df_eq[(df_eq[date_col].dt.day >= date_range[0]) & (df_eq[date_col].dt.day <= date_range[1])]
            
            results[eq] = {
                "count": len(df_eq),
                "breakdown": build_detailed_breakdown(df_eq, key_columns, show_all=True)
            }
    
    elif comparison_type == 'area':
        for area in comparison_entities:
            df_area = df[df[area_col] == area]
            
            if date_col and date_col in df_area.columns:
                if found_months:
                    df_area = df_area[df_area[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_area = df_area[(df_area[date_col].dt.day >= date_range[0]) & (df_area[date_col].dt.day <= date_range[1])]
            
            results[area] = {
                "count": len(df_area),
                "breakdown": build_detailed_breakdown(df_area, key_columns, show_all=True)
            }
    
    elif comparison_type == 'category':
        for cat in comparison_entities:
            df_cat = df[df[category_col] == cat]
            
            if date_col and date_col in df_cat.columns:
                if found_months:
                    df_cat = df_cat[df_cat[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_cat = df_cat[(df_cat[date_col].dt.day >= date_range[0]) & (df_cat[date_col].dt.day <= date_range[1])]
            
            results[cat] = {
                "count": len(df_cat),
                "breakdown": build_detailed_breakdown(df_cat, key_columns, show_all=True)
            }
    
    elif comparison_type == 'month':
        for month in comparison_entities:
            df_month = df[df[date_col].dt.month == month]
            results[month_names[month]] = {
                "count": len(df_month),
                "breakdown": build_detailed_breakdown(df_month, key_columns, show_all=True)
            }
    
    elif comparison_type == 'equipment_month':
        equipment = comparison_entities['equipment']
        months = comparison_entities['months']
        
        for month in months:
            df_eq_month = df[
                (df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)) &
                (df[date_col].dt.month == month)
            ]
            
            if date_range:
                df_eq_month = df_eq_month[(df_eq_month[date_col].dt.day >= date_range[0]) & (df_eq_month[date_col].dt.day <= date_range[1])]
            
            results[f"{equipment} - {month_names[month]}"] = {
                "count": len(df_eq_month),
                "breakdown": build_detailed_breakdown(df_eq_month, key_columns, show_all=True)
            }
    
    # ========================================
    # STEP 4: BUILD TITLE
    # ========================================
    
    title_parts = []
    
    if comparison_type == 'equipment':
        title_parts.append(t('equipment', lang) + " " + t('comparison', lang))
    elif comparison_type == 'area':
        title_parts.append(f"{'Area' if lang == 'en' else 'Area'} {t('comparison', lang)}")
    elif comparison_type == 'category':
        title_parts.append(f"{'Category' if lang == 'en' else 'Kategori'} {t('comparison', lang)}")
    elif comparison_type in ['month', 'equipment_month']:
        title_parts.append(t('comparison', lang))
    
    time_info = []
    if found_months and comparison_type != 'month':
        month_strs = [month_names[m] for m in found_months]
        time_info.append(', '.join(month_strs))
    
    if date_range:
        time_info.append(f"date {date_range[0]}-{date_range[1]}")
    
    if time_info:
        title_parts.append(f"({', '.join(time_info)})")
    
    title = " ".join(title_parts)
    
    return build_enhanced_comparison_report(results, title, lang)

def build_detailed_breakdown(df_filtered, key_columns, show_all=False):
    """Build 3-level hierarchy breakdown"""
    
    breakdown = {}
    limit = None if show_all else 10
    
    # Level 1: Equipment
    if key_columns.get('identifier'):
        col = key_columns['identifier']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['equipment'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Level 2: Equipment Name
    if key_columns.get('equipment_name'):
        col = key_columns['equipment_name']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['equipment_name'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Level 3: PI Tag
    if key_columns.get('pi_tag'):
        col = key_columns['pi_tag']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['pi_tag'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Other dimensions
    for role in ['category', 'area', 'status', 'severity', 'limit_type']:
        col = key_columns.get(role)
        if col and col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown[role] = {str(k): int(v) for k, v in counts.head(5).items()}
    
    return breakdown


def build_enhanced_comparison_report(results, title, lang='en'):
    """Build ENHANCED comparison with full insights"""
    
    report = f"# 📊 {title}\n\n"
    
    # Executive Summary
    total_all = sum(r['count'] for r in results.values())
    report += f"## 🎯 {t('executive_summary', lang)}\n\n"
    report += f"**{t('total_events', lang)}:** {total_all:,}\n"
    report += f"**Entities Compared:** {len(results)}\n\n"
    
    # Summary Table
    report += f"## 📈 {t('comparison', lang)} Summary\n\n"
    report += f"| Entity | {t('total_events', lang)} | Percentage | Status |\n"
    report += "|--------|--------|------------|--------|\n"
    
    sorted_results = sorted(results.items(), key=lambda x: x[1]['count'], reverse=True)
    max_count = max(r['count'] for r in results.values())
    min_count = min(r['count'] for r in results.values())
    
    for entity, data in sorted_results:
        count = data['count']
        pct = (count / total_all * 100) if total_all > 0 else 0
        
        if count == max_count:
            status = f"🔴 {t('highest', lang)}"
        elif count == min_count:
            status = f"🟢 {t('lowest', lang)}"
        else:
            status = f"⚪ {t('normal', lang)}"
        
        report += f"| **{entity}** | {count:,} | {pct:.1f}% | {status} |\n"
    
    report += "\n"
    
    # Detailed Analysis with FULL HIERARCHY
    report += f"## 🔍 {t('detailed_breakdown', lang)}\n\n"
    
    for entity, data in results.items():
        report += f"### {entity}\n\n"
        report += f"**{t('total_events', lang)}:** {data['count']:,}\n\n"
        
        breakdown = data.get('breakdown', {})
        
        # 3-Level Hierarchy Display
        if 'equipment' in breakdown:
            report += f"**🏭 {t('top_equipment', lang)}:**\n"
            for i, (eq, count) in enumerate(breakdown['equipment'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. {eq}: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        if 'equipment_name' in breakdown:
            report += f"**🔧 {t('specific_equipment', lang)}:**\n"
            for i, (name, count) in enumerate(breakdown['equipment_name'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. {name}: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        if 'pi_tag' in breakdown:
            report += f"**📡 {t('pi_sensors', lang)}:**\n"
            for i, (tag, count) in enumerate(breakdown['pi_tag'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. `{tag}`: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        # Other breakdowns
        for key in ['category', 'limit_type', 'area', 'status', 'severity']:
            if key in breakdown and breakdown[key]:
                report += f"**{key.replace('_', ' ').title()}:**\n"
                for val, count in breakdown[key].items():
                    pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                    report += f"  • {val}: {count:,} ({pct:.1f}%)\n"
                report += "\n"
    
    # Key Insights
    report += f"## 💡 {t('insights', lang)}\n\n"
    
    highest = sorted_results[0]
    lowest = sorted_results[-1]
    
    diff = highest[1]['count'] - lowest[1]['count']
    if lowest[1]['count'] > 0:
        diff_pct = (diff / lowest[1]['count'] * 100)
        report += f"1. **{highest[0]}** has **{diff_pct:.0f}% more events** than {lowest[0]}\n"
    else:
        report += f"1. **{highest[0]}** has **{diff:,} more events** than {lowest[0]}\n"
    
    report += f"2. **Total difference:** {diff:,} events\n"
    
    if highest[1]['count'] / total_all > 0.6:
        report += f"3. **⚠️ High concentration:** {highest[0]} accounts for {highest[1]['count']/total_all*100:.1f}% of total\n"
    
    report += f"\n**📌 {t('recommendation', lang)}:** Focus on high-event entities to reduce incident rate.\n"
    
    return report

# ====================================================
# UNIFIED XY GRAPH GENERATOR
# ====================================================
def generate_xy_graph_data(df_filtered, date_col, title="Event Timeline"):
    """Generate XY graph data for ANY filtered dataset"""
    
    if not date_col or date_col not in df_filtered.columns:
        return None
    
    df_valid = df_filtered[df_filtered[date_col].notna()].copy()
    
    if len(df_valid) == 0:
        return None
    
    # Group by date
    df_valid['date_only'] = df_valid[date_col].dt.date
    daily_counts = df_valid.groupby('date_only').size().reset_index(name='count')
    daily_counts['date_only'] = daily_counts['date_only'].astype(str)
    
    # Calculate stats
    total = int(daily_counts['count'].sum())
    avg = round(daily_counts['count'].mean(), 2)
    max_val = int(daily_counts['count'].max())
    min_val = int(daily_counts['count'].min())
    
    # Determine trend
    if len(daily_counts) >= 3:
        counts = daily_counts['count'].values
        first_half_avg = counts[:len(counts)//2].mean()
        second_half_avg = counts[len(counts)//2:].mean()
        
        if second_half_avg > first_half_avg * 1.1:
            trend = "Increase"
        elif second_half_avg < first_half_avg * 0.9:
            trend = "Decrease"
        else:
            trend = "Stable"
    else:
        trend = "Limited data"
    
    return {
        "type": "xy_line",
        "title": title,
        "x_axis": "Tanggal",
        "y_axis": "Jumlah Events",
        "data": {
            "dates": daily_counts['date_only'].tolist(),
            "counts": daily_counts['count'].tolist()
        },
        "stats": {
            "total": total,
            "avg": avg,
            "max": max_val,
            "min": min_val,
            "trend": trend,
            "date_range": {
                "start": daily_counts['date_only'].iloc[0],
                "end": daily_counts['date_only'].iloc[-1]
            }
        }
    }

def should_generate_graph(query):
    """Check if query asks for graph"""
    graph_keywords = ['trend', 'grafik', 'chart', 'graph', 'pola', 'pattern', 'distribusi waktu', 'timeline']
    return any(kw in query.lower() for kw in graph_keywords)

def generate_comparison_graph_data(df, key_columns, equipments, filters=None):
    """Generate multi-dataset graph for equipment comparison"""
    
    date_col = key_columns.get('date')
    identifier_col = key_columns.get('identifier')
    
    if not date_col or not identifier_col:
        print("  ⚠️ Missing date or identifier column")
        return None
    
    print(f"  🎨 Generating comparison graph for: {equipments}")
    print(f"  📋 Filters: {filters}")
    
    datasets = []
    all_dates_set = set()
    
    # Collect all dates
    for equipment in equipments:
        df_eq = df[df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)]
        
        # Apply filters
        if filters:
            if filters.get('months') and date_col in df_eq.columns:
                df_eq = df_eq[df_eq[date_col].dt.month.isin(filters['months'])]
                print(f"    ✓ Applied month filter: {filters['months']}")
            
            if filters.get('date_range') and date_col in df_eq.columns:
                start_day, end_day = filters['date_range']
                df_eq = df_eq[(df_eq[date_col].dt.day >= start_day) & (df_eq[date_col].dt.day <= end_day)]
                print(f"    ✓ Applied date range: {start_day}-{end_day}")
            
            if filters.get('date_range_full') and date_col in df_eq.columns:
                start_date, end_date = filters['date_range_full']
                df_eq = df_eq[
                    (df_eq[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                    (df_eq[date_col].dt.date <= pd.to_datetime(end_date).date())
                ]
                print(f"    ✓ Applied full date range: {start_date} to {end_date}")
        
        df_valid = df_eq[df_eq[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            all_dates_set.update(df_valid['date_only'].unique())
    
    all_dates = sorted(list(all_dates_set))
    
    if len(all_dates) == 0:
        print("  ⚠️ No dates found after filtering")
        return None
    
    print(f"  📅 Date range: {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} days)")
    
    # Generate dataset for each equipment
    for equipment in equipments:
        df_eq = df[df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)]
        
        # Apply same filters
        if filters:
            if filters.get('months') and date_col in df_eq.columns:
                df_eq = df_eq[df_eq[date_col].dt.month.isin(filters['months'])]
            if filters.get('date_range') and date_col in df_eq.columns:
                start_day, end_day = filters['date_range']
                df_eq = df_eq[(df_eq[date_col].dt.day >= start_day) & (df_eq[date_col].dt.day <= end_day)]
            if filters.get('date_range_full') and date_col in df_eq.columns:
                start_date, end_date = filters['date_range_full']
                df_eq = df_eq[
                    (df_eq[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                    (df_eq[date_col].dt.date <= pd.to_datetime(end_date).date())
                ]
        
        df_valid = df_eq[df_eq[date_col].notna()].copy()
        
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily_counts = df_valid.groupby('date_only').size()
            
            # Fill missing dates with 0
            counts = [int(daily_counts.get(date, 0)) for date in all_dates]
            
            datasets.append({
                "label": equipment,
                "dates": [str(d) for d in all_dates],
                "counts": counts,
                "total": int(daily_counts.sum())
            })
            
            print(f"    ✅ {equipment}: {daily_counts.sum()} total events")
    
    if len(datasets) == 0:
        print("  ⚠️ No datasets generated")
        return None
    
    # Build title
    title_parts = ["Comparison"]
    if filters:
        if filters.get('months'):
            month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                          7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            months_str = ', '.join([month_names.get(m, str(m)) for m in filters['months']])
            title_parts.append(months_str)
        
        if filters.get('date_range'):
            start_day, end_day = filters['date_range']
            title_parts.append(f"({start_day}-{end_day})")
    
    final_title = f"{' '.join(title_parts)}: {' vs '.join(equipments)}"
    print(f"  📊 Graph title: {final_title}")
    
    return {
        "type": "comparison",
        "title": final_title,
        "datasets": datasets
    }

def generate_area_comparison_graph(df, key_columns, areas, filters=None):
    """Generate multi-dataset graph for area comparison"""
    
    date_col = key_columns.get('date')
    area_col = key_columns.get('area')
    
    if not date_col or not area_col:
        return None
    
    datasets = []
    all_dates_set = set()
    
    for area in areas:
        df_area = df[df[area_col] == area]
        
        # Apply filters
        if filters:
            if filters.get('months') and date_col in df_area.columns:
                df_area = df_area[df_area[date_col].dt.month.isin(filters['months'])]
            
            if filters.get('date_range') and date_col in df_area.columns:
                start_day, end_day = filters['date_range']
                df_area = df_area[(df_area[date_col].dt.day >= start_day) & (df_area[date_col].dt.day <= end_day)]
        
        df_valid = df_area[df_area[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            all_dates_set.update(df_valid['date_only'].unique())
    
    all_dates = sorted(list(all_dates_set))
    if len(all_dates) == 0:
        return None
    
    for area in areas:
        df_area = df[df[area_col] == area]
        
        if filters:
            if filters.get('months') and date_col in df_area.columns:
                df_area = df_area[df_area[date_col].dt.month.isin(filters['months'])]
            if filters.get('date_range') and date_col in df_area.columns:
                start_day, end_day = filters['date_range']
                df_area = df_area[(df_area[date_col].dt.day >= start_day) & (df_area[date_col].dt.day <= end_day)]
        
        df_valid = df_area[df_area[date_col].notna()].copy()
        
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily_counts = df_valid.groupby('date_only').size()
            counts = [int(daily_counts.get(date, 0)) for date in all_dates]
            
            datasets.append({
                "label": area,
                "dates": [str(d) for d in all_dates],
                "counts": counts,
                "total": int(daily_counts.sum())
            })
    
    if len(datasets) == 0:
        return None
    
    title_parts = ["Area Comparison"]
    if filters:
        if filters.get('months'):
            month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                          7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            months_str = ', '.join([month_names.get(m, str(m)) for m in filters['months']])
            title_parts.append(months_str)
        
        if filters.get('date_range'):
            start_day, end_day = filters['date_range']
            title_parts.append(f"({start_day}-{end_day})")
    
    return {
        "type": "comparison",
        "title": f"{' '.join(title_parts)}: {' vs '.join(areas)}",
        "datasets": datasets
    }

# ====================================================
# ENHANCED CONTEXT BUILDER
# ====================================================
def build_crystal_clear_context(query, df_filtered, filters, filter_descriptions):
    """Build ultra-clear context"""
    
    context_parts = []
    
    # CRITICAL: Event counting concept
    context_parts.append("🔴 INTERNAL RULE - JANGAN DIJELASKAN KE USER:")
    context_parts.append("• 1 row = 1 event")
    context_parts.append("• Hitung dari jumlah rows")
    context_parts.append("• User TIDAK perlu tahu detail ini")
    context_parts.append("")
    
    # Dataset overview
    context_parts.append("📊 DATASET INFO:")
    context_parts.append(f"Total events: **{len(df):,} events**")
    
    # Column info
    col_info = []
    if KEY_COLUMNS['identifier']:
        col_info.append(f"• `{KEY_COLUMNS['identifier']}`: Equipment/Asset")
    if KEY_COLUMNS['category']:
        col_info.append(f"• `{KEY_COLUMNS['category']}`: Kategori")
    if KEY_COLUMNS['area']:
        col_info.append(f"• `{KEY_COLUMNS['area']}`: Area")
    
    context_parts.append("\n".join(col_info))
    
    # Date range
    if 'date_range' in PROFILE:
        dr = PROFILE['date_range']
        context_parts.append(f"\n📅 Periode: {dr['start']} - {dr['end']}")
    
    # Filter results
    context_parts.append(f"\n🔍 HASIL FILTER:")
    if filter_descriptions:
        context_parts.append(f"Filter: {', '.join(filter_descriptions)}")
    else:
        context_parts.append("Tidak ada filter (all data)")
    
    context_parts.append(f"\n🎯 **MATCH: {len(df_filtered):,} events**")
    
    if len(df_filtered) > 0:
        # Equipment list (PENTING untuk query "event apa saja")
        context_parts.append("\n📊 BREAKDOWN:")
        
        identifier_col = KEY_COLUMNS.get('identifier')
        if identifier_col and identifier_col in df_filtered.columns:
            equipment_counts = df_filtered[identifier_col].value_counts()
            context_parts.append(f"\n**Equipment/Asset ({len(equipment_counts)} unique):**")
            for i, (eq, count) in enumerate(equipment_counts.head(20).items(), 1):
                pct = (count / len(df_filtered) * 100)
                context_parts.append(f"  {i}. {eq}: {count:,} events ({pct:.1f}%)")
            
            if len(equipment_counts) > 20:
                context_parts.append(f"  ... dan {len(equipment_counts) - 20} equipment lainnya")
        
        # Other breakdowns
        for role in ['category', 'area', 'status']:
            col = KEY_COLUMNS.get(role)
            if col and col in df_filtered.columns:
                counts = df_filtered[col].value_counts()
                if len(counts) > 0:
                    context_parts.append(f"\n**{col}:**")
                    for i, (val, count) in enumerate(counts.head(10).items(), 1):
                        pct = (count / len(df_filtered) * 100)
                        context_parts.append(f"  {i}. {val}: {count:,} events ({pct:.1f}%)")
        
        # Time distribution
        date_col = KEY_COLUMNS.get('date')
        if date_col and date_col in df_filtered.columns:
            df_valid = df_filtered[df_filtered[date_col].notna()].copy()
            if len(df_valid) > 0:
                df_valid['date_only'] = df_valid[date_col].dt.date
                daily = df_valid.groupby('date_only').size()
                
                context_parts.append(f"\n📊 **Distribusi Waktu:**")
                context_parts.append(f"  • Rata-rata: {daily.mean():.1f} events/hari")
                context_parts.append(f"  • Range: {daily.min()}-{daily.max()} events/hari")
    
    return "\n".join(context_parts)

# ====================================================
# LLM CALLER
# ====================================================
def call_llm(prompt, temperature=0.1):
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL, 
                "prompt": prompt, 
                "stream": True,
                "temperature": temperature
            },
            stream=True,
            timeout=120
        )
        
        text = ""
        for line in resp.iter_lines():
            if line:
                try:
                    data = json.loads(line)
                    if "response" in data:
                        text += data["response"]
                except:
                    pass
        
        return text.strip()
    except Exception as e:
        raise Exception(f"LLM Error: {e}")

# ====================================================
# ENHANCED ANOMALY DETECTION & ANALYSIS
# ====================================================
def detect_anomalies_enhanced(df_filtered, date_col, equipment_name=None, lang='en'):
    """Enhanced anomaly detection with detailed analysis - BILINGUAL"""
    
    if not date_col or date_col not in df_filtered.columns:
        return None
    
    df_valid = df_filtered[df_filtered[date_col].notna()].copy()
    if len(df_valid) < 7:  # Need minimum 7 days of data
        return {
            'detected': False,
            'reason': 'insufficient_data',
            'message': 'Need at least 7 days of data for anomaly analysis' if lang == 'en' else 'Membutuhkan minimal 7 hari data untuk analisis anomali'
        }
    
    df_valid['date_only'] = df_valid[date_col].dt.date
    df_valid['hour'] = df_valid[date_col].dt.hour
    df_valid['day_of_week'] = df_valid[date_col].dt.dayofweek
    
    daily_counts = df_valid.groupby('date_only').size()
    
    # Statistical parameters
    mean = daily_counts.mean()
    std = daily_counts.std()
    median = daily_counts.median()
    q1 = daily_counts.quantile(0.25)
    q3 = daily_counts.quantile(0.75)
    iqr = q3 - q1
    
    # Multiple anomaly detection methods
    
    # Method 1: Z-score (2.5 sigma)
    zscore_threshold = mean + (2.5 * std)
    zscore_anomalies = daily_counts[daily_counts > zscore_threshold]
    
    # Method 2: IQR (Interquartile Range)
    iqr_upper = q3 + (1.5 * iqr)
    iqr_anomalies = daily_counts[daily_counts > iqr_upper]
    
    # Method 3: Percentage spike (3x median)
    spike_threshold = median * 3
    spike_anomalies = daily_counts[daily_counts > spike_threshold]
    
    # Combine all methods
    all_anomaly_dates = set()
    all_anomaly_dates.update(zscore_anomalies.index)
    all_anomaly_dates.update(iqr_anomalies.index)
    all_anomaly_dates.update(spike_anomalies.index)
    
    if len(all_anomaly_dates) == 0:
        return {
            'detected': False,
            'reason': 'no_anomalies',
            'message': 'No anomalies detected in the data' if lang == 'en' else 'Tidak ada anomali terdeteksi dalam data',
            'statistics': {
                'mean': round(mean, 2),
                'median': round(median, 2),
                'std': round(std, 2),
                'min': int(daily_counts.min()),
                'max': int(daily_counts.max())
            }
        }
    
    # Detailed anomaly analysis
    anomaly_details = []
    for date in sorted(all_anomaly_dates):
        count = daily_counts[date]
        df_anomaly_day = df_valid[df_valid['date_only'] == date]
        
        # Analyze what caused the anomaly
        hourly_dist = df_anomaly_day['hour'].value_counts()
        peak_hour = hourly_dist.idxmax() if len(hourly_dist) > 0 else None
        
        # Calculate severity
        zscore = (count - mean) / std if std > 0 else 0
        if zscore > 3:
            severity = 'critical'
        elif zscore > 2.5:
            severity = 'high'
        elif zscore > 2:
            severity = 'medium'
        else:
            severity = 'low'
        
        anomaly_details.append({
            'date': str(date),
            'count': int(count),
            'expected_range': f"{int(mean - std)}-{int(mean + std)}",
            'deviation': round(((count - mean) / mean * 100), 1),
            'zscore': round(zscore, 2),
            'severity': severity,
            'peak_hour': int(peak_hour) if peak_hour is not None else None,
            'day_of_week': df_anomaly_day['day_of_week'].iloc[0]
        })
    
    # Pattern analysis
    day_names_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_id = ['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu']
    day_names = day_names_en if lang == 'en' else day_names_id
    
    anomaly_days = [detail['day_of_week'] for detail in anomaly_details]
    most_common_day = max(set(anomaly_days), key=anomaly_days.count) if anomaly_days else None
    
    return {
        'detected': True,
        'count': len(anomaly_details),
        'anomalies': sorted(anomaly_details, key=lambda x: x['zscore'], reverse=True),
        'statistics': {
            'mean': round(mean, 2),
            'median': round(median, 2),
            'std': round(std, 2),
            'threshold_zscore': round(zscore_threshold, 2),
            'threshold_iqr': round(iqr_upper, 2),
            'min': int(daily_counts.min()),
            'max': int(daily_counts.max())
        },
        'patterns': {
            'most_common_day': day_names[most_common_day] if most_common_day is not None else None,
            'total_anomaly_events': sum([a['count'] for a in anomaly_details]),
            'avg_anomaly_size': round(sum([a['count'] for a in anomaly_details]) / len(anomaly_details), 2)
        }
    }

def build_anomaly_report(anomalies, equipment_name=None, filter_desc=None, lang='en'):
    """Build comprehensive anomaly report - BILINGUAL"""
    
    if not anomalies or not anomalies.get('detected'):
        title = f"{'Anomaly' if lang == 'en' else 'Anomali'} {equipment_name}" if equipment_name else t('anomaly_detected', lang).replace(' Detected', '').replace(' Terdeteksi', '')
        if filter_desc:
            title += f" ({filter_desc})"
        
        report = f"# 🔍 {title}\n\n"
        
        if anomalies and anomalies.get('reason') == 'insufficient_data':
            report += f"⚠️ **{anomalies['message']}**\n\n"
        else:
            report += f"✅ **{'No anomalies detected' if lang == 'en' else 'Tidak ada anomali terdeteksi'}**\n\n"
            if anomalies and 'statistics' in anomalies:
                stats = anomalies['statistics']
                if lang == 'en':
                    report += f"Data shows consistent patterns:\n"
                    report += f"• Average: {stats['mean']} events/day\n"
                    report += f"• Median: {stats['median']} events/day\n"
                    report += f"• Normal range: {stats['min']}-{stats['max']} events/day\n"
                else:
                    report += f"Data menunjukkan pola yang konsisten:\n"
                    report += f"• Rata-rata: {stats['mean']} events/hari\n"
                    report += f"• Median: {stats['median']} events/hari\n"
                    report += f"• Range normal: {stats['min']}-{stats['max']} events/hari\n"
        
        return report
    
    title = f"{'Anomalies Detected' if lang == 'en' else 'Anomali Terdeteksi'}"
    if equipment_name:
        title += f" - {equipment_name}"
    if filter_desc:
        title += f" ({filter_desc})"
    
    report = f"# ⚠️ {title}\n\n"
    
    stats = anomalies['statistics']
    patterns = anomalies['patterns']
    
    # Executive Summary
    report += f"## 📊 {t('executive_summary', lang)}\n\n"
    
    if lang == 'en':
        report += f"**Total anomalies detected:** {anomalies['count']} days\n\n"
        report += f"**Normal Statistics:**\n"
        report += f"• Daily average: {stats['mean']} events\n"
        report += f"• Median: {stats['median']} events\n"
        report += f"• Standard deviation: {stats['std']}\n"
        report += f"• Anomaly threshold: {stats['threshold_zscore']} events\n\n"
        report += f"**Total anomaly events:** {patterns['total_anomaly_events']} events ({patterns['avg_anomaly_size']} avg per day)\n"
        if patterns.get('most_common_day'):
            report += f"**Most common day:** {patterns['most_common_day']}\n"
    else:
        report += f"**Total anomali terdeteksi:** {anomalies['count']} hari\n\n"
        report += f"**Statistik Normal:**\n"
        report += f"• Rata-rata harian: {stats['mean']} events\n"
        report += f"• Median: {stats['median']} events\n"
        report += f"• Standar deviasi: {stats['std']}\n"
        report += f"• Threshold anomali: {stats['threshold_zscore']} events\n\n"
        report += f"**Total events anomali:** {patterns['total_anomaly_events']} events ({patterns['avg_anomaly_size']} avg per hari)\n"
        if patterns.get('most_common_day'):
            report += f"**Hari tersering:** {patterns['most_common_day']}\n"
    
    report += f"\n---\n\n"
    
    # Detailed Anomaly List
    report += f"## 🔴 {'Detailed Anomaly List' if lang == 'en' else 'Daftar Anomali Detail'}\n\n"
    
    # Group by severity
    critical = [a for a in anomalies['anomalies'] if a['severity'] == 'critical']
    high = [a for a in anomalies['anomalies'] if a['severity'] == 'high']
    medium = [a for a in anomalies['anomalies'] if a['severity'] == 'medium']
    low = [a for a in anomalies['anomalies'] if a['severity'] == 'low']
    
    day_names_en = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_names_id = ['Sen', 'Sel', 'Rab', 'Kam', 'Jum', 'Sab', 'Min']
    day_names = day_names_en if lang == 'en' else day_names_id
    
    if critical:
        report += f"### 🚨 Critical (Z-score > 3.0)\n\n"
        for a in critical:
            report += f"**{a['date']}** ({'Day' if lang == 'en' else 'Hari'}: {day_names[a['day_of_week']]})\n"
            if lang == 'en':
                report += f"• **{a['count']} events** (normal: {a['expected_range']})\n"
                report += f"• Deviation: **+{a['deviation']}%** from average\n"
                report += f"• Z-score: {a['zscore']}\n"
                if a['peak_hour'] is not None:
                    report += f"• Peak hour: {a['peak_hour']}:00\n"
            else:
                report += f"• **{a['count']} events** (normal: {a['expected_range']})\n"
                report += f"• Deviasi: **+{a['deviation']}%** dari rata-rata\n"
                report += f"• Z-score: {a['zscore']}\n"
                if a['peak_hour'] is not None:
                    report += f"• Peak hour: {a['peak_hour']}:00\n"
            report += f"\n"
    
    if high:
        report += f"### ⚠️ High (Z-score 2.5-3.0)\n\n"
        for a in high:
            report += f"**{a['date']}**: {a['count']} events (+{a['deviation']}%) | Peak: {a['peak_hour']}:00\n"
    
    if medium or low:
        report += f"\n### 📊 {'Medium/Low Anomalies' if lang == 'en' else 'Anomali Medium/Low'}\n\n"
        for a in (medium + low):
            report += f"• {a['date']}: {a['count']} events (+{a['deviation']}%)\n"
    
    # Insights & Recommendations
    report += f"\n---\n\n"
    report += f"## 💡 {t('insights', lang)} & {t('recommendation', lang)}\n\n"
    
    # Analyze patterns
    avg_deviation = sum([a['deviation'] for a in anomalies['anomalies']]) / len(anomalies['anomalies'])
    max_anomaly = max(anomalies['anomalies'], key=lambda x: x['count'])
    
    if lang == 'en':
        report += f"1. **Largest Spike:** {max_anomaly['date']} with {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
        report += f"2. **Average Deviation:** +{avg_deviation:.1f}% from normal\n"
        
        if len(critical) > 0:
            report += f"3. **Critical Alert:** {len(critical)} days with critical spikes (>3 sigma)\n"
        
        # Peak hour analysis
        peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
        if peak_hours:
            most_common_hour = max(set(peak_hours), key=peak_hours.count)
            report += f"4. **Most Common Hour:** Anomalies often occur around {most_common_hour}:00\n"
        
        report += f"\n**Recommendations:**\n"
        report += f"• Investigate root causes on critical dates\n"
        report += f"• Review maintenance schedule and operational patterns\n"
        if patterns.get('most_common_day'):
            report += f"• Pay special attention on {patterns['most_common_day']}\n"
        report += f"• Set up monitoring alerts to prevent similar anomalies\n"
    else:
        report += f"1. **Lonjakan Terbesar:** {max_anomaly['date']} dengan {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
        report += f"2. **Rata-rata Deviasi:** +{avg_deviation:.1f}% dari normal\n"
        
        if len(critical) > 0:
            report += f"3. **Critical Alert:** Terdapat {len(critical)} hari dengan lonjakan kritis (>3 sigma)\n"
        
        # Peak hour analysis
        peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
        if peak_hours:
            most_common_hour = max(set(peak_hours), key=peak_hours.count)
            report += f"4. **Jam Tersering:** Anomali sering terjadi sekitar jam {most_common_hour}:00\n"
        
        report += f"\n**Rekomendasi:**\n"
        report += f"• Investigasi penyebab lonjakan pada tanggal-tanggal critical\n"
        report += f"• Review maintenance schedule dan operational pattern\n"
        if patterns.get('most_common_day'):
            report += f"• Perhatikan khusus hari {patterns['most_common_day']}\n"
        report += f"• Set up monitoring alerts untuk mencegah anomali serupa\n"
    
    return report

# ====================================================
# ANOMALY QUERY HANDLER - FIXED
# ====================================================
def handle_anomaly_query(query, df, key_columns):
    """Dedicated handler for anomaly detection queries - BILINGUAL"""
    
    print("  🔍 ANOMALY DETECTION MODE")
    
    # Detect language
    lang = detect_language(query)
    
    # Extract filters
    df_filtered, filters, filter_descriptions = extract_filters_from_query(query, df, key_columns)
    
    print(f"  📊 Filtered data: {len(df_filtered)} events")
    print(f"  🔧 Filters applied: {filters}")
    print(f"  🌐 Language: {lang}")
    
    date_col = key_columns.get('date')
    if not date_col:
        msg = "❌ Cannot perform anomaly analysis: date column not found" if lang == 'en' else "❌ Tidak dapat melakukan analisis anomali: kolom tanggal tidak ditemukan"
        return msg, 0, 0, None
    
    # Identify equipment if specified
    equipment_name = None
    if filters.get('identifier'):
        equipment_name = filters['identifier']
    elif filters.get('identifiers'):
        equipment_name = ', '.join(filters['identifiers'])
    
    # Build filter description
    filter_desc = ', '.join(filter_descriptions) if filter_descriptions else None
    
    print(f"  🎯 Equipment: {equipment_name if equipment_name else 'ALL'}")
    print(f"  📝 Filter desc: {filter_desc}")
    
    # Perform anomaly detection
    anomalies = detect_anomalies_enhanced(df_filtered, date_col, equipment_name, lang)
    
    # Build report with language support
    report = build_anomaly_report(anomalies, equipment_name, filter_desc, lang)
    
    # Generate visualization
    graph_data = None
    if len(df_filtered) > 0:
        title_parts = [t('anomaly_detected', lang).replace(' Detected', '').replace(' Terdeteksi', '')]
        if equipment_name:
            title_parts.append(f"- {equipment_name}")
        if filter_desc:
            title_parts.append(f"({filter_desc})")
        
        title = " ".join(title_parts)
        
        # Use simple graph generation
        graph_data = generate_xy_graph_data(df_filtered, date_col, title)
        
        # Add anomaly markers ONLY if anomalies detected
        if graph_data and anomalies and anomalies.get('detected') and anomalies.get('anomalies'):
            anomaly_dates = [a['date'] for a in anomalies['anomalies']]
            graph_data['anomaly_dates'] = anomaly_dates
            graph_data['anomaly_info'] = {
                'count': len(anomaly_dates),
                'severity_distribution': {
                    'critical': len([a for a in anomalies['anomalies'] if a['severity'] == 'critical']),
                    'high': len([a for a in anomalies['anomalies'] if a['severity'] == 'high']),
                    'medium': len([a for a in anomalies['anomalies'] if a['severity'] == 'medium']),
                    'low': len([a for a in anomalies['anomalies'] if a['severity'] == 'low'])
                }
            }
            print(f"  ⚠️ Added {len(anomaly_dates)} anomaly markers to graph")
    
    # Calculate confidence
    confidence = 100 if anomalies and anomalies.get('detected') else 90
    if len(df_filtered) < 30:
        confidence -= 20  # Lower confidence with limited data
    
    print(f"  ✅ Anomaly analysis complete. Detected: {anomalies.get('detected', False)}")
    
    return report, len(df_filtered), confidence, graph_data
def detect_query_intent(query):
    """Detect specific intent"""
    
    q = query.lower()
    
    intents = {
        'pi_tag': ['pi tag', 'tag name', 'tagname', 'pi name', 'tag pi', 'nama tag'],
        'anomaly': ['anomali', 'anomaly', 'lonjakan', 'spike', 'abnormal', 'tidak normal', 'unusual', 'outlier'],
        'count': ['berapa', 'jumlah', 'total', 'count', 'ada berapa', 'hitung'],
        'comparison': ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan'],
        'trend': ['trend', 'pola', 'grafik', 'chart', 'pattern', 'graph'],
        'list': ['apa saja', 'list', 'daftar', 'show all', 'semua'],
        'top': ['paling', 'tertinggi', 'terbanyak', 'top', 'most'],
        'least': ['tersedikit', 'terendah', 'paling sedikit', 'least'],
        'average': ['rata-rata', 'average', 'rerata', 'mean', 'avg', ],
        'when': ['kapan', 'when', 'tanggal berapa', 'waktu', 'pada saat'],
        'where': ['dimana', 'where', 'lokasi', 'area mana'],
        'why': ['kenapa', 'mengapa', 'why', 'alasan', 'sebab', 'penyebab']
    }
    
    detected = []
    for intent, keywords in intents.items():
        if any(kw in q for kw in keywords):
            detected.append(intent)
    
    return detected if detected else ['general']

# ====================================================
# ENHANCED LLM ANSWER
# ====================================================
def get_enhanced_llm_prompt(query, context, lang='en'):
    """Generate bilingual LLM prompt - PROPERLY SEPARATED"""
    
    if lang == 'id':
        # Indonesian version
        return f"""Kamu adalah data analyst profesional yang menjawab dalam BAHASA INDONESIA.

{context}

📋 ATURAN KETAT:

1. **BAHASA:**
   - WAJIB gunakan Bahasa Indonesia 100%
   - JANGAN gunakan bahasa Inggris

2. **JAWABAN LENGKAP & INFORMATIF:**
   - Berikan analisis mendalam, bukan hanya angka mentah
   - Jelaskan pola, trend, dan anomali yang ditemukan
   - Identifikasi temuan penting (outlier, peak, pattern)
   - Sertakan hierarchy: Equipment → Equipment Name → PI Tag (jika ada)
   - Tampilkan SEMUA data jika diminta list/daftar lengkap

3. **FORMAT:**
   - Gunakan **bold** untuk angka dan temuan penting
   - Bullet points (•) untuk breakdown
   - Numbering (1, 2, 3) untuk ranking
   - Emoji untuk visual impact (📊 📈 ⚠️ ✅ 💡)
   - Section headers (##) untuk organize

4. **UNTUK QUERY "LIST/DAFTAR":**
   - Tampilkan SEMUA equipment/data, bukan hanya top 10
   - Format: "1. Item: count (percentage)"
   - Lengkap dengan hierarchy jika tersedia

5. **JANGAN:**
   - Jangan jelaskan "1 row = 1 event" ke user
   - Jangan jelaskan metodologi internal
   - Fokus pada hasil dan insight

PERTANYAAN: {query}

JAWABAN (Bahasa Indonesia, lengkap & informatif):"""
    
    else:  # English version
        return f"""You are a professional data analyst providing ENGLISH responses.

{context}

📋 STRICT RULES:

1. **LANGUAGE:**
   - MUST use English 100%
   - NO Indonesian words

2. **COMPLETE & INFORMATIVE ANSWERS:**
   - Provide deep analysis, not just raw numbers
   - Explain patterns, trends, and anomalies found
   - Identify important findings (outliers, peaks, patterns)
   - Include hierarchy: Equipment → Equipment Name → PI Tag (when available)
   - Show ALL data when asked for complete list

3. **FORMAT:**
   - Use **bold** for numbers and key findings
   - Bullet points (•) for breakdowns
   - Numbering (1, 2, 3) for rankings
   - Emoji for visual impact (📊 📈 ⚠️ ✅ 💡)
   - Section headers (##) for organization

4. **FOR "LIST" QUERIES:**
   - Show ALL equipment/data, not just top 10
   - Format: "1. Item: count (percentage)"
   - Include hierarchy when available

5. **DON'T:**
   - Don't explain "1 row = 1 event" to user
   - Don't explain internal methodology
   - Focus on results and insights

QUESTION: {query}

ANSWER (English, complete & informative):"""


def answer_with_enhanced_llm(query, df, key_columns):
    """Enhanced LLM with BILINGUAL support and full analysis"""
    
    # Detect language FIRST
    lang = detect_language(query)
    print(f"  🌐 Detected language: {lang}")
    
    # Detect intent
    query_intent = detect_query_intent(query)
    
    # Check cache first
    cache_key = get_cache_key(query, {})
    cached = get_from_cache(cache_key)
    if cached:
        print(f"  ⚡ CACHE HIT! Intent: {query_intent}, Lang: {lang}")
        return cached['answer'], cached['count'], cached['confidence'], cached['graph_data'], "cached"
    
    # Extract filters
    df_filtered, filters, filter_descriptions = extract_filters_from_query(query, df, key_columns)
    
    print(f"  📊 Filtered: {len(df_filtered)} events with filters: {filters}")
    
    # Check for comparison
    comparison_result = handle_comparison_query(query, df, key_columns)
    if comparison_result:
        confidence, reasons = calculate_confidence(df_filtered, filters, query, "comparison")
        result = {
            'answer': comparison_result,
            'count': len(df_filtered),
            'confidence': confidence,
            'graph_data': None
        }
        save_to_cache(cache_key, result)
        return comparison_result, len(df_filtered), confidence, None, "comparison"
    
    # Build context with hierarchy awareness
    context = build_crystal_clear_context(query, df_filtered, filters, filter_descriptions, lang)
    
    # Generate prompt using bilingual system
    prompt = get_enhanced_llm_prompt(query, context, lang)
    
    try:
        # Call LLM
        answer = call_llm(prompt, temperature=0.2)
        answer = answer.strip()
        
        # Clean up artifacts
        cleanup_phrases = {
            'en': ["Answer:", "ANSWER:", "Response:"],
            'id': ["Jawaban:", "JAWABAN:", "Respons:"]
        }
        
        for phrase in cleanup_phrases.get(lang, []):
            if answer.startswith(phrase):
                answer = answer[len(phrase):].strip()
        
        # Remove verbose explanations
        verbose_phrases = [
            "Since each row", "Karena setiap baris",
            "The answer is straightforward", "Jawabannya sederhana",
            "No need for further breakdown"
        ]
        for phrase in verbose_phrases:
            if phrase.lower() in answer.lower():
                lines = answer.split('\n')
                answer = '\n'.join([l for l in lines if not any(vp.lower() in l.lower() for vp in verbose_phrases)])
        
        # Generate graph if requested
        graph_data = None
        should_graph = should_generate_graph(query) or any(intent in ['trend', 'count', 'average'] for intent in query_intent)
        
        if should_graph and len(df_filtered) > 0:
            # Build title based on filters
            title_parts = ["Event Timeline"]
            if filters.get('identifier'):
                title_parts.append(f"- {filters['identifier']}")
            if filters.get('months'):
                month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 
                              6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 
                              11: 'Nov', 12: 'Dec'}
                months_str = ", ".join([month_names.get(m, str(m)) for m in filters['months']])
                title_parts.append(f"({months_str})")
            if filters.get('date_range'):
                title_parts.append(f"(Date {filters['date_range'][0]}-{filters['date_range'][1]})")
            if filters.get('week'):
                title_parts.append(f"(Week {filters['week']})")
            
            title = " ".join(title_parts)
            graph_data = generate_xy_graph_data(df_filtered, key_columns['date'], title)
            
            # Add graph summary to answer if not already mentioned
            if graph_data and 'graph' not in answer.lower() and 'visualization' not in answer.lower():
                graph_section = f"\n\n## 📈 {'Visualisasi Data' if lang == 'id' else 'Data Visualization'}\n\n"
                graph_section += f"**{graph_data['title']}**\n\n"
                graph_section += f"• {'Total' if lang == 'en' else 'Total'}: {graph_data['stats']['total']} events\n"
                graph_section += f"• {'Average' if lang == 'en' else 'Rata-rata'}: {graph_data['stats']['avg']} events/day\n"
                graph_section += f"• Range: {graph_data['stats']['min']}-{graph_data['stats']['max']} events/day\n"
                graph_section += f"• Trend: {graph_data['stats']['trend']}\n"
                answer += graph_section
        
        # Calculate confidence
        confidence, reasons = calculate_confidence(df_filtered, filters, query, "llm_analysis")
        
        # Cache result
        result = {
            'answer': answer,
            'count': len(df_filtered),
            'confidence': confidence,
            'graph_data': graph_data
        }
        save_to_cache(cache_key, result)
        
        print(f"  ✅ Analysis complete: {len(df_filtered)} events, {confidence:.1f}% confidence, Lang: {lang}")
        if graph_data:
            print(f"  📈 Graph generated: {graph_data['title']}")
        
        return answer, len(df_filtered), confidence, graph_data, "llm_analysis"
        
    except Exception as e:
        print(f"❌ LLM Error: {str(e)}")
        traceback.print_exc()
        error_msg = "❌ Error: " + str(e) if lang == 'en' else f"❌ Error: {str(e)}"
        return error_msg, 0, 0, None, "error"


def build_crystal_clear_context(query, df_filtered, filters, filter_descriptions, lang='en'):
    """Build context with language awareness"""
    
    context_parts = []
    
    # Internal rule (same for both languages)
    context_parts.append("🔴 INTERNAL RULE - DON'T EXPLAIN TO USER:")
    context_parts.append("• 1 row = 1 event (for your understanding only)")
    context_parts.append("• Count from number of rows")
    context_parts.append("• User doesn't need to know this detail")
    context_parts.append("")
    
    # Dataset overview
    context_parts.append("📊 DATASET INFO:")
    context_parts.append(f"Total events in system: **{len(df):,} events**")
    
    # Column info with hierarchy
    col_info = []
    if KEY_COLUMNS['identifier']:
        col_info.append(f"• Level 1 - `{KEY_COLUMNS['identifier']}`: Equipment code")
    if KEY_COLUMNS.get('equipment_name'):
        col_info.append(f"• Level 2 - `{KEY_COLUMNS['equipment_name']}`: Specific equipment name")
    if KEY_COLUMNS.get('pi_tag'):
        col_info.append(f"• Level 3 - `{KEY_COLUMNS['pi_tag']}`: PI sensor tags")
    if KEY_COLUMNS['category']:
        col_info.append(f"• `{KEY_COLUMNS['category']}`: Event category/type")
    if KEY_COLUMNS['area']:
        col_info.append(f"• `{KEY_COLUMNS['area']}`: Plant area")
    
    context_parts.append("\n".join(col_info))
    
    # Date range
    if 'date_range' in PROFILE:
        dr = PROFILE['date_range']
        context_parts.append(f"\n📅 Data period: {dr['start']} to {dr['end']}")
    
    # Filter results
    context_parts.append(f"\n🔍 FILTER RESULTS:")
    if filter_descriptions:
        context_parts.append(f"Applied filters: {', '.join(filter_descriptions)}")
    else:
        context_parts.append("No filters (all data)")
    
    context_parts.append(f"\n🎯 **MATCHED: {len(df_filtered):,} events**")
    
    if len(df_filtered) > 0:
        # SHOW ALL for list queries
        show_all = any(kw in query.lower() for kw in ['all', 'semua', 'complete', 'lengkap', 'seluruh'])
        limit = None if show_all else 20
        
        context_parts.append("\n📊 BREAKDOWN:")
        
        # Level 1: Equipment
        identifier_col = KEY_COLUMNS.get('identifier')
        if identifier_col and identifier_col in df_filtered.columns:
            equipment_counts = df_filtered[identifier_col].value_counts()
            context_parts.append(f"\n**Equipment Codes ({len(equipment_counts)} unique):**")
            
            items_to_show = equipment_counts.items() if show_all else equipment_counts.head(limit).items()
            for i, (eq, count) in enumerate(items_to_show, 1):
                pct = (count / len(df_filtered) * 100)
                context_parts.append(f"  {i}. {eq}: {count:,} events ({pct:.1f}%)")
            
            if not show_all and len(equipment_counts) > limit:
                context_parts.append(f"  ... and {len(equipment_counts) - limit} more")
        
        # Level 2: Equipment Names (if exists)
        eq_name_col = KEY_COLUMNS.get('equipment_name')
        if eq_name_col and eq_name_col in df_filtered.columns:
            name_counts = df_filtered[eq_name_col].value_counts()
            if len(name_counts) > 0:
                context_parts.append(f"\n**Specific Equipment Names ({len(name_counts)} unique):**")
                items_to_show = name_counts.items() if show_all else name_counts.head(limit).items()
                for i, (name, count) in enumerate(items_to_show, 1):
                    pct = (count / len(df_filtered) * 100)
                    context_parts.append(f"  {i}. {name}: {count:,} events ({pct:.1f}%)")
                if not show_all and len(name_counts) > limit:
                    context_parts.append(f"  ... and {len(name_counts) - limit} more")
        
        # Level 3: PI Tags (if exists)
        pi_tag_col = KEY_COLUMNS.get('pi_tag')
        if pi_tag_col and pi_tag_col in df_filtered.columns:
            tag_counts = df_filtered[pi_tag_col].value_counts()
            if len(tag_counts) > 0:
                context_parts.append(f"\n**PI Tag Sensors ({len(tag_counts)} unique):**")
                items_to_show = tag_counts.items() if show_all else tag_counts.head(limit).items()
                for i, (tag, count) in enumerate(items_to_show, 1):
                    pct = (count / len(df_filtered) * 100)
                    context_parts.append(f"  {i}. `{tag}`: {count:,} events ({pct:.1f}%)")
                if not show_all and len(tag_counts) > limit:
                    context_parts.append(f"  ... and {len(tag_counts) - limit} more")
        
        # Other breakdowns
        for role in ['category', 'area', 'status']:
            col = KEY_COLUMNS.get(role)
            if col and col in df_filtered.columns:
                counts = df_filtered[col].value_counts()
                if len(counts) > 0:
                    context_parts.append(f"\n**{col}:**")
                    for i, (val, count) in enumerate(counts.head(10).items(), 1):
                        pct = (count / len(df_filtered) * 100)
                        context_parts.append(f"  {i}. {val}: {count:,} events ({pct:.1f}%)")
        
        # Time distribution
        date_col = KEY_COLUMNS.get('date')
        if date_col and date_col in df_filtered.columns:
            df_valid = df_filtered[df_filtered[date_col].notna()].copy()
            if len(df_valid) > 0:
                df_valid['date_only'] = df_valid[date_col].dt.date
                daily = df_valid.groupby('date_only').size()
                
                context_parts.append(f"\n📊 **Time Distribution:**")
                context_parts.append(f"  • Average: {daily.mean():.1f} events/day")
                context_parts.append(f"  • Range: {daily.min()}-{daily.max()} events/day")
    
    return "\n".join(context_parts)

# ====================================================
# IDENTIFIER SEARCH
# ====================================================
def is_identifier_query(query):
    """Check if identifier search"""
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    return any(re.search(p, query.upper()) for p in patterns)

def search_by_identifier(query, df, key_columns):
    """Enhanced identifier search with all filters + LLM analysis + event details"""
    
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
        r'\b([A-Z]{2}-\d+)\b',
    ]
    
    identifier = None
    for pattern in patterns:
        matches = re.findall(pattern, query.upper())
        if matches:
            identifier = matches[0]
            break
    
    if not identifier:
        return None, None, 0, None
    
    # Detect language
    try:
        lang = detect_language(query)
    except:
        lang = 'en'
    
    # Search columns
    search_cols = []
    if key_columns.get('identifier'):
        search_cols.append(key_columns['identifier'])
    for col in ['Equipment Name', 'TagNamePI', 'Asset Name']:
        if col in df.columns:
            search_cols.append(col)
    
    mask = pd.Series([False] * len(df))
    found_in_cols = []
    
    for col in search_cols:
        if col in df.columns:
            col_match = df[col].astype(str).str.contains(identifier, case=False, na=False)
            if col_match.any():
                mask = mask | col_match
                found_in_cols.append(col)
    
    df_found = df[mask]
    
    # Apply ALL time filters
    q = query.lower()
    date_col = key_columns.get('date')
    
    if date_col and date_col in df_found.columns:
        # Month filter
        months_map = {
            'january': 1, 'januari': 1, 'february': 2, 'februari': 2,
            'march': 3, 'maret': 3, 'april': 4, 'may': 5, 'mei': 5,
            'june': 6, 'juni': 6, 'july': 7, 'juli': 7,
            'august': 8, 'agustus': 8, 'ags': 8, 'aug': 8,
            'september': 9, 'sept': 9, 'sep': 9,
            'october': 10, 'oktober': 10, 'november': 11, 'december': 12, 'desember': 12
        }
        
        # ENHANCED: Cross-month date range (28 august - 16 september)
        cross_month_pattern = r'(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)\s*[-–to/vs]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
        match = re.search(cross_month_pattern, q, re.IGNORECASE)
        
        applied_filter = False
        
        if match:
            start_day = int(match.group(1))
            start_month_str = match.group(2).lower()
            end_day = int(match.group(3))
            end_month_str = match.group(4).lower()
            
            start_month = months_map.get(start_month_str)
            end_month = months_map.get(end_month_str)
            
            if start_month and end_month:
                year = df_found[date_col].dt.year.mode()[0] if len(df_found) > 0 else datetime.now().year
                
                try:
                    from datetime import datetime as dt_class
                    start_date = dt_class(year, start_month, start_day).date()
                    end_date = dt_class(year, end_month, end_day).date()
                    
                    df_found = df_found[
                        (df_found[date_col].dt.date >= start_date) & 
                        (df_found[date_col].dt.date <= end_date)
                    ]
                    applied_filter = True
                    print(f"  ✓ Applied cross-month date range: {start_date} to {end_date}")
                except Exception as e:
                    print(f"  ⚠️ Error applying cross-month filter: {e}")
        
        # If no cross-month range, try single month filter
        if not applied_filter:
            for month_str, month_num in months_map.items():
                if month_str in q and len(month_str) > 3:
                    df_found = df_found[df_found[date_col].dt.month == month_num]
                    print(f"  ✓ Applied month filter: {month_str}")
                    applied_filter = True
                    break
        
        # Date range filter (same month)
        if not applied_filter:
            date_range_pattern = r'(date|tanggal)\s*(\d{1,2})\s*[-–to/sd]\s*(\d{1,2})'
            match = re.search(date_range_pattern, q)
            if match:
                start_day = int(match.group(2))
                end_day = int(match.group(3))
                df_found = df_found[(df_found[date_col].dt.day >= start_day) & (df_found[date_col].dt.day <= end_day)]
                print(f"  ✓ Applied date range: {start_day}-{end_day}")
                applied_filter = True
        
        # Week filter
        if not applied_filter:
            week_pattern = r'(week|minggu)\s*(\d{1,2})'
            match = re.search(week_pattern, q)
            if match:
                week_num = int(match.group(2))
                df_found = df_found[df_found[date_col].dt.isocalendar().week == week_num]
                print(f"  ✓ Applied week filter: week {week_num}")
                applied_filter = True
    
    if len(df_found) == 0:
        return None, f"❌ No data found for `{identifier}` with those filters", 0, None
    
    # Build COMPREHENSIVE report
    explanation = f"# 🔍 Search Results: `{identifier}`\n\n"
    explanation += f"**Found {len(df_found):,} events** in columns: {', '.join([f'`{c}`' for c in found_in_cols])}\n\n"
    
    # Equipment Hierarchy Information (ENHANCED!)
    explanation += f"## 🏗️ Equipment Hierarchy\n\n"
    
    # Level 1: Equipment
    if key_columns.get('identifier'):
        col = key_columns['identifier']
        unique_eq = df_found[col].dropna().unique()
        if len(unique_eq) <= 5:
            explanation += f"**Equipment Codes:** {', '.join([f'`{e}`' for e in unique_eq])}\n"
    
    # Level 2: Equipment Names (if column exists)
    if key_columns.get('equipment_name'):
        col = key_columns['equipment_name']
        if col in df_found.columns:
            unique_names = df_found[col].dropna().unique()
            if len(unique_names) > 0:
                explanation += f"\n**Specific Equipment Names ({len(unique_names)}):**\n"
                for name in list(unique_names)[:10]:
                    count = len(df_found[df_found[col] == name])
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {name}: {count} events ({pct:.1f}%)\n"
                if len(unique_names) > 10:
                    explanation += f"  • ... and {len(unique_names) - 10} more\n"
    
    # Level 3: PI Tags (if column exists)
    if key_columns.get('pi_tag'):
        col = key_columns['pi_tag']
        if col in df_found.columns:
            unique_tags = df_found[col].dropna().unique()
            if len(unique_tags) > 0:
                explanation += f"\n**PI Tag Sensors ({len(unique_tags)}):**\n"
                tag_counts = df_found[col].value_counts()
                for tag, count in list(tag_counts.items())[:10]:
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • `{tag}`: {count} events ({pct:.1f}%)\n"
                if len(unique_tags) > 10:
                    explanation += f"  • ... and {len(unique_tags) - 10} more sensors\n"
    
    explanation += "\n"
    
    # Timeline with Graph
    graph_data = None
    
    if date_col and date_col in df_found.columns:
        df_valid = df_found[df_found[date_col].notna()].copy()
        if len(df_valid) > 0:
            min_date = df_valid[date_col].min()
            max_date = df_valid[date_col].max()
            days_span = (max_date - min_date).days + 1
            
            explanation += f"## 📅 Timeline\n\n"
            explanation += f"• **Period:** {min_date.date()} to {max_date.date()} ({days_span} days)\n"
            
            df_valid['date_only'] = df_valid[date_col].dt.date
            events_by_date = df_valid.groupby('date_only').size().sort_index()
            
            avg_per_day = events_by_date.mean()
            explanation += f"• **Average:** {avg_per_day:.1f} events/day\n"
            explanation += f"• **Range:** {events_by_date.min()}-{events_by_date.max()} events/day\n"
            
            # Generate graph
            graph_data = generate_xy_graph_data(df_found, date_col, f"Timeline {identifier}")
            
            if graph_data:
                explanation += f"• **Trend:** {graph_data['stats']['trend']}\n"
            
            # Top 5 busiest days
            top_days = events_by_date.sort_values(ascending=False).head(5)
            explanation += f"\n**Top 5 Busiest Days:**\n"
            for date, count in top_days.items():
                explanation += f"  • {date}: {count} events\n"
            explanation += "\n"
    
    # Event Details Breakdown
    explanation += f"## 📊 Event Breakdown\n\n"
    
    # Category/Alarm Type
    if key_columns.get('category'):
        col = key_columns['category']
        if col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**Event Type/Category:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # Limit Type (High/Low) - if exists
    if key_columns.get('limit_type'):
        col = key_columns['limit_type']
        if col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**Limit/Alarm Type:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # Other dimensions
    for role in ['area', 'status', 'severity']:
        col = key_columns.get(role)
        if col and col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**{role.title()}:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # ROOT CAUSE ANALYSIS (NEW!) - ENHANCED WITH LLM
    if key_columns.get('description'):
        desc_col = key_columns['description']
        if desc_col in df_found.columns:
            descriptions = df_found[desc_col].dropna()
            if len(descriptions) > 0:
                explanation += f"## 🔬 {t('root_cause', lang)}\n\n"
                
                # Get LLM analysis for root cause
                sample_descriptions = descriptions.head(10).tolist()
                analysis_prompt = f"""Analyze these alarm/event descriptions and provide root cause analysis.

Sample descriptions:
{chr(10).join([f"- {d}" for d in sample_descriptions])}

Provide:
1. Common patterns/issues
2. Likely root causes
3. Equipment health indicators

Keep it concise (max 5 bullet points)."""

                try:
                    root_cause_analysis = call_llm(analysis_prompt, temperature=0.3)
                    explanation += f"{root_cause_analysis}\n\n"
                except:
                    # Fallback to keyword analysis
                    all_text = ' '.join(descriptions.astype(str).str.lower())
                    keywords = ['high', 'low', 'limit', 'alarm', 'trip', 'fault', 'failure', 'leak', 'pressure', 'temperature', 'flow']
                    found_keywords = {kw: all_text.count(kw) for kw in keywords if kw in all_text}
                    
                    if found_keywords:
                        sorted_kw = sorted(found_keywords.items(), key=lambda x: x[1], reverse=True)[:5]
                        explanation += f"**Common Issues Detected:**\n"
                        for kw, count in sorted_kw:
                            explanation += f"  • {kw.title()}: mentioned {count} times\n"
                        explanation += "\n"
                
                # Sample descriptions
                explanation += f"**Sample Event Descriptions:**\n"
                for desc in descriptions.head(3):
                    explanation += f"  • {desc}\n"
                explanation += "\n"
    
    # RECOMMENDATIONS (NEW!) - ENHANCED WITH LLM
    explanation += f"## 💡 {t('corrective_actions', lang)}\n\n"
    
    # Build context for LLM recommendations
    context_for_rec = f"""Equipment: {identifier}
Total events: {len(df_found)}
Period: {df_found[date_col].min().date() if date_col and date_col in df_found.columns else 'N/A'} to {df_found[date_col].max().date() if date_col and date_col in df_found.columns else 'N/A'}"""
    
    # Add severity context if available
    if key_columns.get('severity') and key_columns['severity'] in df_found.columns:
        sev_col = key_columns['severity']
        critical_count = len(df_found[df_found[sev_col].astype(str).str.contains('critical|high', case=False, na=False)])
        context_for_rec += f"\nCritical/High severity: {critical_count} events ({critical_count/len(df_found)*100:.1f}%)"
    
    # Add frequency context
    if date_col and date_col in df_found.columns:
        df_valid = df_found[df_found[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily = df_valid.groupby('date_only').size()
            context_for_rec += f"\nAverage frequency: {daily.mean():.1f} events/day"
    
    rec_prompt = f"""Based on this equipment data, provide specific actionable recommendations:

{context_for_rec}

Provide 4-5 specific, actionable recommendations for maintenance team. Focus on:
- Immediate actions needed
- Preventive measures
- Monitoring improvements

Format: numbered list, concise."""

    try:
        recommendations = call_llm(rec_prompt, temperature=0.3)
        explanation += f"{recommendations}\n"
    except:
        # Fallback to template recommendations
        explanation += f"**Regular Monitoring Recommendations:**\n"
        explanation += f"1. Set up automated alerts for this equipment\n"
        explanation += f"2. Review sensor calibration schedule\n"
        explanation += f"3. Analyze patterns in time-of-day and day-of-week\n"
        explanation += f"4. Consider equipment upgrade if failure rate is high\n"
    
    # Calculate confidence
    confidence, _ = calculate_confidence(df_found, {}, query, "identifier_search")
    
    return df_found.to_dict('records'), explanation, confidence, graph_data

# ====================================================
# PI TAG NAME LOOKUP HANDLER
# ====================================================
def handle_pi_tag_lookup(query, df, key_columns):
    """Handle queries asking for PI tag names of equipment"""
    
    print("  🏷️ PI TAG LOOKUP MODE")
    
    # Extract equipment identifier
    identifier_patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    
    found_identifier = None
    for pattern in identifier_patterns:
        matches = re.findall(pattern, query.upper())
        if matches:
            found_identifier = matches[0]
            break
    
    if not found_identifier:
        return None
    
    print(f"  🎯 Looking up PI tags for: {found_identifier}")
    
    # Search in identifier column
    identifier_col = key_columns.get('identifier')
    if not identifier_col or identifier_col not in df.columns:
        return "❌ Kolom equipment tidak ditemukan"
    
    # Find matching rows
    df_match = df[df[identifier_col].astype(str).str.contains(found_identifier, case=False, na=False)]
    
    if len(df_match) == 0:
        return f"❌ Equipment `{found_identifier}` tidak ditemukan dalam data"
    
    # Look for PI tag columns
    pi_tag_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['tag', 'pi', 'tagname', 'tag_name', 'pitag']):
            pi_tag_columns.append(col)
    
    if len(pi_tag_columns) == 0:
        return f"❌ Tidak ada kolom PI Tag Name dalam dataset"
    
    print(f"  📋 Found PI tag columns: {pi_tag_columns}")
    
    # Build report
    report = f"# 🏷️ PI Tag Names untuk {found_identifier}\n\n"
    report += f"Ditemukan **{len(df_match)} events** untuk equipment ini.\n\n"
    
    for col in pi_tag_columns:
        unique_tags = df_match[col].dropna().unique()
        if len(unique_tags) > 0:
            report += f"## {col}\n\n"
            report += f"Total **{len(unique_tags)} unique tag(s)**:\n\n"
            
            for i, tag in enumerate(sorted(unique_tags), 1):
                count = len(df_match[df_match[col] == tag])
                pct = (count / len(df_match) * 100)
                report += f"{i}. `{tag}` - {count} events ({pct:.1f}%)\n"
            
            report += "\n"
    
    # Add additional info
    report += f"---\n\n"
    report += f"💡 **Info Tambahan:**\n"
    report += f"• Total events: {len(df_match)}\n"
    
    # Date range
    date_col = key_columns.get('date')
    if date_col and date_col in df_match.columns:
        df_valid = df_match[df_match[date_col].notna()]
        if len(df_valid) > 0:
            min_date = df_valid[date_col].min()
            max_date = df_valid[date_col].max()
            report += f"• Periode: {min_date.date()} s/d {max_date.date()}\n"
    
    # Category breakdown
    if key_columns.get('category'):
        cat_col = key_columns['category']
        if cat_col in df_match.columns:
            top_cat = df_match[cat_col].value_counts().head(3)
            report += f"• Top categories: {', '.join([str(k) for k in top_cat.index])}\n"
    
    print(f"  ✅ PI tag lookup complete: {len(pi_tag_columns)} tag column(s) found")
    
    return report

# ====================================================
# TEMPORAL ANALYSIS WITH FULL DATE DISPLAY
# ====================================================
def handle_temporal_analysis(query, df, key_columns):
    """Enhanced temporal analysis with complete date information"""
    
    print("  📊 TEMPORAL ANALYSIS MODE")
    
    q = query.lower()
    lang = detect_language(query)
    date_col = key_columns.get('date')
    
    if not date_col or date_col not in df.columns:
        return None
    
    df_valid = df[df[date_col].notna()].copy()
    if len(df_valid) == 0:
        return None
    
    # Detect what user is asking
    asking_month = 'month' in q or 'bulan' in q
    asking_date = 'date' in q or 'tanggal' in q
    asking_day = ('day' in q or 'hari' in q) and not asking_date
    asking_most = any(kw in q for kw in ['most', 'terbanyak', 'tertinggi', 'maksimal', 'paling banyak'])
    asking_least = any(kw in q for kw in ['least', 'tersedikit', 'terendah', 'minimal', 'paling sedikit'])
    
    report = ""
    
    # MONTH ANALYSIS
    if asking_month:
        df_valid['month'] = df_valid[date_col].dt.month
        month_counts = df_valid.groupby('month').size().sort_values(ascending=False)
        
        month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 
                      5: 'May', 6: 'June', 7: 'July', 8: 'August',
                      9: 'September', 10: 'October', 11: 'November', 12: 'December'}
        
        if asking_most:
            top_month = month_counts.index[0]
            top_count = month_counts.iloc[0]
            top_dates = df_valid[df_valid['month'] == top_month][date_col].dt.date.value_counts().head(5)
            
            report = f"# 📊 {t('most_events', lang)} - {t('month', lang)}\n\n"
            report += f"**{month_names[top_month]}** had the most events: **{top_count:,} events**\n\n"
            report += f"## 📅 Top 5 dates in {month_names[top_month]}:\n\n"
            for date, count in top_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_month = month_counts.index[-1]
            least_count = month_counts.iloc[-1]
            least_dates = df_valid[df_valid['month'] == least_month][date_col].dt.date.value_counts().head(5)
            
            report = f"# 📊 {t('least_events', lang)} - {t('month', lang)}\n\n"
            report += f"**{month_names[least_month]}** had the least events: **{least_count:,} events**\n\n"
            report += f"## 📅 Dates in {month_names[least_month]}:\n\n"
            for date, count in least_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by {t('month', lang)}\n\n"
        
        report += "## 📊 Monthly Ranking:\n\n"
        for i, (month, count) in enumerate(month_counts.items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **{month_names[month]}**: {count:,} events ({pct:.1f}%)\n"
    
    # DATE ANALYSIS
    elif asking_date:
        df_valid['day'] = df_valid[date_col].dt.day
        df_valid['month'] = df_valid[date_col].dt.month
        day_counts = df_valid.groupby('day').size().sort_values(ascending=False)
        
        if asking_most:
            top_day = day_counts.index[0]
            top_count = day_counts.iloc[0]
            
            # Get actual dates
            actual_dates = df_valid[df_valid['day'] == top_day].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('most_events', lang)} - {t('date', lang)}\n\n"
            report += f"**Day {top_day}** of the month had most events: **{top_count:,} events total**\n\n"
            report += f"## 📅 Specific dates with day {top_day}:\n\n"
            for date, count in actual_dates.head(10).items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_day = day_counts.index[-1]
            least_count = day_counts.iloc[-1]
            
            actual_dates = df_valid[df_valid['day'] == least_day].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('least_events', lang)} - {t('date', lang)}\n\n"
            report += f"**Day {least_day}** of the month had least events: **{least_count:,} events total**\n\n"
            report += f"## 📅 Specific dates with day {least_day}:\n\n"
            for date, count in actual_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by Day of Month\n\n"
        
        report += "## 📊 Top 10 Days:\n\n"
        for i, (day, count) in enumerate(day_counts.head(10).items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **Day {day}**: {count:,} events ({pct:.1f}%)\n"
    
    # DAY OF WEEK ANALYSIS  
    elif asking_day:
        df_valid['dow'] = df_valid[date_col].dt.dayofweek
        dow_counts = df_valid.groupby('dow').size().sort_values(ascending=False)
        
        day_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                    4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
        
        if asking_most:
            top_dow = dow_counts.index[0]
            top_count = dow_counts.iloc[0]
            
            # Get specific dates
            top_dates = df_valid[df_valid['dow'] == top_dow].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('most_events', lang)} - Day of Week\n\n"
            report += f"**{day_names[top_dow]}** had the most events: **{top_count:,} events**\n\n"
            report += f"## 📅 Top {day_names[top_dow]} dates:\n\n"
            for date, count in top_dates.head(10).items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_dow = dow_counts.index[-1]
            least_count = dow_counts.iloc[-1]
            
            least_dates = df_valid[df_valid['dow'] == least_dow].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('least_events', lang)} - Day of Week\n\n"
            report += f"**{day_names[least_dow]}** had the least events: **{least_count:,} events**\n\n"
            report += f"## 📅 {day_names[least_dow]} dates:\n\n"
            for date, count in least_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by Day of Week\n\n"
        
        report += "## 📊 Weekly Ranking:\n\n"
        for i, (dow, count) in enumerate(dow_counts.items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **{day_names[dow]}**: {count:,} events ({pct:.1f}%)\n"
    
    if report:
        print(f"  ✅ Temporal analysis complete")
        return report
    
    return None

# ====================================================
# QUERY ROUTER - ENHANCED
# ====================================================
def route_query(query, df, key_columns):
    """
    Ultimate smart routing with:
    - Bilingual support (EN/ID auto-detect)
    - All comparison scenarios
    - Temporal analysis with full dates
    - PI tag lookup
    - Anomaly detection
    - Week filter support
    - Enhanced identifier search
    - Complex multi-intent queries
    """
    
    q = query.lower()
    
    # LANGUAGE DETECTION - Always first
    lang = detect_language(query)
    
    # INTENT DETECTION
    query_intent = detect_query_intent(query)
    
    print(f"  🎯 Detected intents: {query_intent}")
    print(f"  🌐 Language: {lang.upper()}")
    
    # ========================================
    # PRIORITY 1: TEMPORAL ANALYSIS
    # "event terbanyak/tersedikit di bulan/tanggal/hari apa"
    # ========================================
    temporal_keywords_id = ['event terbanyak', 'event tersedikit', 'paling banyak event', 'paling sedikit event']
    temporal_keywords_en = ['most events', 'least events', 'highest event', 'lowest event']
    temporal_time_id = ['bulan apa', 'tanggal berapa', 'hari apa', 'kapan']
    temporal_time_en = ['which month', 'which date', 'which day', 'when']
    
    is_temporal = (
        (any(tk in q for tk in temporal_keywords_id) and any(tt in q for tt in temporal_time_id)) or
        (any(tk in q for tk in temporal_keywords_en) and any(tt in q for tt in temporal_time_en))
    )
    
    if is_temporal:
        print("  📊 Route: TEMPORAL_ANALYSIS")
        result = handle_temporal_analysis(query, df, key_columns)
        if result:
            return {
                "answer": result,
                "method": "temporal_analysis",
                "confidence": 95,
                "graph_data": None,
                "count": len(df)
            }
    
    # ========================================
    # PRIORITY 2: PI TAG LOOKUP
    # "GB-651 pi tag name apa saja?"
    # ========================================
    if 'pi_tag' in query_intent:
        print("  🏷️ Route: PI_TAG_LOOKUP")
        result = handle_pi_tag_lookup(query, df, key_columns)
        if result:
            return {
                "answer": result,
                "method": "pi_tag_lookup",
                "confidence": 95,
                "graph_data": None,
                "count": 0
            }
    
    # ========================================
    # PRIORITY 3: ANOMALY DETECTION
    # "anomali GB-651", "lonjakan bulan agustus"
    # ========================================
    if 'anomaly' in query_intent:
        print("  ⚠️ Route: ANOMALY_DETECTION")
        report, count, confidence, graph_data = handle_anomaly_query(query, df, key_columns)
        return {
            "answer": report,
            "method": "anomaly_detection",
            "confidence": confidence,
            "graph_data": graph_data,
            "count": count
        }
    
# ========================================
    # PRIORITY 4: COMPARISON QUERIES - UNIVERSAL
    # Handles ALL types of comparisons
    # ========================================
    
    # Detect comparison keywords
    comparison_keywords = [
        'bandingkan', 'vs', 'versus', 'dibanding', 'compare', 
        'perbandingan', 'comparison', 'dan'
    ]
    has_comparison_keyword = any(kw in q for kw in comparison_keywords)
    
    # Detect visualization request
    viz_keywords = ['trend', 'grafik', 'graph', 'chart', 'pola', 'pattern', 
                    'analisis', 'analysis', 'visualize', 'visualization', 'visualisasi']
    has_viz_request = any(kw in q for kw in viz_keywords)
    
    if has_comparison_keyword or 'comparison' in query_intent:
        print(f"  📊 Route: UNIVERSAL_COMPARISON")
        
        # Call universal comparison handler
        comparison_result = handle_comparison_query(query, df, key_columns)
        
        if comparison_result:
            confidence, _ = calculate_confidence(df, {}, query, "comparison")
            
            # ALWAYS try to generate graph if visualization requested
            graph_data = None
            
            if has_viz_request:
                print(f"  📈 Visualization requested - generating graph")
                
                # Extract what we're comparing
                identifier_patterns = [r'\b([A-Z]{2,}-\d+[A-Z]*)\b', r'\b([A-Z]{2,}\d+[A-Z]*)\b']
                found_equipments = []
                for pattern in identifier_patterns:
                    found_equipments.extend(re.findall(pattern, query.upper()))
                found_equipments = list(dict.fromkeys(found_equipments))
                
                # Check for "top N"
                top_pattern = r'top\s*(\d+)'
                top_match = re.search(top_pattern, q, re.IGNORECASE)
                
                if top_match:
                    top_n = int(top_match.group(1))
                    identifier_col = key_columns.get('identifier')
                    if identifier_col:
                        found_equipments = df[identifier_col].value_counts().head(top_n).index.tolist()
                        print(f"  🎯 Top {top_n} equipment: {found_equipments}")
                
                # Check for areas
                found_areas = []
                area_col = key_columns.get('area')
                if area_col and area_col in df.columns and len(found_equipments) == 0:
                    unique_areas = df[area_col].dropna().unique()
                    for area in unique_areas:
                        area_str = str(area).lower()
                        if len(area_str) >= 4 and area_str in q:
                            pattern = r'\b' + re.escape(area_str) + r'\b'
                            if re.search(pattern, q):
                                found_areas.append(area)
                
                print(f"  🔍 For graph - Equipment: {found_equipments}, Areas: {found_areas}")
                
                # Extract filters
                _, filters, _ = extract_filters_from_query(query, df, key_columns)
                
                # Generate graph based on what we found
                if len(found_equipments) >= 2:
                    # Multi-equipment comparison graph
                    print(f"  📊 Generating multi-equipment graph")
                    graph_data = generate_comparison_graph_data(df, key_columns, found_equipments, filters)
                elif len(found_areas) >= 2:
                    # Multi-area comparison graph
                    print(f"  📊 Generating multi-area graph")
                    graph_data = generate_area_comparison_graph(df, key_columns, found_areas, filters)
                elif len(found_equipments) == 1:
                    # Single equipment with time filter - use regular graph
                    print(f"  📊 Generating single equipment graph")
                    df_filtered, _, _ = extract_filters_from_query(query, df, key_columns)
                    date_col = key_columns.get('date')
                    if date_col and len(df_filtered) > 0:
                        title = f"{found_equipments[0]} Trend"
                        graph_data = generate_xy_graph_data(df_filtered, date_col, title)
                else:
                    # General filtered graph
                    print(f"  📊 Generating general filtered graph")
                    df_filtered, _, _ = extract_filters_from_query(query, df, key_columns)
                    date_col = key_columns.get('date')
                    if date_col and len(df_filtered) > 0:
                        graph_data = generate_xy_graph_data(df_filtered, date_col, "Event Timeline")
                
                if graph_data:
                    print(f"  ✅ Graph generated: {graph_data.get('title')}")
                    comparison_result += f"\n\n---\n\n## 📈 {'Trend Visualization' if lang == 'en' else 'Visualisasi Trend'}\n\n"
                    
                    if graph_data.get('type') == 'comparison':
                        comparison_result += f"**Multi-Line Comparison Chart**\n\n"
                        for dataset in graph_data['datasets']:
                            comparison_result += f"• **{dataset['label']}**: {dataset['total']:,} events\n"
                    else:
                        comparison_result += f"**{graph_data.get('title', 'Timeline')}**\n\n"
                        if 'stats' in graph_data:
                            comparison_result += f"• Total: {graph_data['stats']['total']:,} events\n"
                            comparison_result += f"• Average: {graph_data['stats']['avg']} events/day\n"
                            comparison_result += f"• Trend: {graph_data['stats']['trend']}\n"
                else:
                    print(f"  ⚠️ Failed to generate visualization")
            
            return {
                "answer": comparison_result,
                "method": "comparison_with_viz" if has_viz_request else "comparison",
                "confidence": confidence,
                "graph_data": graph_data,
                "count": len(df)
            }
    
    # ========================================
    # PRIORITY 5: IDENTIFIER SEARCH (Single Equipment)
    # "GB-651", "BA-109 bulan agustus", "EA-119 week 2"
    # ========================================
    if is_identifier_query(query) and len(found_equipments) == 1:
        print("  🔍 Route: IDENTIFIER_SEARCH")
        result, explanation, confidence, graph_data = search_by_identifier(query, df, key_columns)
        if result is not None:
            return {
                "answer": explanation,
                "method": "identifier_search",
                "confidence": confidence,
                "graph_data": graph_data
            }
    
    # ========================================
    # PRIORITY 6: LLM ANALYSIS (General Data Queries)
    # Any query with data keywords
    # ========================================
    data_keywords = [
        # Indonesian
        'berapa', 'jumlah', 'rata-rata', 'paling', 'sering', 'jarang',
        'event', 'bulan', 'tanggal', 'equipment', 'area', 'list',
        'tertinggi', 'terendah', 'apa saja', 'distribusi', 
        'statistik', 'total', 'trend', 'grafik',
        # English
        'how many', 'total', 'average', 'most', 'least',
        'events', 'month', 'date', 'highest', 'lowest',
        'what', 'show', 'display', 'statistics', 'count', 'graph'
    ]
    
    if any(kw in q for kw in data_keywords):
        print("  🧠 Route: LLM_ANALYSIS")
        answer, count, confidence, graph_data, method = answer_with_enhanced_llm(query, df, key_columns)
        return {
            "answer": answer,
            "method": method,
            "confidence": confidence,
            "graph_data": graph_data,
            "count": count
        }
    
    # ========================================
    # PRIORITY 7: GENERAL LLM (Fallback)
    # Casual questions, greetings, etc.
    # ========================================
    print("  💬 Route: GENERAL_LLM")
    answer = answer_general_question(query, lang)
    return {
        "answer": answer,
        "method": "general_llm",
        "confidence": 100,
        "graph_data": None
    }


# ============================================================
# HELPER: UPDATE answer_general_question untuk bilingual
# ============================================================

def answer_general_question(query, lang='en'):
    """General questions with bilingual support"""
    
    if lang == 'id':
        prompt = f"""Kamu adalah asisten AI yang helpful dan ramah. Jawab pertanyaan berikut dalam Bahasa Indonesia dengan natural.

PERTANYAAN: {query}

JAWABAN:"""
    else:
        prompt = f"""You are a helpful and friendly AI assistant. Answer the following question naturally in English.

QUESTION: {query}

ANSWER:"""
    
    try:
        answer = call_llm(prompt, temperature=0.7)
        return answer
    except Exception as e:
        if lang == 'id':
            return "Maaf, saya mengalami kesulitan menjawab pertanyaan tersebut."
        else:
            return "Sorry, I'm having difficulty answering that question."


# ============================================================
# HELPER: is_identifier_query check
# ============================================================

def is_identifier_query(query):
    """Check if query contains equipment identifier"""
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    return any(re.search(p, query.upper()) for p in patterns)

# ====================================================
# ENDPOINTS
# ====================================================
@app.route("/ask", methods=["POST"])
def ask():
    query = request.json.get("query", "")
    
    if not query:
        return jsonify({"success": False, "error": "Query kosong"}), 400

    try:
        print(f"\n{'='*70}")
        print(f"🔍 QUERY: {query}")
        print(f"{'='*70}")
        
        result = route_query(query, df, KEY_COLUMNS)
        
        print(f"✅ SUCCESS - Method: {result['method']}")
        print(f"📊 Confidence: {result['confidence']:.1f}%")
        if result['method'] == 'cached':
            print(f"⚡ FROM CACHE")
        print(f"{'='*70}\n")
        
        response = {
            "success": True,
            "answer": result['answer'],
            "method": result['method'],
            "confidence": round(result['confidence'], 1),
            "query": query
        }
        
        # Add graph data if available
        if result.get('graph_data'):
            response['graph_data'] = result['graph_data']
        
        return jsonify(response)

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": f"Error: {str(e)}"
        }), 500

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "model": MODEL,
        "data_loaded": len(df),
        "key_columns": KEY_COLUMNS,
        "cache_size": len(QUERY_CACHE)
    })

@app.route("/profile", methods=["GET"])
def get_profile():
    return jsonify(PROFILE)

@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    """Clear query cache"""
    global QUERY_CACHE
    QUERY_CACHE = {}
    return jsonify({"success": True, "message": "Cache cleared"})

@app.route("/cache/stats", methods=["GET"])
def cache_stats():
    """Get cache statistics"""
    return jsonify({
        "total_cached": len(QUERY_CACHE),
        "max_size": CACHE_MAX_SIZE,
        "usage_pct": round(len(QUERY_CACHE) / CACHE_MAX_SIZE * 100, 1)
    })

@app.route("/quick-stats", methods=["GET"])
def quick_stats():
    """Get quick overview statistics"""
    
    stats = {
        "total_events": len(df),
        "date_range": PROFILE.get("date_range"),
        "daily_average": PROFILE.get("daily_stats", {}).get("avg_events_per_day", 0)
    }
    
    # Top equipment
    if KEY_COLUMNS.get('identifier'):
        col = KEY_COLUMNS['identifier']
        top_equipment = df[col].value_counts().head(5).to_dict()
        stats['top_equipment'] = {str(k): int(v) for k, v in top_equipment.items()}
    
    # Top categories
    if KEY_COLUMNS.get('category'):
        col = KEY_COLUMNS['category']
        top_categories = df[col].value_counts().head(5).to_dict()
        stats['top_categories'] = {str(k): int(v) for k, v in top_categories.items()}
    
    # By area
    if KEY_COLUMNS.get('area'):
        col = KEY_COLUMNS['area']
        by_area = df[col].value_counts().to_dict()
        stats['by_area'] = {str(k): int(v) for k, v in by_area.items()}
    
    return jsonify(stats)

@app.route("/suggest", methods=["POST"])
def suggest_queries():
    """Suggest relevant queries based on data"""
    
    suggestions = []
    
    # General suggestions
    suggestions.append({
        "query": "Berapa total event di sistem?",
        "category": "Overview"
    })
    
    # Equipment-based
    if KEY_COLUMNS.get('identifier'):
        col = KEY_COLUMNS['identifier']
        top_eq = df[col].value_counts().head(1).index[0]
        suggestions.append({
            "query": f"Trend {top_eq}",
            "category": "Equipment"
        })
        suggestions.append({
            "query": f"Grafik {top_eq} bulan September",
            "category": "Graph"
        })
    
    # Time-based
    if 'date_range' in PROFILE:
        months = PROFILE['date_range'].get('span_months', [])
        if len(months) >= 2:
            month_names = {1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April', 
                          5: 'Mei', 6: 'Juni', 7: 'Juli', 8: 'Agustus',
                          9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'}
            m1 = month_names.get(months[-2], '')
            m2 = month_names.get(months[-1], '')
            if m1 and m2:
                suggestions.append({
                    "query": f"Bandingkan {m1} vs {m2}",
                    "category": "Comparison"
                })
                suggestions.append({
                    "query": f"Event apa saja di bulan {m2}?",
                    "category": "List"
                })
    
    return jsonify({"suggestions": suggestions[:5]})

@app.route("/export", methods=["POST"])
def export_data():
    """Export filtered data"""
    
    data = request.json
    query = data.get('query', '')
    
    # Apply same filters as query
    df_filtered, filters, _ = extract_filters_from_query(query, df, KEY_COLUMNS)
    
    # Generate CSV
    import io
    output = io.StringIO()
    df_filtered.to_csv(output, index=False)
    csv_content = output.getvalue()
    
    return jsonify({
        "success": True,
        "row_count": len(df_filtered),
        "csv_preview": csv_content[:500] + "..." if len(csv_content) > 500 else csv_content
    })

# ====================================================
# RUN
# ====================================================
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 ENHANCED CSV ANALYZER - FULL FEATURES")
    print("="*80)
    print(f"📊 Data: {len(df):,} events × {len(df.columns)} columns")
    print(f"\n🎯 KEY COLUMNS:")
    for role, col in KEY_COLUMNS.items():
        print(f"  • {role.upper()}: {col if col else 'Not found'}")
    
    if 'date_range' in PROFILE:
        dr = PROFILE['date_range']
        print(f"\n📅 Date Range: {dr['start']} to {dr['end']} ({dr['total_days']} hari)")
    
    if 'daily_stats' in PROFILE:
        ds = PROFILE['daily_stats']
        print(f"📈 Daily Stats: Avg {ds['avg_events_per_day']} events/day")
    
    print(f"\n🧠 Model: {MODEL}")
    print(f"💾 Cache: Max {CACHE_MAX_SIZE} queries")
    print(f"🌐 URL: http://localhost:8000")
    print("="*80)
    print("\n✨ NEW FEATURES:")
    print("  ✅ List equipment untuk query 'apa saja'")
    print("  ✅ Perbandingan: Equipment, Category, Area, Month")
    print("  ✅ XY Graph universal (semua jenis filter)")
    print("  ✅ Graph support: GB-651 bulan September")
    print("  ✅ Graph support: BA-109 tanggal 10-17 September")
    print("  ✅ Enhanced filter: equipment + date combined")
    print("="*80)
    print("\n📝 EXAMPLE QUERIES:")
    print("  • Event apa saja di bulan September? (list equipment)")
    print("  • Bandingkan GB-651 vs EA-119 (equipment comparison)")
    print("  • Bandingkan Ethylene vs Propylene (area comparison)")
    print("  • Grafik GB-651 bulan September (XY graph)")
    print("  • Trend BA-109 tanggal 10-17 September (XY graph range)")
    print("  • Bandingkan Agustus vs September (month comparison)")
    print("="*80 + "\n")
    
    app.run(host="0.0.0.0", port=8000, debug=True)