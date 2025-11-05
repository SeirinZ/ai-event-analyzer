import re
from datetime import datetime
import pandas as pd
from config import MONTHS_MAP, MONTH_NAMES

def extract_filters_from_query(query, df, key_columns):
    """Extract filters from query with cross-month support"""
    
    q = query.lower()
    filters = {}
    df_filtered = df.copy()
    filter_descriptions = []
    
    date_col = key_columns['date']
    
    # Month filters
    found_months = []
    for month_str, month_num in MONTHS_MAP.items():
        if month_str in q:
            found_months.append(month_num)
    
    if found_months and date_col:
        found_months = list(set(found_months))
        df_filtered = df_filtered[df_filtered[date_col].dt.month.isin(found_months)]
        filters['months'] = found_months
        
        if len(found_months) == 1:
            filter_descriptions.append(f"bulan {MONTH_NAMES[found_months[0]]}")
        else:
            month_strs = [MONTH_NAMES[m] for m in sorted(found_months)]
            filter_descriptions.append(f"bulan {' dan '.join(month_strs)}")
    
    # Cross-month date range (28 august - 16 september)
    cross_month_pattern = r'(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)\s*[-–to/sd/vs]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
    match = re.search(cross_month_pattern, q, re.IGNORECASE)
    
    if match and date_col:
        start_day = int(match.group(1))
        start_month_str = match.group(2).lower()
        end_day = int(match.group(3))
        end_month_str = match.group(4).lower()
        
        start_month = MONTHS_MAP.get(start_month_str)
        end_month = MONTHS_MAP.get(end_month_str)
        
        if start_month and end_month:
            year = df_filtered[date_col].dt.year.mode()[0] if len(df_filtered) > 0 else datetime.now().year
            
            try:
                start_date = datetime(year, start_month, start_day).date()
                end_date = datetime(year, end_month, end_day).date()
                
                df_filtered = df_filtered[
                    (df_filtered[date_col].dt.date >= start_date) & 
                    (df_filtered[date_col].dt.date <= end_date)
                ]
                filters['date_range_full'] = (str(start_date), str(end_date))
                filter_descriptions.append(f"{start_day} {MONTH_NAMES[start_month]} - {end_day} {MONTH_NAMES[end_month]}")
                match = None
            except:
                pass
    
    # Same month date range: "X-Y september"
    if not match:
        date_range_pattern = r'(\d{1,2})\s*[-–to/sd]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
        match_with_month = re.search(date_range_pattern, q, re.IGNORECASE)
        
        if match_with_month and date_col:
            start_day = int(match_with_month.group(1))
            end_day = int(match_with_month.group(2))
            month_str = match_with_month.group(3).lower()
            month_num = MONTHS_MAP.get(month_str)
            
            if month_num:
                df_filtered = df_filtered[
                    (df_filtered[date_col].dt.month == month_num) &
                    (df_filtered[date_col].dt.day >= start_day) & 
                    (df_filtered[date_col].dt.day <= end_day)
                ]
                filters['months'] = [month_num]
                filters['date_range'] = (start_day, end_day)
                filter_descriptions.append(f"{start_day}-{end_day} {MONTH_NAMES[month_num]}")
                match = None
        
        # Fallback: "tanggal X-Y"
        if not match_with_month:
            simple_date_pattern = r'tanggal\s*(\d{1,2})\s*[-–sd/]\s*(\d{1,2})'
            match = re.search(simple_date_pattern, q)
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
    
    # Equipment/Identifier filter
    identifier_patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    
    found_identifiers = []
    for pattern in identifier_patterns:
        matches = re.findall(pattern, query.upper())
        found_identifiers.extend(matches)
    
    found_identifiers = list(dict.fromkeys(found_identifiers))
    
    if found_identifiers and key_columns.get('identifier'):
        id_col = key_columns['identifier']
        if id_col in df_filtered.columns:
            comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'dan']
            is_comparison = any(kw in q for kw in comparison_keywords) and len(found_identifiers) >= 2
            
            if is_comparison:
                filters['comparison_identifiers'] = found_identifiers
            elif len(found_identifiers) == 1:
                identifier = found_identifiers[0]
                df_filtered = df_filtered[
                    df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                ]
                filters['identifier'] = identifier
                filter_descriptions.append(f"equipment {identifier}")
            else:
                identifier = found_identifiers[0]
                df_filtered = df_filtered[
                    df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                ]
                filters['identifier'] = identifier
                filter_descriptions.append(f"equipment {identifier}")
    
    # Value filters (category, area, status, severity)
    for role, col_name in key_columns.items():
        if not col_name or col_name not in df.columns:
            continue
        
        if role in ['category', 'area', 'status', 'severity'] and role not in filters:
            unique_values = df[col_name].dropna().unique()
            
            for value in unique_values:
                value_str = str(value).lower()
                
                if len(value_str) < 4:
                    if value_str == q or f" {value_str} " in f" {q} ":
                        df_filtered = df_filtered[df_filtered[col_name] == value]
                        filters[col_name] = value
                        filter_descriptions.append(f"{role}: {value}")
                        break
                else:
                    matched = False
                    if value_str in q:
                        pattern = r'\b' + re.escape(value_str) + r'\b'
                        if re.search(pattern, q):
                            matched = True
                    
                    if matched:
                        df_filtered = df_filtered[df_filtered[col_name] == value]
                        filters[col_name] = value
                        filter_descriptions.append(f"{role}: {value}")
                        break
    
    return df_filtered, filters, filter_descriptions


def calculate_confidence(df_filtered, filters, query, method):
    """Calculate answer confidence (0-100)"""
    
    confidence = 100.0
    reasons = []
    
    result_count = len(df_filtered)
    if result_count == 0:
        confidence = 0
        reasons.append("No matching data")
        return confidence, reasons
    elif result_count < 3:
        confidence -= 20
        reasons.append(f"Very limited data ({result_count} events)")
    elif result_count < 10:
        confidence -= 10
        reasons.append(f"Limited data ({result_count} events)")
    
    if not filters:
        confidence -= 15
        reasons.append("No specific filters")
    
    ambiguous_terms = ['itu', 'ini', 'nya', 'tersebut', 'yang tadi']
    if any(term in query.lower() for term in ambiguous_terms):
        confidence -= 10
        reasons.append("Ambiguous query")
    
    if method == 'identifier_search':
        confidence = min(confidence + 10, 100)
    elif method == 'llm_analysis':
        confidence -= 5
    
    if result_count > 0:
        null_ratio = df_filtered.isnull().sum().sum() / (len(df_filtered) * len(df_filtered.columns))
        if null_ratio > 0.3:
            confidence -= 15
            reasons.append("Many null values")
    
    if filters.get('months') or filters.get('day'):
        confidence = min(confidence + 5, 100)
    
    return max(confidence, 0), reasons