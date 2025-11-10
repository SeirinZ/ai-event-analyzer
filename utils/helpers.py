"""
Helper functions and utilities
"""
import re
from datetime import datetime

def detect_query_intent(query):
    """Detect specific intent from query"""
    
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
        'average': ['rata-rata', 'average', 'rerata', 'mean', 'avg'],
        'when': ['kapan', 'when', 'tanggal berapa', 'waktu', 'pada saat'],
        'where': ['dimana', 'where', 'lokasi', 'area mana'],
        'why': ['kenapa', 'mengapa', 'why', 'alasan', 'sebab', 'penyebab']
    }
    
    detected = []
    for intent, keywords in intents.items():
        if any(kw in q for kw in keywords):
            detected.append(intent)
    
    return detected if detected else ['general']

def extract_equipment_codes(query):
    """Extract equipment codes from query"""
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    
    found_codes = []
    for pattern in patterns:
        matches = re.findall(pattern, query.upper())
        found_codes.extend(matches)
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(found_codes))

def parse_month_from_query(query):
    """Parse month information from query"""
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
    
    q = query.lower()
    found_months = []
    
    for month_str, month_num in months_map.items():
        if month_str in q and len(month_str) > 3:  # Avoid short matches
            found_months.append(month_num)
    
    return list(set(found_months))

def parse_date_range_from_query(query):
    """Parse date range from query"""
    q = query.lower()
    
    # Cross-month pattern: "28 august - 16 september"
    cross_month_pattern = r'(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)\s*[-–to/sd]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
    match = re.search(cross_month_pattern, q, re.IGNORECASE)
    
    if match:
        months_map = {
            'january': 1, 'jan': 1, 'februari': 2, 'february': 2, 'feb': 2,
            'march': 3, 'mar': 3, 'maret': 3, 'april': 4, 'apr': 4,
            'may': 5, 'mei': 5, 'june': 6, 'jun': 6, 'juni': 6,
            'july': 7, 'jul': 7, 'juli': 7, 'august': 8, 'aug': 8, 
            'agustus': 8, 'ags': 8, 'september': 9, 'sept': 9, 'sep': 9,
            'october': 10, 'oct': 10, 'oktober': 10, 'okt': 10,
            'november': 11, 'nov': 11, 'december': 12, 'dec': 12, 
            'desember': 12, 'des': 12
        }
        
        start_day = int(match.group(1))
        start_month = months_map.get(match.group(2).lower())
        end_day = int(match.group(3))
        end_month = months_map.get(match.group(4).lower())
        
        if start_month and end_month:
            return {
                'type': 'cross_month',
                'start_day': start_day,
                'start_month': start_month,
                'end_day': end_day,
                'end_month': end_month
            }
    
    # Same month range: "tanggal 10-17"
    date_range_pattern = r'tanggal\s*(\d{1,2})\s*[-–sd/]\s*(\d{1,2})'
    match = re.search(date_range_pattern, q)
    if match:
        return {
            'type': 'same_month',
            'start_day': int(match.group(1)),
            'end_day': int(match.group(2))
        }
    
    # Single date: "tanggal 15"
    single_date_pattern = r'tanggal\s*(\d{1,2})\b'
    match = re.search(single_date_pattern, q)
    if match:
        return {
            'type': 'single_day',
            'day': int(match.group(1))
        }
    
    return None

def get_month_name(month_num, lang='en'):
    """Get month name from number"""
    month_names_en = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    
    month_names_id = {
        1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April',
        5: 'Mei', 6: 'Juni', 7: 'Juli', 8: 'Agustus',
        9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'
    }
    
    names = month_names_id if lang == 'id' else month_names_en
    return names.get(month_num, str(month_num))

def get_day_name(day_num, lang='en'):
    """Get day name from number (0=Monday)"""
    day_names_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_id = ['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu']
    
    names = day_names_id if lang == 'id' else day_names_en
    return names[day_num] if 0 <= day_num < 7 else str(day_num)

def calculate_confidence(df_filtered, filters, query, method):
    """Calculate answer confidence (0-100)"""
    
    confidence = 100.0
    reasons = []
    
    # 1. Result count factor
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
    
    # 2. Filter clarity
    if not filters:
        confidence -= 15
        reasons.append("No specific filters")
    
    # 3. Query ambiguity
    ambiguous_terms = ['itu', 'ini', 'nya', 'tersebut', 'yang tadi', 'that', 'this']
    if any(term in query.lower() for term in ambiguous_terms):
        confidence -= 10
        reasons.append("Ambiguous query")
    
    # 4. Method factor
    if method == 'identifier_search':
        confidence = min(confidence + 10, 100)
        reasons.append("Specific identifier search")
    elif method == 'llm_analysis':
        confidence -= 5
        reasons.append("LLM analysis")
    
    # 5. Data quality
    if result_count > 0:
        null_ratio = df_filtered.isnull().sum().sum() / (len(df_filtered) * len(df_filtered.columns))
        if null_ratio > 0.3:
            confidence -= 15
            reasons.append("Many null values in data")
    
    # 6. Date range coverage
    if filters.get('months') or filters.get('day'):
        confidence = min(confidence + 5, 100)
        reasons.append("Specific date filters")
    
    return max(confidence, 0), reasons