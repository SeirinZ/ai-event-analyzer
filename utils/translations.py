"""
Translation utilities for bilingual support
"""

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
        'month': 'Month',
        'date': 'Date',
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
        'month': 'Bulan',
        'date': 'Tanggal',
    }
}

def detect_language(query):
    """Auto-detect language from query"""
    indonesian_keywords = [
        'apa', 'berapa', 'yang', 'di', 'pada', 'bulan', 'tanggal', 
        'dan', 'atau', 'dengan', 'untuk', 'bandingkan', 'analisis', 
        'ringkasan', 'rekomendasi', 'perbandingan', 'tertinggi', 
        'terendah', 'kritis', 'normal'
    ]
    q = query.lower()
    id_count = sum(1 for kw in indonesian_keywords if kw in q.split())
    return 'id' if id_count >= 2 else 'en'

def t(key, lang='en', **kwargs):
    """Translate key with optional formatting"""
    text = TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)
    return text.format(**kwargs) if kwargs else text