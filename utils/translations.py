from config import TRANSLATIONS, DEFAULT_LANGUAGE

def detect_language(query):
    """Auto-detect language from query"""
    indonesian_keywords = ['apa', 'berapa', 'yang', 'di', 'pada', 'bulan', 'tanggal', 'dan', 'atau', 'dengan', 'untuk', 'nya', 'ini', 'itu']
    q = query.lower()
    id_count = sum(1 for kw in indonesian_keywords if kw in q.split())
    return 'id' if id_count >= 2 else 'en'


def t(key, lang=DEFAULT_LANGUAGE, **kwargs):
    """Translate key with optional formatting"""
    text = TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)
    return text.format(**kwargs) if kwargs else text