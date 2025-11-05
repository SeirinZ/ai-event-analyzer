def detect_query_intent(query):
    """Detect specific query intent"""
    
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