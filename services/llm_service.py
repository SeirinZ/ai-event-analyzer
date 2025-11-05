import requests
import json
from config import OLLAMA_URL, MODEL

def call_llm(prompt, temperature=0.1):
    """Call Ollama LLM"""
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


def get_enhanced_llm_prompt(query, context, lang='en'):
    """Generate bilingual LLM prompt"""
    
    if lang == 'id':
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
    
    else:
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