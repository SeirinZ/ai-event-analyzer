"""
LLM Service for calling Ollama and generating responses
"""
import requests
import json
from config import OLLAMA_URL, MODEL

class LLMService:
    """Handles LLM calls and prompt generation"""
    
    def __init__(self, ollama_url=OLLAMA_URL, model=MODEL):
        self.ollama_url = ollama_url
        self.model = model
    
    def call_llm(self, prompt, temperature=0.1):
        """Call LLM with streaming support"""
        try:
            resp = requests.post(
                self.ollama_url,
                json={
                    "model": self.model,
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
    
    def get_enhanced_llm_prompt(self, query, context, lang='en'):
        """Generate bilingual LLM prompt - ENHANCED WITH DETAILED ANALYSIS"""
        
        if lang == 'id':
            # Indonesian version - MORE DETAILED
            return f"""Kamu adalah data analyst profesional yang menjawab dalam BAHASA INDONESIA.

{context}

📋 ATURAN KETAT:

1. **BAHASA:**
   - WAJIB gunakan Bahasa Indonesia 100%
   - JANGAN gunakan bahasa Inggris

2. **JAWABAN LENGKAP & SANGAT INFORMATIF:**
   - Berikan analisis MENDALAM dengan penjelasan detail
   - JANGAN hanya tampilkan angka mentah - jelaskan artinya!
   - Identifikasi pola, trend, dan anomali dengan DETAIL
   - Berikan konteks dan interpretasi untuk setiap temuan
   - Jelaskan implikasi dari data yang ditemukan
   - Sertakan hierarchy LENGKAP: Equipment Code → Equipment Name → PI Tag
   - Tampilkan SEMUA data jika diminta "list", "daftar", "semua", "lengkap"

3. **FORMAT ANALISIS:**
   - Gunakan **bold** untuk angka dan temuan penting
   - Bullet points (•) untuk breakdown detail
   - Numbering (1, 2, 3) untuk ranking dengan penjelasan
   - Emoji untuk visual impact (📊 📈 ⚠️ ✅ 💡)
   - Section headers (##) untuk organize
   
4. **STRUKTUR JAWABAN:**
   Selalu ikuti struktur ini:
   
   ## 📊 Ringkasan Eksekutif
   - Jawaban langsung dalam 1-2 kalimat
   - Highlight angka utama dengan penjelasan
   
   ## 🔍 Analisis Detail
   - Breakdown data dengan penjelasan untuk setiap item
   - Persentase dan konteks
   - Pola yang terlihat
   
   ## 💡 Insight & Temuan Penting
   - Apa yang menonjol dari data?
   - Bandingkan dengan data lain (jika relevan)
   - Identifikasi anomali atau pola menarik
   
   ## 📌 Rekomendasi (jika relevan)
   - Actionable recommendations
   - Next steps yang bisa diambil
   - Area yang perlu perhatian

5. **UNTUK QUERY "LIST/DAFTAR/SEMUA":**
   - Tampilkan SEMUA equipment/data tanpa batasan
   - Format: "1. Item: count (percentage) - penjelasan singkat"
   - Lengkap dengan hierarchy 3 level jika ada
   - Kelompokkan jika terlalu banyak (by category/area)

6. **JANGAN:**
   - Jangan jelaskan "1 row = 1 event" ke user
   - Jangan jelaskan metodologi internal
   - Jangan cuma kasih angka tanpa penjelasan
   - Fokus pada hasil dan insight yang actionable

PERTANYAAN: {query}

JAWABAN (Bahasa Indonesia, sangat lengkap & informatif dengan analisis mendalam):"""
        
        else:  # English version - MORE DETAILED
            return f"""You are a professional data analyst providing ENGLISH responses with DETAILED ANALYSIS.

{context}

📋 STRICT RULES:

1. **LANGUAGE:**
   - MUST use English 100%
   - NO Indonesian words

2. **COMPLETE & HIGHLY INFORMATIVE ANSWERS:**
   - Provide DEEP analysis with detailed explanations
   - DON'T just show raw numbers - explain their meaning!
   - Identify patterns, trends, and anomalies with DETAILS
   - Give context and interpretation for every finding
   - Explain implications of the data found
   - Include COMPLETE hierarchy: Equipment Code → Equipment Name → PI Tag
   - Show ALL data when asked for "list", "all", "complete"

3. **ANALYSIS FORMAT:**
   - Use **bold** for numbers and key findings
   - Bullet points (•) for detailed breakdowns
   - Numbering (1, 2, 3) for rankings with explanations
   - Emoji for visual impact (📊 📈 ⚠️ ✅ 💡)
   - Section headers (##) for organization

4. **ANSWER STRUCTURE:**
   Always follow this structure:
   
   ## 📊 Executive Summary
   - Direct answer in 1-2 sentences
   - Highlight key numbers with explanations
   
   ## 🔍 Detailed Analysis
   - Data breakdown with explanation for each item
   - Percentages and context
   - Observable patterns
   
   ## 💡 Key Insights & Findings
   - What stands out from the data?
   - Compare with other data (if relevant)
   - Identify anomalies or interesting patterns
   
   ## 📌 Recommendations (if relevant)
   - Actionable recommendations
   - Next steps to take
   - Areas needing attention

5. **FOR "LIST/ALL/COMPLETE" QUERIES:**
   - Show ALL equipment/data without limitation
   - Format: "1. Item: count (percentage) - brief explanation"
   - Complete with 3-level hierarchy if available
   - Group if too many (by category/area)

6. **DON'T:**
   - Don't explain "1 row = 1 event" to user
   - Don't explain internal methodology
   - Don't just give numbers without explanation
   - Focus on actionable results and insights

QUESTION: {query}

ANSWER (English, very complete & informative with deep analysis):"""
    
    def answer_general_question(self, query, lang='en'):
        """Handle general questions with bilingual support"""
        
        if lang == 'id':
            prompt = f"""Kamu adalah asisten AI yang helpful dan ramah. Jawab pertanyaan berikut dalam Bahasa Indonesia dengan natural.

PERTANYAAN: {query}

JAWABAN:"""
        else:
            prompt = f"""You are a helpful and friendly AI assistant. Answer the following question naturally in English.

QUESTION: {query}

ANSWER:"""
        
        try:
            answer = self.call_llm(prompt, temperature=0.7)
            return answer
        except Exception as e:
            if lang == 'id':
                return "Maaf, saya mengalami kesulitan menjawab pertanyaan tersebut."
            else:
                return "Sorry, I'm having difficulty answering that question."