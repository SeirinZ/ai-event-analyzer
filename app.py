"""
AI Event Analyzer - Main Flask Application
Refactored with modular architecture
"""
from flask import Flask, request, jsonify, render_template
import traceback

# Import configuration
from config import APP_HOST, APP_PORT, DEBUG, CSV_PATH

# Import models
from models.data_loader import DataLoader

# Import services
from services.llm_service import LLMService
from services.filter_service import FilterService
from services.comparison_service import ComparisonService
from services.anomaly_service import AnomalyService
from services.graph_service import GraphService
from services.identifier_service import IdentifierService
from services.temporal_service import TemporalService
from services.query_router import QueryRouter

# Import utilities
from utils.cache_manager import CacheManager

# Initialize Flask app
app = Flask(__name__)

# Initialize data loader
print("\n" + "="*80)
print("🚀 LOADING DATA...")
print("="*80)

data_loader = DataLoader(CSV_PATH)
df = data_loader.load_csv()
key_columns = data_loader.identify_key_columns()
profile = data_loader.build_profile()

print(f"\n✅ Data loaded: {len(df):,} events")
print(f"📊 Key columns identified:")
for role, col in key_columns.items():
    if col:
        print(f"  • {role}: {col}")

# Initialize services
llm_service = LLMService()
filter_service = FilterService(df, key_columns)
comparison_service = ComparisonService(df, key_columns)
anomaly_service = AnomalyService(key_columns)
graph_service = GraphService(key_columns)
identifier_service = IdentifierService(df, key_columns)
temporal_service = TemporalService(df, key_columns)

# Initialize cache
cache_manager = CacheManager()

# Package services for router
services = {
    'llm': llm_service,
    'filter': filter_service,
    'comparison': comparison_service,
    'anomaly': anomaly_service,
    'graph': graph_service,
    'identifier': identifier_service,
    'temporal': temporal_service
}

# Initialize query router
query_router = QueryRouter(df, key_columns, services)

print(f"\n✅ All services initialized")
print("="*80 + "\n")

# ====================================================
# ROUTES - ALIGNED WITH FRONTEND
# ====================================================

@app.route("/")
def home():
    """Render main page"""
    return render_template("index.html")

@app.route("/ask", methods=["POST"])
def ask():
    """Main query endpoint - ALIGNED WITH FE"""
    query = request.json.get("query", "")
    
    if not query:
        return jsonify({"success": False, "error": "Query is empty"}), 400

    try:
        print(f"\n{'='*70}")
        print(f"🔍 QUERY: {query}")
        print(f"{'='*70}")
        
        # Check cache first
        cached_result = cache_manager.get(query)
        if cached_result:
            print(f"⚡ CACHE HIT!")
            print(f"{'='*70}\n")
            cached_result['cached'] = True
            return jsonify({
                "success": True,
                **cached_result
            })
        
        # Route query
        result = query_router.route(query)
        
        print(f"✅ SUCCESS - Method: {result['method']}")
        print(f"📊 Confidence: {result['confidence']:.1f}%")
        if result.get('graph_data'):
            print(f"📈 Graph: {result['graph_data'].get('title', 'Generated')}")
        print(f"{'='*70}\n")
        
        # Cache result
        cache_manager.set(query, {}, {
            "answer": result['answer'],
            "method": result['method'],
            "confidence": result['confidence'],
            "graph_data": result.get('graph_data'),
            "query": query
        })
        
        response = {
            "success": True,
            "answer": result['answer'],
            "method": result['method'],
            "confidence": round(result['confidence'], 1),
            "query": query,
            "cached": False
        }
        
        # Add graph_data if exists (CRITICAL FOR FE)
        if result.get('graph_data'):
            response['graph_data'] = result['graph_data']
        
        return jsonify(response)

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": f"Error: {str(e)}"
        }), 500

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "data_loaded": len(df),
        "key_columns": key_columns,
        "cache_stats": cache_manager.stats()
    })

@app.route("/profile", methods=["GET"])
def get_profile():
    """Get data profile - USED BY FE ON LOAD"""
    return jsonify(profile)

@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    """Clear query cache"""
    cache_manager.clear()
    return jsonify({"success": True, "message": "Cache cleared"})

@app.route("/cache/stats", methods=["GET"])
def cache_stats():
    """Get cache statistics"""
    return jsonify(cache_manager.stats())

@app.route("/quick-stats", methods=["GET"])
def quick_stats():
    """Get quick overview statistics"""
    
    stats = {
        "total_events": len(df),
        "date_range": profile.get("date_range"),
        "daily_average": profile.get("daily_stats", {}).get("avg_events_per_day", 0)
    }
    
    # Top equipment
    if key_columns.get('identifier'):
        col = key_columns['identifier']
        top_equipment = df[col].value_counts().head(5).to_dict()
        stats['top_equipment'] = {str(k): int(v) for k, v in top_equipment.items()}
    
    return jsonify(stats)

# ====================================================
# RUN
# ====================================================
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 AI EVENT ANALYZER - REFACTORED")
    print("="*80)
    print(f"📊 Data: {len(df):,} events × {len(df.columns)} columns")
    
    if profile.get('date_range'):
        dr = profile['date_range']
        print(f"📅 Date Range: {dr['start']} to {dr['end']}")
    
    if profile.get('daily_stats'):
        ds = profile['daily_stats']
        print(f"📈 Daily Avg: {ds['avg_events_per_day']} events/day")
    
    print(f"\n🌐 Server: http://{APP_HOST}:{APP_PORT}")
    print("="*80)
    print("\n✨ FEATURES:")
    print("  ✅ Bilingual Support (EN/ID)")
    print("  ✅ Comparison with Multi-line Graphs")
    print("  ✅ Anomaly Detection with Markers")
    print("  ✅ Equipment Search with Timeline")
    print("  ✅ Temporal Analysis")
    print("  ✅ Smart Caching")
    print("="*80 + "\n")
    
    app.run(host=APP_HOST, port=APP_PORT, debug=DEBUG)