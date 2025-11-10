"""
Configuration file for AI Event Analyzer
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Application Settings
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", 8000))
DEBUG = os.getenv("DEBUG", "True").lower() == "true"

# LLM Settings
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL = os.getenv("MODEL", "llama3:8b")

# Data Settings
CSV_PATH = os.getenv("CSV_PATH", "events.csv")

# Cache Settings
CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", 100))
CACHE_EXPIRY_SECONDS = int(os.getenv("CACHE_EXPIRY_SECONDS", 300))

# Language Settings
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "en")
SUPPORTED_LANGUAGES = ["en", "id"]