"""
Quick launcher for Streamlit UI
Run this to start the email scraper interface
"""

import os
import sys
import subprocess

def main():
    print("📧 Email Scraper - Streamlit UI")
    print("=" * 40)
    print()
    
    # Check if .env exists
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print()
        print("Please create .env from .env.example:")
        print("  cp .env.example .env")
        print()
        print("Then edit .env with your API keys:")
        print("  - BRIGHTDATA_API_KEY")
        print("  - SUPABASE_URL")
        print("  - SUPABASE_KEY")
        sys.exit(1)
    
    print("✅ .env file found")
    print()
    
    # Check if required packages are installed
    print("Checking dependencies...")
    try:
        import streamlit
        import pandas
        print("✅ All dependencies ready")
    except ImportError:
        print("❌ Dependencies not installed!")
        print()
        print("Installing requirements...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
    
    print()
    print("🚀 Launching Streamlit UI...")
    print("    Opening: http://localhost:8501")
    print()
    print("Press Ctrl+C to stop the server")
    print()
    
    # Launch Streamlit
    try:
        subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
    except KeyboardInterrupt:
        print()
        print("Streamlit UI stopped.")
        sys.exit(0)

if __name__ == "__main__":
    main()
