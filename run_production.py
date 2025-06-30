#!/usr/bin/env python3
"""
Production startup script for Strike Mission Backend
"""
import sys
import os

# Add the current directory and src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, 'src')
sys.path.insert(0, current_dir)
sys.path.insert(0, src_path)

# Import and run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    from backend_api import app
    
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port) 