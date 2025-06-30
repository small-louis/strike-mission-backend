#!/usr/bin/env python3
"""
Simple script to run flight data update from the src root directory.
"""
import os
import sys

def run_flight_update():
    """Run the flight data update script."""
    # Set up environment
    api_key = "481a550d02msh234f431534bff22p1ab62bjsn7b3bb79de1a8"
    os.environ['KIWI_API_KEY'] = api_key
    
    # Change to flights directory and run
    original_dir = os.getcwd()
    try:
        os.chdir('flights')
        
        # Import and run the flight fetcher
        sys.path.insert(0, '.')
        from test_flight_fetcher import main
        
        print("🔍 Running flight data update...")
        main()
        print("✅ Flight data update complete!")
        
    except Exception as e:
        print(f"❌ Error running flight update: {e}")
    finally:
        os.chdir(original_dir)

if __name__ == "__main__":
    run_flight_update() 