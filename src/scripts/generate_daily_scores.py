#!/usr/bin/env python3
"""
Daily Scores Generation Script

This script generates daily average scores for all surf spots and caches them in the database.
It's similar to the half-day scoring but calculates full-day averages during daylight hours.

Usage:
    python generate_daily_scores.py [--force-update] [--spot SPOT_NAME]
"""

import sys
import os
import argparse
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.db_manager import SurfDataDB
from utils.surf_spots import surf_spots
from scoring.daily_scoring import get_daily_scores, cache_daily_scores
from services.data_fetcher import SurfDataFetcher


def generate_daily_scores_for_spot(spot_name, spot_data, db_manager, force_update=False):
    """
    Generate and cache daily scores for a single spot.
    
    :param spot_name: Name of the surf spot
    :param spot_data: Spot configuration data
    :param db_manager: Database manager instance
    :param force_update: Whether to force update even if data is fresh
    :return: Success status
    """
    spot_id = spot_name.lower().replace(' ', '_')
    
    try:
        # Check if we need to update daily scores
        if not force_update and not db_manager.needs_update(spot_id, 'daily_scores', hours_threshold=12):
            print(f"✓ Daily scores for {spot_name} are up to date")
            return True
        
        print(f"📊 Generating daily scores for {spot_name}...")
        
        # Get scored forecast data from database
        scored_forecast = db_manager.get_scored_forecast(spot_id)
        if scored_forecast.empty:
            print(f"✗ No scored forecast data found for {spot_name}")
            return False
        
        # Get daily weather data for sunrise/sunset times
        daily_weather = db_manager.get_daily_weather(spot_id)
        if daily_weather.empty:
            print(f"✗ No daily weather data found for {spot_name}")
            return False
        
        # Generate daily scores
        daily_scores = get_daily_scores(scored_forecast, spot_name, daily_weather)
        
        if daily_scores.empty:
            print(f"✗ No daily scores generated for {spot_name}")
            return False
        
        # Cache the daily scores
        cache_daily_scores(spot_name, daily_scores, db_manager)
        
        print(f"✓ Generated {len(daily_scores)} daily scores for {spot_name}")
        return True
        
    except Exception as e:
        print(f"✗ Error generating daily scores for {spot_name}: {e}")
        return False


def main():
    """Main function to generate daily scores for all or specific spots."""
    parser = argparse.ArgumentParser(description='Generate daily surf scores')
    parser.add_argument('--force-update', action='store_true', 
                       help='Force update even if data is fresh')
    parser.add_argument('--spot', type=str, 
                       help='Generate scores for specific spot only')
    
    args = parser.parse_args()
    
    print("🌊 Daily Surf Scores Generator")
    print("=" * 40)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize database
    db_path = "/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db"
    
    success_count = 0
    total_count = 0
    
    with SurfDataDB(db_path) as db:
        # Determine which spots to process
        if args.spot:
            if args.spot in surf_spots:
                spots_to_process = {args.spot: surf_spots[args.spot]}
            else:
                print(f"✗ Spot '{args.spot}' not found in surf_spots")
                return 1
        else:
            spots_to_process = surf_spots
        
        # Process each spot
        for spot_name, spot_data in spots_to_process.items():
            total_count += 1
            
            if generate_daily_scores_for_spot(spot_name, spot_data, db, args.force_update):
                success_count += 1
    
    print("\n" + "=" * 40)
    print(f"✓ Successfully processed {success_count}/{total_count} spots")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return 0 if success_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main()) 