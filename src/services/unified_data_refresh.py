#!/usr/bin/env python3
"""
Unified Data Refresh Service

This service ensures that when forecast data is updated, ALL dependent data is also refreshed:
1. Raw weather and marine data
2. Scored forecasts with proper ratings
3. Half-day scores (used by trip analysis)
4. Daily scores (used by long-range forecast)

This prevents the issue where trip data becomes stale compared to forecast data.
"""

import sys
import os
from datetime import datetime

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data_fetching.openmeteo import fetch_weather_data, fetch_marine_data
from utils.db_manager import SurfDataDB
from utils.surf_spots import surf_spots
from utils.data_processor import merge_weather_and_marine_data
from scoring.wave_scoring import score_forecast
from scoring.half_daily_scoring import get_half_day_scores
from scoring.daily_scoring import get_daily_scores


class UnifiedDataRefresh:
    """
    Service that ensures all data layers are updated consistently.
    """
    
    def __init__(self, db_path="/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db"):
        self.db_path = db_path
    
    def refresh_all_spots(self, force_update=False):
        """
        Refresh all data for all spots in a coordinated manner.
        
        :param force_update: If True, refresh all spots regardless of age
        :return: Dictionary with refresh status for each spot
        """
        print("🌊 Starting unified data refresh...")
        print("This will update forecast data AND trip analysis data together")
        print("=" * 60)
        
        refresh_status = {}
        
        for spot_name in surf_spots.keys():
            print(f"\n📊 Refreshing {spot_name}...")
            refresh_status[spot_name] = self.refresh_spot(spot_name, force_update)
        
        print(f"\n✅ Unified refresh complete!")
        return refresh_status
    
    def refresh_spot(self, spot_name, force_update=False):
        """
        Refresh all data layers for a single spot.
        
        :param spot_name: Name of the surf spot
        :param force_update: Force refresh regardless of age
        :return: Dictionary with status of each data layer
        """
        spot_data = surf_spots[spot_name]
        spot_id = spot_name.lower().replace(' ', '_')
        
        status = {
            'weather_fetched': False,
            'marine_fetched': False,
            'scored_forecast_updated': False,
            'half_day_scores_updated': False,
            'daily_scores_updated': False,
            'errors': []
        }
        
        try:
            with SurfDataDB(self.db_path) as db:
                # Step 1: Check if we need fresh raw data
                needs_weather = force_update or db.needs_update(spot_id, 'weather', 6)
                needs_marine = force_update or db.needs_update(spot_id, 'marine', 6)
                
                if needs_weather or needs_marine or force_update:
                    print(f"  📡 Fetching fresh raw data...")
                    
                    # Fetch fresh weather data
                    if needs_weather or force_update:
                        try:
                            weather_df, daily_weather_df = fetch_weather_data(spot_data['lat'], spot_data['lon'])
                            db.store_weather_data(spot_id, weather_df)
                            db.store_daily_weather(spot_id, daily_weather_df)
                            status['weather_fetched'] = True
                            print(f"    ✓ Weather data updated")
                        except Exception as e:
                            status['errors'].append(f"Weather fetch failed: {e}")
                            print(f"    ✗ Weather fetch failed: {e}")
                    
                    # Fetch fresh marine data
                    if needs_marine or force_update:
                        try:
                            marine_df, daily_marine_df = fetch_marine_data(spot_data['lat'], spot_data['lon'])
                            db.store_marine_data(spot_id, marine_df)
                            db.store_daily_marine(spot_id, daily_marine_df)
                            status['marine_fetched'] = True
                            print(f"    ✓ Marine data updated")
                        except Exception as e:
                            status['errors'].append(f"Marine fetch failed: {e}")
                            print(f"    ✗ Marine fetch failed: {e}")
                else:
                    print(f"  📋 Using existing raw data (still fresh)")
                
                # Step 2: Process scored forecasts (always if raw data was updated)
                if status['weather_fetched'] or status['marine_fetched'] or force_update:
                    print(f"  ⚙️  Processing scored forecasts...")
                    try:
                        # Get the raw data
                        weather_df = db.get_weather_data(spot_id)
                        marine_df = db.get_marine_data(spot_id)
                        
                        if not weather_df.empty and not marine_df.empty:
                            # Merge and process
                            merged_df = merge_weather_and_marine_data(weather_df, marine_df)
                            scored_df = score_forecast(merged_df, spot_data)
                            db.store_scored_forecast(spot_id, scored_df)
                            status['scored_forecast_updated'] = True
                            print(f"    ✓ Scored forecasts updated with proper ratings")
                        else:
                            status['errors'].append("Missing raw data for scoring")
                    except Exception as e:
                        status['errors'].append(f"Scoring failed: {e}")
                        print(f"    ✗ Scoring failed: {e}")
                
                # Step 3: Update half-day scores (used by trip analysis)
                if status['scored_forecast_updated'] or force_update:
                    print(f"  📈 Updating half-day scores...")
                    try:
                        scored_df = db.get_scored_forecast(spot_id)
                        daily_weather_df = db.get_daily_weather(spot_id)
                        
                        if not scored_df.empty and not daily_weather_df.empty:
                            half_day_df = get_half_day_scores(scored_df, spot_name, daily_weather_df)
                            db.store_half_day_scores(spot_id, half_day_df)
                            status['half_day_scores_updated'] = True
                            print(f"    ✓ Half-day scores updated ({len(half_day_df)} records)")
                        else:
                            status['errors'].append("Missing data for half-day calculation")
                    except Exception as e:
                        status['errors'].append(f"Half-day scoring failed: {e}")
                        print(f"    ✗ Half-day scoring failed: {e}")
                
                # Step 4: Update daily scores (used by long-range forecast)
                if status['scored_forecast_updated'] or force_update:
                    print(f"  📊 Updating daily scores...")
                    try:
                        scored_df = db.get_scored_forecast(spot_id)
                        daily_weather_df = db.get_daily_weather(spot_id)
                        
                        if not scored_df.empty and not daily_weather_df.empty:
                            daily_scores_df = get_daily_scores(scored_df, spot_name, daily_weather_df)
                            db.store_daily_scores(spot_id, daily_scores_df)
                            status['daily_scores_updated'] = True
                            print(f"    ✓ Daily scores updated ({len(daily_scores_df)} records)")
                        else:
                            status['errors'].append("Missing data for daily scores calculation")
                    except Exception as e:
                        status['errors'].append(f"Daily scoring failed: {e}")
                        print(f"    ✗ Daily scoring failed: {e}")
                
                # Summary
                updates = [k for k, v in status.items() if k.endswith('_updated') and v]
                if updates:
                    print(f"  ✅ {spot_name}: Updated {len(updates)} data layers")
                elif not status['errors']:
                    print(f"  ✅ {spot_name}: All data up to date")
                else:
                    print(f"  ⚠️ {spot_name}: Some updates failed")
                
        except Exception as e:
            status['errors'].append(f"Unexpected error: {e}")
            print(f"  ✗ {spot_name}: Unexpected error: {e}")
        
        return status
    
    def refresh_specific_spots(self, spot_names, force_update=False):
        """
        Refresh data for specific spots only.
        
        :param spot_names: List of spot names to refresh
        :param force_update: Force refresh regardless of age
        :return: Dictionary with refresh status for each spot
        """
        print(f"🌊 Refreshing {len(spot_names)} specific spots...")
        print("=" * 50)
        
        refresh_status = {}
        
        for spot_name in spot_names:
            if spot_name not in surf_spots:
                print(f"⚠️ Unknown spot: {spot_name}")
                continue
            
            print(f"\n📊 Refreshing {spot_name}...")
            refresh_status[spot_name] = self.refresh_spot(spot_name, force_update)
        
        print(f"\n✅ Specific spots refresh complete!")
        return refresh_status


def main():
    """Command line interface for the unified refresh service."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified data refresh service')
    parser.add_argument('--force', action='store_true', help='Force refresh all spots')
    parser.add_argument('--spots', nargs='+', help='Refresh specific spots only')
    
    args = parser.parse_args()
    
    refresher = UnifiedDataRefresh()
    
    if args.spots:
        status = refresher.refresh_specific_spots(args.spots, args.force)
    else:
        status = refresher.refresh_all_spots(args.force)
    
    # Print summary
    print(f"\n📋 REFRESH SUMMARY:")
    print("=" * 40)
    for spot_name, spot_status in status.items():
        updates = [k for k, v in spot_status.items() if k.endswith('_updated') and v]
        errors = len(spot_status.get('errors', []))
        
        if updates:
            print(f"✅ {spot_name}: {len(updates)} updates, {errors} errors")
        elif errors == 0:
            print(f"📋 {spot_name}: Up to date")
        else:
            print(f"❌ {spot_name}: {errors} errors")


if __name__ == "__main__":
    main() 