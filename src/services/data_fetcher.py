import sys
import os
from datetime import datetime
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data_fetching.openmeteo import fetch_weather_data, fetch_marine_data
from utils.db_manager import SurfDataDB
from utils.surf_spots import surf_spots
from scoring.wave_scoring import score_forecast
from scoring.half_daily_scoring import get_half_day_scores
from scoring.daily_scoring import get_daily_scores


class SurfDataFetcher:
    """
    Service for fetching and caching surf forecast data.
    Manages updates for all spots and determines when fresh data is needed.
    """
    
    def __init__(self, db_path="/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db", update_threshold_hours=6):
        """
        Initialize the data fetcher service.
        
        :param db_path: Path to SQLite database
        :param update_threshold_hours: Hours after which data is considered stale
        """
        self.db_path = db_path
        self.update_threshold = update_threshold_hours
    
    def update_all_spots(self, force_update=False):
        """
        Update forecast data for all spots that need fresh data.
        
        :param force_update: If True, update all spots regardless of age
        :return: Dictionary with update status for each spot
        """
        update_status = {}
        
        with SurfDataDB(self.db_path) as db:
            for spot_name, spot_data in surf_spots.items():
                spot_id = spot_name.lower().replace(' ', '_')
                
                # Upsert spot info
                spot_data_with_name = spot_data.copy()
                spot_data_with_name['name'] = spot_name
                db.upsert_spot(spot_id, spot_data_with_name)
                
                update_status[spot_name] = self._update_spot_data(
                    db, spot_id, spot_data, force_update
                )
        
        return update_status
    
    def _update_spot_data(self, db, spot_id, spot_data, force_update=False):
        """
        Update data for a single spot if needed.
        
        :param db: Database connection
        :param spot_id: Spot identifier
        :param spot_data: Spot configuration
        :param force_update: Force update regardless of age
        :return: Dictionary with update status
        """
        status = {
            'weather_updated': False,
            'marine_updated': False,
            'daily_updated': False,
            'scored_forecast_updated': False,
            'half_day_updated': False,
            'daily_scores_updated': False,
            'errors': []
        }
        
        try:
            # Check if updates are needed for raw data
            needs_weather = force_update or db.needs_update(spot_id, 'weather', self.update_threshold)
            needs_marine = force_update or db.needs_update(spot_id, 'marine', self.update_threshold)
            needs_daily = force_update or db.needs_update(spot_id, 'daily', self.update_threshold)
            
            # Check if processing is needed
            needs_scored = (force_update or 
                          db.needs_update(spot_id, 'scored_forecast', self.update_threshold) or
                          needs_weather or needs_marine)  # Also reprocess if raw data changed
            needs_half_day = (force_update or 
                            db.needs_update(spot_id, 'half_day', self.update_threshold) or
                            needs_scored)  # Reprocess if scored data changed
            needs_daily_scores = (force_update or 
                                db.needs_update(spot_id, 'daily_scores', self.update_threshold) or
                                needs_scored)  # Reprocess if scored data changed
            
            raw_data_needed = needs_weather or needs_marine or needs_daily
            
            if raw_data_needed:
                print(f"Fetching data for {spot_id}...")
                
                # Fetch weather data (includes daily data)
                if needs_weather or needs_daily:
                    try:
                        hourly_weather, daily_weather = fetch_weather_data(
                            spot_data['lat'], spot_data['lon']
                        )
                        
                        if needs_weather:
                            db.store_weather_data(spot_id, hourly_weather)
                            status['weather_updated'] = True
                            print(f"  ✓ Weather data updated for {spot_id}")
                        
                        if needs_daily:
                            db.store_daily_weather(spot_id, daily_weather)
                            status['daily_updated'] = True
                            print(f"  ✓ Daily weather updated for {spot_id}")
                            
                    except Exception as e:
                        error_msg = f"Weather fetch failed for {spot_id}: {e}"
                        status['errors'].append(error_msg)
                        print(f"  ✗ {error_msg}")
                
                # Fetch marine data
                if needs_marine:
                    try:
                        hourly_marine = fetch_marine_data(
                            spot_data['lat'], spot_data['lon']
                        )
                        db.store_marine_data(spot_id, hourly_marine)
                        status['marine_updated'] = True
                        print(f"  ✓ Marine data updated for {spot_id}")
                        
                    except Exception as e:
                        error_msg = f"Marine fetch failed for {spot_id}: {e}"
                        status['errors'].append(error_msg)
                        print(f"  ✗ {error_msg}")
            elif needs_scored or needs_half_day or needs_daily_scores:
                print(f"Processing data for {spot_id} (no raw data fetch needed)...")
            
            # Process scored forecasts if needed
            if needs_scored:
                try:
                    # Get raw data from cache
                    weather_df = db.get_weather_data(spot_id)
                    marine_df = db.get_marine_data(spot_id)
                    
                    if not weather_df.empty and not marine_df.empty:
                        # Merge weather and marine data by date
                        merged_df = pd.merge(weather_df, marine_df, on='date', how='inner')
                        
                        # Rename columns to match expected format
                        merged_df['time'] = merged_df['date']
                        merged_df['wind_angle'] = merged_df['wind_direction_10m']
                        merged_df['wind_strength'] = merged_df['wind_speed_10m']
                        merged_df['wave_size'] = merged_df['wave_height']
                        
                        # Score the forecast
                        scored_df = score_forecast(merged_df, spot_data)
                        db.store_scored_forecast(spot_id, scored_df)
                        status['scored_forecast_updated'] = True
                        print(f"  ✓ Scored forecast updated for {spot_id}")
                    else:
                        status['errors'].append(f"Missing raw data for scoring {spot_id}")
                        
                except Exception as e:
                    error_msg = f"Scoring failed for {spot_id}: {e}"
                    status['errors'].append(error_msg)
                    print(f"  ✗ {error_msg}")
            
            # Process half-day scores if needed
            if needs_half_day:
                try:
                    # Get scored forecast and daily weather from cache
                    scored_df = db.get_scored_forecast(spot_id)
                    daily_weather_df = db.get_daily_weather(spot_id)
                    
                    if not scored_df.empty and not daily_weather_df.empty:
                        # Calculate half-day scores
                        half_day_df = get_half_day_scores(scored_df, spot_id, daily_weather_df)
                        db.store_half_day_scores(spot_id, half_day_df)
                        status['half_day_updated'] = True
                        print(f"  ✓ Half-day scores updated for {spot_id}")
                    else:
                        status['errors'].append(f"Missing scored or daily data for half-day calculation {spot_id}")
                        
                except Exception as e:
                    error_msg = f"Half-day calculation failed for {spot_id}: {e}"
                    status['errors'].append(error_msg)
                    print(f"  ✗ {error_msg}")
            
            # Process daily scores if needed
            if needs_daily_scores:
                try:
                    # Get scored forecast and daily weather from cache
                    scored_df = db.get_scored_forecast(spot_id)
                    daily_weather_df = db.get_daily_weather(spot_id)
                    
                    if not scored_df.empty and not daily_weather_df.empty:
                        # Calculate daily scores
                        daily_scores_df = get_daily_scores(scored_df, spot_id, daily_weather_df)
                        db.store_daily_scores(spot_id, daily_scores_df)
                        status['daily_scores_updated'] = True
                        print(f"  ✓ Daily scores updated for {spot_id}")
                    else:
                        status['errors'].append(f"Missing scored or daily data for daily scores calculation {spot_id}")
                        
                except Exception as e:
                    error_msg = f"Daily scores calculation failed for {spot_id}: {e}"
                    status['errors'].append(error_msg)
                    print(f"  ✗ {error_msg}")
            
            if not (raw_data_needed or needs_scored or needs_half_day or needs_daily_scores):
                print(f"All data for {spot_id} is still fresh (< {self.update_threshold} hours old)")
        
        except Exception as e:
            error_msg = f"Unexpected error updating {spot_id}: {e}"
            status['errors'].append(error_msg)
            print(f"  ✗ {error_msg}")
        
        return status
    
    def get_update_summary(self):
        """
        Get summary of when each spot was last updated.
        
        :return: DataFrame with update information
        """
        import pandas as pd
        
        with SurfDataDB(self.db_path) as db:
            summary_df = pd.read_sql_query("""
                SELECT 
                    s.name as spot_name,
                    u.last_weather_update,
                    u.last_marine_update,
                    u.last_daily_update,
                    u.last_scored_forecast_update,
                    u.last_half_day_update,
                    u.last_daily_scores_update
                FROM spots s
                LEFT JOIN update_log u ON s.spot_id = u.spot_id
                ORDER BY s.name
            """, db.conn)
        
        return summary_df
    
    def force_update_spot(self, spot_name):
        """
        Force update a specific spot regardless of data age.
        
        :param spot_name: Name of the spot to update
        :return: Update status for the spot
        """
        if spot_name not in surf_spots:
            return {'error': f'Spot {spot_name} not found'}
        
        spot_id = spot_name.lower().replace(' ', '_')
        spot_data = surf_spots[spot_name]
        
        with SurfDataDB(self.db_path) as db:
            # Upsert spot info
            spot_data_with_name = spot_data.copy()
            spot_data_with_name['name'] = spot_name
            db.upsert_spot(spot_id, spot_data_with_name)
            
            return self._update_spot_data(db, spot_id, spot_data, force_update=True)


def main():
    """CLI interface for the data fetcher service."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Surf Data Fetcher Service')
    parser.add_argument('--force', action='store_true', 
                       help='Force update all spots regardless of data age')
    parser.add_argument('--spot', type=str, 
                       help='Update specific spot only')
    parser.add_argument('--summary', action='store_true',
                       help='Show update summary for all spots')
    parser.add_argument('--threshold', type=int, default=6,
                       help='Update threshold in hours (default: 6)')
    
    args = parser.parse_args()
    
    fetcher = SurfDataFetcher(update_threshold_hours=args.threshold)
    
    if args.summary:
        print("Update Summary:")
        print(fetcher.get_update_summary())
    elif args.spot:
        print(f"Force updating {args.spot}...")
        status = fetcher.force_update_spot(args.spot)
        print(f"Update status: {status}")
    else:
        print(f"Updating all spots (force: {args.force}, threshold: {args.threshold}h)...")
        status = fetcher.update_all_spots(force_update=args.force)
        
        # Print summary
        total_updates = 0
        total_errors = 0
        for spot, info in status.items():
            updates = sum([info['weather_updated'], info['marine_updated'], info['daily_updated'],
                         info['scored_forecast_updated'], info['half_day_updated']])
            total_updates += updates
            total_errors += len(info['errors'])
            
            if updates > 0 or info['errors']:
                print(f"\n{spot}: {updates} updates, {len(info['errors'])} errors")
                if info['errors']:
                    for error in info['errors']:
                        print(f"  - {error}")
        
        print(f"\nTotal: {total_updates} updates, {total_errors} errors")


if __name__ == "__main__":
    main() 