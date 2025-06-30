import sys
import os
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.db_manager import SurfDataDB
from data_fetching.openmeteo import fetch_weather_data, fetch_marine_data
from utils.surf_spots import surf_spots


class CachedDataProvider:
    """
    Provides surf forecast data from cache with fallback to live API.
    Maintains the same interface as the original data fetching functions.
    """
    
    def __init__(self, db_path="data/surf_cache.db", max_age_hours=6):
        """
        Initialize the cached data provider.
        
        :param db_path: Path to SQLite database
        :param max_age_hours: Maximum age of cached data before fetching fresh data
        """
        self.db_path = db_path
        self.max_age_hours = max_age_hours
    
    def get_weather_data(self, latitude, longitude, spot_name=None):
        """
        Get weather data from cache or fetch fresh if needed.
        
        :param latitude: Latitude of the location
        :param longitude: Longitude of the location  
        :param spot_name: Optional spot name for better logging
        :return: Tuple of (hourly_weather, daily_weather) DataFrames
        """
        spot_id = self._get_spot_id(latitude, longitude, spot_name)
        
        with SurfDataDB(self.db_path) as db:
            # Check if we have fresh cached data
            if not db.needs_update(spot_id, 'weather', self.max_age_hours) and \
               not db.needs_update(spot_id, 'daily', self.max_age_hours):
                
                print(f"Using cached weather data for {spot_id}")
                hourly_weather = db.get_weather_data(spot_id)
                daily_weather = db.get_daily_weather(spot_id)
                
                if not hourly_weather.empty and not daily_weather.empty:
                    return hourly_weather, daily_weather
                else:
                    print(f"Cached weather data incomplete for {spot_id}, fetching fresh data...")
            else:
                print(f"Weather data stale for {spot_id}, fetching fresh data...")
        
        # Fallback to live API
        try:
            print(f"Fetching fresh weather data from OpenMeteo for {spot_id}...")
            hourly_weather, daily_weather = fetch_weather_data(latitude, longitude)
            
            # Cache the fresh data
            with SurfDataDB(self.db_path) as db:
                # Store spot info if not exists
                if spot_name and spot_name in surf_spots:
                    spot_data = surf_spots[spot_name].copy()
                    spot_data['name'] = spot_name
                    db.upsert_spot(spot_id, spot_data)
                
                db.store_weather_data(spot_id, hourly_weather)
                db.store_daily_weather(spot_id, daily_weather)
                print(f"Cached fresh weather data for {spot_id}")
            
            return hourly_weather, daily_weather
            
        except Exception as e:
            print(f"Failed to fetch weather data for {spot_id}: {e}")
            raise
    
    def get_marine_data(self, latitude, longitude, spot_name=None):
        """
        Get marine data from cache or fetch fresh if needed.
        
        :param latitude: Latitude of the location
        :param longitude: Longitude of the location
        :param spot_name: Optional spot name for better logging  
        :return: DataFrame with marine data
        """
        spot_id = self._get_spot_id(latitude, longitude, spot_name)
        
        with SurfDataDB(self.db_path) as db:
            # Check if we have fresh cached data
            if not db.needs_update(spot_id, 'marine', self.max_age_hours):
                print(f"Using cached marine data for {spot_id}")
                marine_data = db.get_marine_data(spot_id)
                
                if not marine_data.empty:
                    return marine_data
                else:
                    print(f"Cached marine data incomplete for {spot_id}, fetching fresh data...")
            else:
                print(f"Marine data stale for {spot_id}, fetching fresh data...")
        
        # Fallback to live API
        try:
            print(f"Fetching fresh marine data from OpenMeteo for {spot_id}...")
            marine_data = fetch_marine_data(latitude, longitude)
            
            # Cache the fresh data
            with SurfDataDB(self.db_path) as db:
                # Store spot info if not exists
                if spot_name and spot_name in surf_spots:
                    spot_data = surf_spots[spot_name].copy()
                    spot_data['name'] = spot_name
                    db.upsert_spot(spot_id, spot_data)
                
                db.store_marine_data(spot_id, marine_data)
                print(f"Cached fresh marine data for {spot_id}")
            
            return marine_data
            
        except Exception as e:
            print(f"Failed to fetch marine data for {spot_id}: {e}")
            raise
    
    def _get_spot_id(self, latitude, longitude, spot_name=None):
        """
        Generate a spot ID from coordinates or spot name.
        
        :param latitude: Latitude of the location
        :param longitude: Longitude of the location
        :param spot_name: Optional spot name
        :return: Spot identifier string
        """
        if spot_name:
            return spot_name.lower().replace(' ', '_')
        else:
            # Generate ID from coordinates if no name provided
            return f"spot_{latitude:.3f}_{longitude:.3f}"
    
    def get_cache_status(self):
        """
        Get status of cached data for all spots.
        
        :return: DataFrame with cache status information
        """
        with SurfDataDB(self.db_path) as db:
            return db.get_update_summary()
    
    def clear_cache(self, spot_name=None):
        """
        Clear cached data for a spot or all spots.
        
        :param spot_name: Optional spot name. If None, clears all data.
        """
        with SurfDataDB(self.db_path) as db:
            if spot_name:
                spot_id = spot_name.lower().replace(' ', '_')
                cursor = db.conn.cursor()
                cursor.execute("DELETE FROM weather_data WHERE spot_id = ?", (spot_id,))
                cursor.execute("DELETE FROM marine_data WHERE spot_id = ?", (spot_id,))
                cursor.execute("DELETE FROM daily_weather WHERE spot_id = ?", (spot_id,))
                cursor.execute("DELETE FROM update_log WHERE spot_id = ?", (spot_id,))
                db.conn.commit()
                print(f"Cleared cache for {spot_name}")
            else:
                cursor = db.conn.cursor()
                cursor.execute("DELETE FROM weather_data")
                cursor.execute("DELETE FROM marine_data") 
                cursor.execute("DELETE FROM daily_weather")
                cursor.execute("DELETE FROM update_log")
                db.conn.commit()
                print("Cleared all cached data")


# Convenience functions that maintain the original API
def fetch_weather_data_cached(latitude, longitude, spot_name=None, start_date=None, end_date=None):
    """
    Cached version of fetch_weather_data with same interface.
    
    :param latitude: Latitude of the location
    :param longitude: Longitude of the location
    :param spot_name: Optional spot name for better caching
    :param start_date: Currently ignored (future feature)
    :param end_date: Currently ignored (future feature)
    :return: Tuple of (hourly_weather, daily_weather) DataFrames
    """
    cache = CachedDataProvider()
    return cache.get_weather_data(latitude, longitude, spot_name)


def fetch_marine_data_cached(latitude, longitude, spot_name=None, start_date=None, end_date=None):
    """
    Cached version of fetch_marine_data with same interface.
    
    :param latitude: Latitude of the location
    :param longitude: Longitude of the location
    :param spot_name: Optional spot name for better caching
    :param start_date: Currently ignored (future feature)
    :param end_date: Currently ignored (future feature)
    :return: DataFrame with marine data
    """
    cache = CachedDataProvider()
    return cache.get_marine_data(latitude, longitude, spot_name)


def get_cached_marine_forecast(lat, lon, spot_config=None):
    """
    Get marine forecast from cache if available, otherwise return empty DataFrame.
    Maintains compatibility with original API but uses cache.
    """
    if not spot_config or 'spot_id' not in spot_config:
        return pd.DataFrame()
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return db.get_marine_data(spot_id)


def get_cached_weather_forecast(lat, lon, spot_config=None):
    """
    Get weather forecast from cache if available, otherwise return empty DataFrame.
    Maintains compatibility with original API but uses cache.
    """
    if not spot_config or 'spot_id' not in spot_config:
        return pd.DataFrame()
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return db.get_weather_data(spot_id)


def get_cached_daily_weather(lat, lon, spot_config=None):
    """
    Get daily weather from cache if available, otherwise return empty DataFrame.
    Maintains compatibility with original API but uses cache.
    """
    if not spot_config or 'spot_id' not in spot_config:
        return pd.DataFrame()
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return db.get_daily_weather(spot_id)


def get_cached_scored_forecast(spot_config):
    """
    Get scored forecast from cache if available, otherwise return empty DataFrame.
    This is the new processed-data function.
    
    :param spot_config: Dictionary with spot configuration including 'spot_id'
    :return: DataFrame with scored forecast data
    """
    if not spot_config or 'spot_id' not in spot_config:
        return pd.DataFrame()
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return db.get_scored_forecast(spot_id)


def get_cached_half_day_scores(spot_config):
    """
    Get half-day scores from cache if available, otherwise return empty DataFrame.
    This is the new processed-data function.
    
    :param spot_config: Dictionary with spot configuration including 'spot_id'
    :return: DataFrame with half-day scores
    """
    if not spot_config or 'spot_id' not in spot_config:
        return pd.DataFrame()
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return db.get_half_day_scores(spot_id)


def is_data_fresh(spot_config, data_type='half_day', hours_threshold=6):
    """
    Check if cached data is fresh enough.
    
    :param spot_config: Dictionary with spot configuration including 'spot_id'
    :param data_type: Type of data to check ('weather', 'marine', 'daily', 'scored_forecast', 'half_day')
    :param hours_threshold: Hours after which data is considered stale
    :return: Boolean indicating if data is fresh
    """
    if not spot_config or 'spot_id' not in spot_config:
        return False
    
    spot_id = spot_config['spot_id']
    
    with SurfDataDB("/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db") as db:
        return not db.needs_update(spot_id, data_type, hours_threshold)


if __name__ == "__main__":
    # Test the cache interface
    cache = CachedDataProvider()
    
    # Test with La Graviere
    spot = surf_spots['La Graviere']
    
    print("Testing cached data provider...")
    weather, daily = cache.get_weather_data(spot['lat'], spot['lon'], 'La Graviere')
    marine = cache.get_marine_data(spot['lat'], spot['lon'], 'La Graviere')
    
    print(f"Weather data shape: {weather.shape}")
    print(f"Daily data shape: {daily.shape}")
    print(f"Marine data shape: {marine.shape}")
    
    print("\nCache status:")
    print(cache.get_cache_status()) 