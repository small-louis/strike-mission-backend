import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
import os


class SurfDataDB:
    """
    Database manager for surf forecast data caching.
    Handles SQLite database operations for weather, marine, and daily data.
    """
    
    def __init__(self, db_path="data/surf_cache.db"):
        """
        Initialize database connection and create tables if they don't exist.
        
        :param db_path: Path to SQLite database file
        """
        # Ensure path is relative to project root
        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent.parent.parent
            db_path = project_root / db_path
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self._create_tables()
    
    def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Spots table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS spots (
                spot_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                swell_dir_range TEXT,
                wind_dir_range TEXT,
                timezone TEXT
            )
        """)
        
        # Weather data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                spot_id TEXT,
                timestamp TEXT,
                temperature_2m REAL,
                wind_speed_10m REAL,
                wind_direction_10m REAL,
                wind_gusts_10m REAL,
                created_at TEXT,
                PRIMARY KEY (spot_id, timestamp),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Marine data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marine_data (
                spot_id TEXT,
                timestamp TEXT,
                wave_height REAL,
                wave_direction REAL,
                wave_period REAL,
                sea_level_height_msl REAL,
                created_at TEXT,
                PRIMARY KEY (spot_id, timestamp),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Daily weather table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_weather (
                spot_id TEXT,
                date TEXT,
                sunrise INTEGER,
                sunset INTEGER,
                daylight_duration REAL,
                temperature_2m_min REAL,
                temperature_2m_max REAL,
                created_at TEXT,
                PRIMARY KEY (spot_id, date),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Scored forecasts table (processed data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scored_forecasts (
                spot_id TEXT,
                timestamp TEXT,
                wave_size REAL,
                wave_direction REAL,
                wave_period REAL,
                wind_strength REAL,
                wind_angle REAL,
                swell_direction_points INTEGER,
                wind_points INTEGER,
                wave_height_points INTEGER,
                wave_period_points INTEGER,
                total_points REAL,
                surf_rating TEXT,
                wind_relationship TEXT,
                wave_height_ft REAL,
                conditions_summary TEXT,
                created_at TEXT,
                PRIMARY KEY (spot_id, timestamp),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Add new rating columns to existing tables if they don't exist
        try:
            cursor.execute("ALTER TABLE scored_forecasts ADD COLUMN surf_rating TEXT")
        except:
            pass  # Column already exists
        try:
            cursor.execute("ALTER TABLE scored_forecasts ADD COLUMN wind_relationship TEXT")
        except:
            pass  # Column already exists
        try:
            cursor.execute("ALTER TABLE scored_forecasts ADD COLUMN wave_height_ft REAL")
        except:
            pass  # Column already exists
        try:
            cursor.execute("ALTER TABLE scored_forecasts ADD COLUMN conditions_summary TEXT")
        except:
            pass  # Column already exists
        
        # Half-day scores table (aggregated processed data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS half_day_scores (
                spot_id TEXT,
                date TEXT,
                half_day TEXT,
                avg_total_points REAL,
                created_at TEXT,
                PRIMARY KEY (spot_id, date, half_day),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Daily scores table (full day aggregated data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_scores (
                spot_id TEXT,
                date TEXT,
                avg_total_points REAL,
                avg_surf_rating TEXT,
                wind_relationship TEXT,
                conditions_summary TEXT,
                created_at TEXT,
                PRIMARY KEY (spot_id, date),
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Update log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS update_log (
                spot_id TEXT PRIMARY KEY,
                last_weather_update TEXT,
                last_marine_update TEXT,
                last_daily_update TEXT,
                last_scored_forecast_update TEXT,
                last_half_day_update TEXT,
                last_daily_scores_update TEXT,
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            )
        """)
        
        # Add sea_level_height_msl column to existing marine_data table if it doesn't exist
        try:
            cursor.execute("ALTER TABLE marine_data ADD COLUMN sea_level_height_msl REAL")
        except:
            pass  # Column already exists
        
        self.conn.commit()
    
    def upsert_spot(self, spot_id, spot_data):
        """
        Insert or update spot information.
        
        :param spot_id: Unique spot identifier
        :param spot_data: Dictionary with spot details
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO spots 
            (spot_id, name, lat, lon, swell_dir_range, wind_dir_range, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            spot_id,
            spot_data.get('name', spot_id),
            spot_data['lat'],
            spot_data['lon'],
            json.dumps(spot_data.get('swell_dir_range')),
            json.dumps(spot_data.get('wind_dir_range')),
            spot_data.get('timezone')
        ))
        self.conn.commit()
    
    def store_weather_data(self, spot_id, weather_df):
        """
        Store weather data for a spot.
        
        :param spot_id: Spot identifier
        :param weather_df: DataFrame with weather data
        """
        weather_df = weather_df.copy()
        weather_df['spot_id'] = spot_id
        weather_df['created_at'] = datetime.now().isoformat()
        weather_df['timestamp'] = weather_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select only the columns we need
        columns = ['spot_id', 'timestamp', 'temperature_2m', 'wind_speed_10m', 
                  'wind_direction_10m', 'wind_gusts_10m', 'created_at']
        weather_df[columns].to_sql('weather_data', self.conn, if_exists='replace', 
                                  method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'weather')
    
    def store_marine_data(self, spot_id, marine_df):
        """
        Store marine data for a spot.
        
        :param spot_id: Spot identifier
        :param marine_df: DataFrame with marine data
        """
        marine_df = marine_df.copy()
        marine_df['spot_id'] = spot_id
        marine_df['created_at'] = datetime.now().isoformat()
        marine_df['timestamp'] = marine_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select only the columns we need (including tide data)
        columns = ['spot_id', 'timestamp', 'wave_height', 'wave_direction', 
                  'wave_period', 'created_at']
        
        # Add sea_level_height_msl if it exists in the data
        if 'sea_level_height_msl' in marine_df.columns:
            columns.append('sea_level_height_msl')
        
        # Delete existing data for this spot first
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM marine_data WHERE spot_id = ?", (spot_id,))
        
        # Insert new data
        marine_df[columns].to_sql('marine_data', self.conn, if_exists='append', 
                                 method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'marine')
    
    def store_daily_weather(self, spot_id, daily_df):
        """
        Store daily weather data for a spot.
        
        :param spot_id: Spot identifier
        :param daily_df: DataFrame with daily weather data
        """
        daily_df = daily_df.copy()
        daily_df['spot_id'] = spot_id
        daily_df['created_at'] = datetime.now().isoformat()
        daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')
        
        # Select only the columns we need
        columns = ['spot_id', 'date', 'sunrise', 'sunset', 'daylight_duration',
                  'temperature_2m_min', 'temperature_2m_max', 'created_at']
        
        # Delete existing data for this spot first
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM daily_weather WHERE spot_id = ?", (spot_id,))
        
        # Insert new data
        daily_df[columns].to_sql('daily_weather', self.conn, if_exists='append', 
                                method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'daily')
    
    def store_scored_forecast(self, spot_id, scored_df):
        """
        Store scored forecast data for a spot.
        
        :param spot_id: Spot identifier
        :param scored_df: DataFrame with scored forecast data
        """
        scored_df = scored_df.copy()
        scored_df['spot_id'] = spot_id
        scored_df['created_at'] = datetime.now().isoformat()
        scored_df['timestamp'] = scored_df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select only the columns we need (including new rating columns)
        columns = ['spot_id', 'timestamp', 'wave_size', 'wave_direction', 'wave_period',
                  'wind_strength', 'wind_angle', 'swell_direction_points', 'wind_points',
                  'wave_height_points', 'wave_period_points', 'total_points', 
                  'surf_rating', 'wind_relationship', 'wave_height_ft', 'conditions_summary',
                  'created_at']
        
        # Only include columns that actually exist in the DataFrame
        available_columns = [col for col in columns if col in scored_df.columns]
        
        # Delete existing data for this spot first
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM scored_forecasts WHERE spot_id = ?", (spot_id,))
        
        # Insert new data
        scored_df[available_columns].to_sql('scored_forecasts', self.conn, if_exists='append', 
                                           method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'scored_forecast')
    
    def store_half_day_scores(self, spot_id, half_day_df):
        """
        Store half-day scores for a spot.
        
        :param spot_id: Spot identifier
        :param half_day_df: DataFrame with half-day scores
        """
        half_day_df = half_day_df.copy()
        half_day_df['spot_id'] = spot_id
        half_day_df['created_at'] = datetime.now().isoformat()
        
        # Handle date column - might already be string or date object
        if 'date' in half_day_df.columns and len(half_day_df) > 0:
            if pd.api.types.is_datetime64_any_dtype(half_day_df['date']):
                # It's a datetime-like object
                half_day_df['date'] = half_day_df['date'].dt.strftime('%Y-%m-%d')
            else:
                # It's already a string, just ensure it's properly formatted
                half_day_df['date'] = pd.to_datetime(half_day_df['date']).dt.strftime('%Y-%m-%d')
        
        # Select only the columns we need
        columns = ['spot_id', 'date', 'half_day', 'avg_total_points', 'created_at']
        
        # Delete existing data for this spot first
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM half_day_scores WHERE spot_id = ?", (spot_id,))
        
        # Insert new data
        half_day_df[columns].to_sql('half_day_scores', self.conn, if_exists='append', 
                                   method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'half_day')
    
    def store_daily_scores(self, spot_id, daily_df):
        """
        Store daily scores for a spot.
        
        :param spot_id: Spot identifier
        :param daily_df: DataFrame with daily scores
        """
        daily_df = daily_df.copy()
        daily_df['spot_id'] = spot_id
        daily_df['created_at'] = datetime.now().isoformat()
        
        # Handle date column - might already be string or date object
        if 'date' in daily_df.columns and len(daily_df) > 0:
            if pd.api.types.is_datetime64_any_dtype(daily_df['date']):
                # It's a datetime-like object
                daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')
            else:
                # It's already a string, just ensure it's properly formatted
                daily_df['date'] = pd.to_datetime(daily_df['date']).dt.strftime('%Y-%m-%d')
        
        # Select only the columns we need
        columns = ['spot_id', 'date', 'avg_total_points', 'avg_surf_rating', 'wind_relationship', 'conditions_summary', 'created_at']
        available_columns = [col for col in columns if col in daily_df.columns]
        
        # Delete existing data for this spot first
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM daily_scores WHERE spot_id = ?", (spot_id,))
        
        # Insert new data
        daily_df[available_columns].to_sql('daily_scores', self.conn, if_exists='append', 
                                          method='multi', index=False)
        
        self._update_last_fetch(spot_id, 'daily_scores')
    
    def _update_last_fetch(self, spot_id, data_type):
        """Update the last fetch timestamp for a spot and data type."""
        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        
        # First, ensure the spot_id exists in the table
        cursor.execute("INSERT OR IGNORE INTO update_log (spot_id) VALUES (?)", (spot_id,))
        
        # Update the specific data type timestamp
        if data_type == 'weather':
            cursor.execute("UPDATE update_log SET last_weather_update = ? WHERE spot_id = ?", (now, spot_id))
        elif data_type == 'marine':
            cursor.execute("UPDATE update_log SET last_marine_update = ? WHERE spot_id = ?", (now, spot_id))
        elif data_type == 'daily':
            cursor.execute("UPDATE update_log SET last_daily_update = ? WHERE spot_id = ?", (now, spot_id))
        elif data_type == 'scored_forecast':
            cursor.execute("UPDATE update_log SET last_scored_forecast_update = ? WHERE spot_id = ?", (now, spot_id))
        elif data_type == 'half_day':
            cursor.execute("UPDATE update_log SET last_half_day_update = ? WHERE spot_id = ?", (now, spot_id))
        elif data_type == 'daily_scores':
            cursor.execute("UPDATE update_log SET last_daily_scores_update = ? WHERE spot_id = ?", (now, spot_id))
        
        self.conn.commit()
    
    def needs_update(self, spot_id, data_type, hours_threshold=6):
        """
        Check if data needs updating based on age threshold.
        
        :param spot_id: Spot identifier
        :param data_type: 'weather', 'marine', 'daily', 'scored_forecast', or 'half_day'
        :param hours_threshold: Hours after which data is considered stale
        :return: Boolean indicating if update is needed
        """
        cursor = self.conn.cursor()
        
        column_map = {
            'weather': 'last_weather_update',
            'marine': 'last_marine_update',
            'daily': 'last_daily_update',
            'scored_forecast': 'last_scored_forecast_update',
            'half_day': 'last_half_day_update',
            'daily_scores': 'last_daily_scores_update'
        }
        
        column = column_map.get(data_type)
        if not column:
            return True
        
        cursor.execute(f"SELECT {column} FROM update_log WHERE spot_id = ?", (spot_id,))
        result = cursor.fetchone()
        
        if not result or not result[0]:
            return True
        
        last_update = datetime.fromisoformat(result[0])
        # Make both datetimes timezone-naive for comparison
        if last_update.tzinfo is not None:
            last_update = last_update.replace(tzinfo=None)
        
        threshold = datetime.now() - timedelta(hours=hours_threshold)
        
        return last_update < threshold
    
    def get_weather_data(self, spot_id):
        """Retrieve weather data for a spot."""
        df = pd.read_sql_query("""
            SELECT timestamp, temperature_2m, wind_speed_10m, wind_direction_10m, wind_gusts_10m
            FROM weather_data 
            WHERE spot_id = ?
            ORDER BY timestamp
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['timestamp'])
            df = df.drop('timestamp', axis=1)
        
        return df
    
    def get_marine_data(self, spot_id):
        """Retrieve marine data for a spot."""
        df = pd.read_sql_query("""
            SELECT timestamp, wave_height, wave_direction, wave_period, sea_level_height_msl
            FROM marine_data 
            WHERE spot_id = ?
            ORDER BY timestamp
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['timestamp'])
            df = df.drop('timestamp', axis=1)
        
        return df
    
    def get_daily_weather(self, spot_id):
        """Retrieve daily weather data for a spot."""
        df = pd.read_sql_query("""
            SELECT date, sunrise, sunset, daylight_duration, temperature_2m_min, temperature_2m_max
            FROM daily_weather 
            WHERE spot_id = ?
            ORDER BY date
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def get_scored_forecast(self, spot_id):
        """Retrieve scored forecast data for a spot."""
        df = pd.read_sql_query("""
            SELECT timestamp, wave_size, wave_direction, wave_period, wind_strength, wind_angle,
                   swell_direction_points, wind_points, wave_height_points, wave_period_points, total_points,
                   surf_rating, wind_relationship, wave_height_ft, conditions_summary
            FROM scored_forecasts 
            WHERE spot_id = ?
            ORDER BY timestamp
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['time'] = pd.to_datetime(df['timestamp'])
            # Keep timestamp for debugging, drop later if needed
        
        return df
    
    def get_half_day_scores(self, spot_id):
        """Retrieve half-day scores for a spot."""
        df = pd.read_sql_query("""
            SELECT date, half_day, avg_total_points
            FROM half_day_scores 
            WHERE spot_id = ?
            ORDER BY date, half_day
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def get_daily_scores(self, spot_id):
        """Retrieve daily scores for a spot."""
        df = pd.read_sql_query("""
            SELECT date, avg_total_points, avg_surf_rating, wind_relationship, conditions_summary
            FROM daily_scores 
            WHERE spot_id = ?
            ORDER BY date
        """, self.conn, params=(spot_id,))
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def close(self):
        """Close database connection."""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 