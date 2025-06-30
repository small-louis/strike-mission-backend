import pandas as pd
from datetime import time
import pytz


def is_daylight(timestamp, daylight_times):
    """
    Check if a timestamp is during daylight hours.
    
    :param timestamp: Pandas Timestamp
    :param daylight_times: Dictionary mapping date to sunrise/sunset times
    :return: Boolean indicating if timestamp is during daylight
    """
    date_key = timestamp.date()
    
    if date_key not in daylight_times:
        # Default to 6am-6pm if no sunrise/sunset data
        return time(6, 0) <= timestamp.time() <= time(18, 0)
    
    sunrise = daylight_times[date_key]['sunrise']
    sunset = daylight_times[date_key]['sunset']
    
    return sunrise <= timestamp.time() <= sunset


def score_daily_averages(scored_data, spot_name, daily_weather):
    """
    Calculate average total points for each full day during daylight hours.
    
    :param scored_data: DataFrame with scored surf conditions
    :param spot_name: Name of the surf spot
    :param daily_weather: DataFrame containing sunrise and sunset times
    :return: DataFrame with daily average scores
    """
    # Create a copy to avoid modifying the original
    daily_data = scored_data.copy()
    daily_weather = daily_weather.copy()
    
    # Make sure date column in daily_weather is a date
    daily_weather['date'] = pd.to_datetime(daily_weather['date']).dt.date
    
    # Convert sunrise/sunset times to time objects
    daily_weather['sunrise'] = pd.to_datetime(daily_weather['sunrise'], unit='s').dt.time
    daily_weather['sunset'] = pd.to_datetime(daily_weather['sunset'], unit='s').dt.time
    
    # Create a mapping of date to sunrise/sunset times
    daylight_times = {}
    for _, row in daily_weather.iterrows():
        daylight_times[row['date']] = {
            'sunrise': row['sunrise'],
            'sunset': row['sunset']
        }
    
    # Filter for daylight hours
    daily_data['is_daylight'] = daily_data['time'].apply(
        lambda x: is_daylight(x, daylight_times)
    )
    
    # Only consider daylight hours
    daily_data = daily_data[daily_data['is_daylight']]
    
    # Check if we have data after filtering
    if daily_data.empty:
        print(f"Warning: No daylight data for {spot_name}")
        return pd.DataFrame(columns=['date', 'avg_total_points', 'avg_surf_rating', 'conditions_summary'])
    
    # Group by date and calculate averages
    daily_scores = daily_data.groupby(daily_data['time'].dt.date).agg({
        'total_points': 'mean',
        'surf_rating': lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown',
        'wind_relationship': lambda x: x.mode().iloc[0] if not x.mode().empty else 'unknown',
        'conditions_summary': lambda x: x.mode().iloc[0] if not x.mode().empty else 'N/A'
    }).round(2)
    
    # Reset index and rename columns
    daily_scores = daily_scores.reset_index()
    daily_scores = daily_scores.rename(columns={
        'time': 'date',
        'total_points': 'avg_total_points',
        'surf_rating': 'avg_surf_rating'
    })
    
    # Sort by date
    daily_scores = daily_scores.sort_values('date')
    
    return daily_scores


def get_daily_scores(scored_data, spot_name, daily_weather):
    """
    Get daily average scores for a surf spot during daylight hours.
    
    :param scored_data: DataFrame with scored surf conditions
    :param spot_name: Name of the surf spot
    :param daily_weather: DataFrame containing sunrise and sunset times
    :return: DataFrame with daily average scores
    """
    # Ensure time column is datetime
    if not pd.api.types.is_datetime64_any_dtype(scored_data['time']):
        scored_data['time'] = pd.to_datetime(scored_data['time'])
    
    # Calculate daily scores
    daily_scores = score_daily_averages(scored_data, spot_name, daily_weather)
    
    return daily_scores


def cache_daily_scores(spot_name, daily_scores, db_manager):
    """
    Cache daily scores in the database.
    
    :param spot_name: Name of the surf spot
    :param daily_scores: DataFrame with daily scores
    :param db_manager: Database manager instance
    """
    spot_id = spot_name.lower().replace(' ', '_')
    
    try:
        db_manager.store_daily_scores(spot_id, daily_scores)
        print(f"✓ Cached daily scores for {spot_name}")
    except Exception as e:
        print(f"✗ Failed to cache daily scores for {spot_name}: {e}") 