import pandas as pd
from datetime import time
import pytz


def classify_half_day(timestamp):
    """
    Classify a timestamp as morning (before 1pm) or afternoon (after 1pm).
    
    :param timestamp: Pandas Timestamp
    :return: 'morning' or 'afternoon'
    """
    if timestamp.time() < time(13, 0):  # Before 1pm
        return 'morning'
    else:
        return 'afternoon'


def is_daylight(timestamp, daylight_times):
    """
    Check if a timestamp is during daylight hours.
    
    :param timestamp: Pandas Timestamp to check
    :param daylight_times: Dictionary mapping dates to sunrise/sunset times
    :return: Boolean indicating if it's daylight
    """
    # Convert timestamp to date for lookup
    date = timestamp.date()
    
    # Check if we have daylight data for this date
    if date not in daylight_times:
        return True  # Default to True if we don't have data
    
    sunrise = daylight_times[date]['sunrise']
    sunset = daylight_times[date]['sunset']
    
    return sunrise <= timestamp.time() <= sunset


def score_half_days(scored_data, spot_name, daily_weather):
    """
    Calculate average total points for each half-day period during daylight hours.
    
    :param scored_data: DataFrame with scored surf conditions
    :param spot_name: Name of the surf spot
    :param daily_weather: DataFrame containing sunrise and sunset times
    :return: DataFrame with half-day scores
    """
    # Create a copy to avoid modifying the original
    half_day_data = scored_data.copy()
    daily_weather = daily_weather.copy()
    
    # Add half-day classification
    half_day_data['half_day'] = half_day_data['time'].apply(classify_half_day)
    
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
    half_day_data['is_daylight'] = half_day_data['time'].apply(
        lambda x: is_daylight(x, daylight_times)
    )
    
    # Only consider daylight hours
    half_day_data = half_day_data[half_day_data['is_daylight']]
    
    # Check if we have data after filtering
    if half_day_data.empty:
        print(f"Warning: No daylight data for {spot_name}")
        return pd.DataFrame(columns=['date', 'half_day', 'avg_total_points'])
    
    # Group by date and half-day, calculate average total points
    half_day_scores = half_day_data.groupby([
        half_day_data['time'].dt.date,
        'half_day'
    ])['total_points'].mean().round(2)
    
    # Reset index and rename columns
    half_day_scores = half_day_scores.reset_index()
    half_day_scores = half_day_scores.rename(columns={
        'time': 'date',
        'total_points': 'avg_total_points'
    })
    
    # Sort by date and half-day
    half_day_scores = half_day_scores.sort_values(['date', 'half_day'])
    
    return half_day_scores


def get_half_day_scores(scored_data, spot_name, daily_weather):
    """
    Get half-day scores for a surf spot during daylight hours.
    
    :param scored_data: DataFrame with scored surf conditions
    :param spot_name: Name of the surf spot
    :param daily_weather: DataFrame containing sunrise and sunset times
    :return: DataFrame with half-day scores
    """
    # Ensure time column is datetime
    if not pd.api.types.is_datetime64_any_dtype(scored_data['time']):
        scored_data['time'] = pd.to_datetime(scored_data['time'])
    
    # Calculate half-day scores
    half_day_scores = score_half_days(scored_data, spot_name, daily_weather)
    
    return half_day_scores 