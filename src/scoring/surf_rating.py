"""
Surf condition rating system based on wave size, period, and wind conditions.
Converts numerical scores into descriptive ratings for better user experience.
"""

import pandas as pd
import numpy as np


def get_wind_relationship(wind_direction, spot_config):
    """
    Determine if wind is favorable or unfavorable based on the spot's configured wind direction range.
    
    :param wind_direction: Wind direction in degrees (0-360)
    :param spot_config: Spot configuration dictionary with 'wind_dir_range' tuple
    :return: String indicating wind relationship ('favorable', 'unfavorable')
    """
    if not spot_config or 'wind_dir_range' not in spot_config:
        return 'unfavorable'  # Default to unfavorable if no config
    
    wind_range = spot_config['wind_dir_range']
    min_wind, max_wind = wind_range
    
    # Handle wind ranges that cross 0/360 degrees (e.g., 340-60)
    if min_wind > max_wind:
        # Range crosses 0 degrees (e.g., 340-60 means 340-360 and 0-60)
        is_favorable = (wind_direction >= min_wind) or (wind_direction <= max_wind)
    else:
        # Normal range (e.g., 45-135)
        is_favorable = min_wind <= wind_direction <= max_wind
    
    return 'favorable' if is_favorable else 'unfavorable'


def calculate_surf_rating(wave_height, wave_period, wind_speed, wind_direction, spot_config=None):
    """
    Calculate surf rating based on wave conditions and wind using spot-specific wind direction ranges.
    
    :param wave_height: Wave height in meters
    :param wave_period: Wave period in seconds
    :param wind_speed: Wind speed in knots
    :param wind_direction: Wind direction in degrees
    :param spot_config: Spot configuration dictionary with wind_dir_range
    :return: Dictionary with rating and wind relationship
    """
    # Convert wave height to feet for rating calculation
    wave_height_ft = wave_height * 3.28084
    
    # Determine wind relationship using spot-specific wind direction ranges
    wind_relationship = get_wind_relationship(wind_direction, spot_config)
    
    # Apply rating logic based on wind conditions
    if wind_relationship == 'favorable':
        # Favorable wind conditions (within spot's preferred wind direction range)
        rating = _get_favorable_rating(wave_height_ft, wave_period)
    else:
        # Unfavorable wind conditions (outside spot's preferred wind direction range)
        rating = _get_unfavorable_rating(wave_height_ft, wave_period, wind_speed)
    
    return {
        'rating': rating,
        'wind_relationship': wind_relationship,
        'wave_height_ft': round(wave_height_ft, 1),
        'conditions_summary': f"{rating} - {wind_relationship} {wind_speed:.0f}kts"
    }


def _get_favorable_rating(wave_height_ft, wave_period):
    """
    Get rating for favorable wind conditions (within spot's preferred wind direction range).
    
    :param wave_height_ft: Wave height in feet
    :param wave_period: Wave period in seconds
    :return: Rating string
    """
    if wave_height_ft < 1:
        return "No surf"
    elif wave_height_ft < 3:
        return "Small"
    elif wave_height_ft >= 7 and wave_period > 19:
        return "Epic"
    elif wave_height_ft >= 7 and wave_period > 15:
        return "Firing"
    elif wave_height_ft > 5 and wave_period > 13:
        return "Pumping"
    elif wave_height_ft >= 3 and wave_period > 11:
        return "Good"
    elif wave_height_ft >= 3 and wave_period < 11:
        return "Fun"
    elif wave_height_ft >= 3 and wave_period < 9:
        return "Fair"
    else:
        return "Small"


def _get_unfavorable_rating(wave_height_ft, wave_period, wind_speed):
    """
    Get rating for unfavorable wind conditions (outside spot's preferred wind direction range).
    
    :param wave_height_ft: Wave height in feet
    :param wave_period: Wave period in seconds
    :param wind_speed: Wind speed in knots
    :return: Rating string
    """
    if wave_height_ft < 3 and wave_period < 8:
        return "Slop"
    elif 3 <= wave_height_ft <= 5 and 8 <= wave_period <= 12:
        return "Mush"
    elif wave_height_ft >= 3 and wave_period > 12:
        return "Messy"
    else:
        return "Meh"


def add_ratings_to_forecast(forecast_df, spot_config=None):
    """
    Add surf ratings to a forecast DataFrame.
    
    :param forecast_df: DataFrame with wave and wind data
    :param spot_config: Spot configuration dictionary
    :return: DataFrame with added rating columns
    """
    if forecast_df.empty:
        return forecast_df
    
    # Ensure required columns exist - check for both column name formats
    wave_col = 'wave_size' if 'wave_size' in forecast_df.columns else 'wave_height'
    wind_speed_col = 'wind_strength' if 'wind_strength' in forecast_df.columns else 'wind_speed_10m'
    wind_dir_col = 'wind_angle' if 'wind_angle' in forecast_df.columns else 'wind_direction_10m'
    
    required_cols = [wave_col, 'wave_period', wind_speed_col, wind_dir_col]
    missing_cols = [col for col in required_cols if col not in forecast_df.columns]
    
    if missing_cols:
        print(f"Warning: Missing columns for rating calculation: {missing_cols}")
        # Add default rating columns
        forecast_df['surf_rating'] = 'Unknown'
        forecast_df['wind_relationship'] = 'unknown'
        forecast_df['conditions_summary'] = 'Data unavailable'
        return forecast_df
    
    # Calculate ratings for each row
    ratings_data = []
    for _, row in forecast_df.iterrows():
        rating_info = calculate_surf_rating(
            wave_height=row[wave_col],
            wave_period=row['wave_period'],
            wind_speed=row[wind_speed_col],
            wind_direction=row[wind_dir_col],
            spot_config=spot_config
        )
        ratings_data.append(rating_info)
    
    # Add rating columns to DataFrame
    ratings_df = pd.DataFrame(ratings_data)
    forecast_df['surf_rating'] = ratings_df['rating']
    forecast_df['wind_relationship'] = ratings_df['wind_relationship']
    forecast_df['wave_height_ft'] = ratings_df['wave_height_ft']
    forecast_df['conditions_summary'] = ratings_df['conditions_summary']
    
    return forecast_df


def get_rating_score(rating):
    """
    Convert rating to numerical score for sorting/comparison.
    
    :param rating: Rating string
    :return: Numerical score (higher = better)
    """
    rating_scores = {
        'Epic': 10,
        'Firing': 9,
        'Pumping': 8,
        'Good': 7,
        'Fun': 6,
        'Fair': 5,
        'Small': 3,
        'Messy': 3,
        'Mush': 2,
        'Slop': 1,
        'Meh': 1,
        'No surf': 0,
        'Unknown': 0
    }
    return rating_scores.get(rating, 0)


if __name__ == "__main__":
    # Test the rating system
    test_conditions = [
        (2.1, 14, 8, 270),  # 7ft, 14s, 8kts offshore
        (1.5, 8, 15, 90),   # 5ft, 8s, 15kts onshore
        (0.6, 6, 5, 180),   # 2ft, 6s, 5kts
    ]
    
    print("Testing surf rating system:")
    for wave_h, wave_p, wind_s, wind_d in test_conditions:
        rating = calculate_surf_rating(wave_h, wave_p, wind_s, wind_d)
        print(f"Wave: {wave_h}m ({rating['wave_height_ft']}ft), Period: {wave_p}s, "
              f"Wind: {wind_s}kts @ {wind_d}° -> {rating['rating']} ({rating['wind_relationship']})") 