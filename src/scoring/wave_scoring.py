import numpy as np
import pandas as pd
from .surf_rating import add_ratings_to_forecast


def score_swell_direction(wave_direction, ideal_range):
    """
    Score the swell direction based on the angle to the ideal range.
    
    :param wave_direction: The direction of the wave in degrees
    :param ideal_range: Tuple containing the min and max of the ideal swell direction range
    :return: Points awarded based on angle
    """
    min_dir, max_dir = ideal_range
    
    # Check if the direction is within the ideal range
    if max_dir < min_dir:  # Crosses the 0/360 boundary
        if wave_direction >= min_dir or wave_direction <= max_dir:
            return 0  # Direct angle (0 points)
    else:
        if min_dir <= wave_direction <= max_dir:
            return 0  # Direct angle (0 points)
    
    # Check if it's semi-direct (close to the ideal range)
    buffer = 30  # degrees buffer for semi-direct
    if max_dir < min_dir:  # Crosses the 0/360 boundary
        if (wave_direction >= min_dir - buffer or wave_direction <= max_dir + buffer) and not (wave_direction >= min_dir or wave_direction <= max_dir):
            return -1  # Semi-direct (-1 point)
    else:
        if (min_dir - buffer <= wave_direction <= max_dir + buffer) and not (min_dir <= wave_direction <= max_dir):
            return -1  # Semi-direct (-1 point)
    
    # Out of window
    return -10  # Out of window (-10 points)


def score_wind_direction_speed(wind_direction, wind_speed, ideal_range):
    """
    Score the wind based on direction and speed.
    
    :param wind_direction: The direction of the wind in degrees
    :param wind_speed: Wind speed in knots
    :param ideal_range: Tuple containing the min and max of the ideal wind direction range
    :return: Points awarded based on wind conditions
    """
    min_dir, max_dir = ideal_range
    
    # Determine if the wind is from a favorable direction
    favorable = False
    if max_dir < min_dir:  # Crosses the 0/360 boundary
        if wind_direction >= min_dir or wind_direction <= max_dir:
            favorable = True
    else:
        if min_dir <= wind_direction <= max_dir:
            favorable = True
            
    # Score based on direction and speed
    if favorable:
        if 5 <= wind_speed <= 12:
            return 2
        elif 12 < wind_speed <= 20:
            return 1
        elif 20 < wind_speed <= 30:
            return 0
        elif 30 < wind_speed <= 40:
            return -1
        elif wind_speed > 40:
            return -3
        else:  # wind_speed < 5
            return 2  # Assuming light favorable winds are good
    else:
        if wind_speed < 3:
            return 1
        elif 3 <= wind_speed <= 6:
            return 0
        elif 6 < wind_speed <= 10:
            return -1
        elif 10 < wind_speed <= 20:
            return -4
        else:  # wind_speed > 20
            return -6


def score_wave_period(period):
    """
    Score the wave period based on seconds.
    
    :param period: Wave period in seconds
    :return: Points awarded based on period
    """
    if period < 6:
        return -4
    elif 6 <= period < 8:
        return -2
    elif 8 <= period < 10:
        return -1
    elif 10 <= period < 11.5:
        return 0
    elif 11.5 <= period < 14:
        return 1
    else:  # period >= 14
        return 2


def score_wave_height(wave_height):
    """
    Score the wave height.
    
    :param wave_height: Wave height in meters
    :return: Points awarded based on height
    """
    # Convert meters to feet (approximate conversion)
    wave_height_ft = wave_height * 3.28084
    
    if wave_height_ft < 1:
        return 1
    elif 1 <= wave_height_ft < 2:
        return 2
    elif 2 <= wave_height_ft < 3:
        return 3
    elif 3 <= wave_height_ft < 5:
        return 4
    else:  # wave_height_ft >= 5
        return 5


def score_forecast(merged_data, spot_details):
    """
    Add scoring columns to the forecast dataframe using the point-based system.
    
    This function scores surf forecasts by calculating individual points for each factor:
    - Swell direction
    - Wind direction and speed
    - Wave height
    - Wave period
    
    Additionally, it now includes descriptive surf ratings based on wave conditions and wind.
    
    :param merged_data: DataFrame containing merged weather and marine data
    :param spot_details: Dictionary with spot-specific parameters
    :return: DataFrame with additional scoring columns and surf ratings
    """
    # Create a copy to avoid modifying the original
    scored_data = merged_data.copy()
    
    # Initialize score columns
    scored_data['swell_direction_points'] = 0
    scored_data['wind_points'] = 0
    scored_data['wave_height_points'] = 0
    scored_data['wave_period_points'] = 0
    
    # Score individual components
    if 'wave_direction' in scored_data.columns:
        scored_data['swell_direction_points'] = scored_data['wave_direction'].apply(
            lambda x: score_swell_direction(x, spot_details['swell_dir_range'])
        )
    
    # Handle wind direction and speed together
    if 'wind_angle' in scored_data.columns and 'wind_strength' in scored_data.columns:
        scored_data['wind_points'] = scored_data.apply(
            lambda x: score_wind_direction_speed(
                x['wind_angle'], 
                x['wind_strength'], 
                spot_details['wind_dir_range']
            ), 
            axis=1
        )
    
    if 'wave_size' in scored_data.columns:
        scored_data['wave_height_points'] = scored_data['wave_size'].apply(score_wave_height)
    
    if 'wave_period' in scored_data.columns:
        scored_data['wave_period_points'] = scored_data['wave_period'].apply(score_wave_period)
    
    # Calculate total points as the sum of all individual factor points
    point_columns = [
        'swell_direction_points', 
        'wind_points', 
        'wave_height_points', 
        'wave_period_points'
    ]
    
    available_columns = [col for col in point_columns if col in scored_data.columns]
    if available_columns:
        raw_total = scored_data[available_columns].sum(axis=1)
        # Bound total points between 1 and 10
        scored_data['total_points'] = raw_total.clip(lower=1, upper=10)
    
    # Add descriptive surf ratings based on wave conditions and wind
    try:
        scored_data = add_ratings_to_forecast(scored_data, spot_details)
        print(f"✓ Added surf ratings to forecast")
    except Exception as e:
        print(f"Warning: Could not add surf ratings: {e}")
        # Add default rating columns if rating system fails
        scored_data['surf_rating'] = 'Unknown'
        scored_data['wind_relationship'] = 'unknown'
        scored_data['conditions_summary'] = 'Rating unavailable'
    
    return scored_data 