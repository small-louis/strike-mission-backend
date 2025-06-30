import pandas as pd

def merge_weather_and_marine_data(hourly_weather, hourly_marine):
    """
    Merge weather and marine data into a single dataframe.
    
    :param hourly_weather: DataFrame containing hourly weather data
    :param hourly_marine: DataFrame containing hourly marine data
    :return: DataFrame with merged data
    """
    # Create a new DataFrame with the merged data
    merged_data = pd.DataFrame({
        "time": hourly_weather['date'],
        # Keep wave height in meters (conversion to feet happens in scoring function)
        "wave_size": hourly_marine['wave_height'],
        "wave_direction": hourly_marine['wave_direction'],
        "wave_period": hourly_marine['wave_period'],
        "wind_strength": hourly_weather['wind_speed_10m'],
        "wind_angle": hourly_weather['wind_direction_10m']
    })
    
    return merged_data


def process_spot_data(spot_name, spot, hourly_weather, daily_weather, hourly_marine, score_forecast_func, half_day_score_func=None):
    """
    Process the data for a given surf spot.
    
    This includes:
    1. Merging weather and marine data
    2. Scoring the data
    3. Calculating half-day scores
    
    :param spot_name: Name of the surf spot
    :param spot: Dictionary with spot details
    :param hourly_weather: DataFrame with hourly weather data
    :param daily_weather: DataFrame with daily weather data
    :param hourly_marine: DataFrame with hourly marine data
    :param score_forecast_func: Function to score the forecast
    :param half_day_score_func: Optional function to calculate half-day scores
    :return: Tuple of (scored_data, half_day_scores, daily_weather)
    """
    # Merge the data
    merged_data = merge_weather_and_marine_data(hourly_weather, hourly_marine)
    
    # Score the data
    scored_data = score_forecast_func(merged_data, spot)
    
    # Calculate half-day scores if function is provided
    half_day_scores = None
    if half_day_score_func:
        half_day_scores = half_day_score_func(scored_data, spot_name, daily_weather)
        return scored_data, half_day_scores, daily_weather
    
    return scored_data, daily_weather 