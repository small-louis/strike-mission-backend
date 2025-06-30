import openmeteo_requests
import pandas as pd
import requests
from retry_requests import retry

# Setup the Open-Meteo API client with retry but no cache (to avoid timezone issues)
session = requests.Session()
retry_session = retry(session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_weather_data(latitude, longitude, start_date=None, end_date=None):
    """
    Fetch weather data for a given latitude and longitude using the Open-Meteo API.

    :param latitude: Latitude of the location.
    :param longitude: Longitude of the location.
    :param start_date: Start date for filtering data.
    :param end_date: End date for filtering data.
    :return: Tuple of DataFrames containing hourly and daily weather data.
    """
    # Define the API endpoint and parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ["sunset", "sunrise", "daylight_duration", "temperature_2m_min", "temperature_2m_max"],
        "hourly": ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
        "models": ["gfs_seamless"],
        "wind_speed_unit": "kn",
        "forecast_days": 16  # Request up to 16 days of forecast data
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process the response
    response = responses[0]

    # Process hourly data
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(1).ValuesAsNumpy(),
        "wind_direction_10m": hourly.Variables(2).ValuesAsNumpy(),
        "wind_gusts_10m": hourly.Variables(3).ValuesAsNumpy()
    }
    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Process daily data
    daily = response.Daily()
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "sunset": daily.Variables(0).ValuesInt64AsNumpy(),
        "sunrise": daily.Variables(1).ValuesInt64AsNumpy(),
        "daylight_duration": daily.Variables(2).ValuesAsNumpy(),
        "temperature_2m_min": daily.Variables(3).ValuesAsNumpy(),
        "temperature_2m_max": daily.Variables(4).ValuesAsNumpy()
    }
    daily_dataframe = pd.DataFrame(data=daily_data)

    # Filter data by date range if specified
    if start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Handle timezone compatibility
        if hourly_dataframe['date'].dt.tz is not None:
            # If DataFrame dates are timezone-aware, make comparison dates timezone-aware too
            if start_date.tz is None:
                start_date = start_date.tz_localize(hourly_dataframe['date'].dt.tz)
            if end_date.tz is None:
                end_date = end_date.tz_localize(hourly_dataframe['date'].dt.tz)
        
        hourly_dataframe = hourly_dataframe[(hourly_dataframe['date'] >= start_date) & (hourly_dataframe['date'] <= end_date)]
        daily_dataframe = daily_dataframe[(daily_dataframe['date'] >= start_date) & (daily_dataframe['date'] <= end_date)]

    # Print info about the data range (fixed to avoid indexing error)
    start_time = pd.to_datetime(hourly.Time(), unit="s")
    end_time = pd.to_datetime(hourly.TimeEnd(), unit="s")
    print(f"Weather data range: {start_time} to {end_time}")
    print(f"Total weather forecast days: {(end_time - start_time).total_seconds() / (24*3600):.1f}")

    return hourly_dataframe, daily_dataframe


def fetch_marine_data(latitude, longitude, start_date=None, end_date=None):
    """
    Fetch marine data for a given latitude and longitude using the Open-Meteo API.

    :param latitude: Latitude of the location.
    :param longitude: Longitude of the location.
    :param start_date: Start date for filtering data.
    :param end_date: End date for filtering data.
    :return: DataFrame containing hourly marine data including tide information.
    """
    # Define the API endpoint and parameters
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["wave_height", "wave_direction", "wave_period", "sea_level_height_msl"],
        "models": ["ncep_gfswave025"],
        "forecast_days": 16  # Request up to 16 days of forecast data
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process the response
    response = responses[0]

    # Process hourly marine data
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "wave_height": hourly.Variables(0).ValuesAsNumpy(),
        "wave_direction": hourly.Variables(1).ValuesAsNumpy(),
        "wave_period": hourly.Variables(2).ValuesAsNumpy(),
        "sea_level_height_msl": hourly.Variables(3).ValuesAsNumpy()
    }
    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Filter data by date range if specified
    if start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Handle timezone compatibility  
        if hourly_dataframe['date'].dt.tz is not None:
            # If DataFrame dates are timezone-aware, make comparison dates timezone-aware too
            if start_date.tz is None:
                start_date = start_date.tz_localize(hourly_dataframe['date'].dt.tz)
            if end_date.tz is None:
                end_date = end_date.tz_localize(hourly_dataframe['date'].dt.tz)
        
        hourly_dataframe = hourly_dataframe[(hourly_dataframe['date'] >= start_date) & (hourly_dataframe['date'] <= end_date)]

    # Print info about the data range (fixed to avoid indexing error)
    start_time = pd.to_datetime(hourly.Time(), unit="s")
    end_time = pd.to_datetime(hourly.TimeEnd(), unit="s")
    print(f"Marine data range: {start_time} to {end_time}")
    print(f"Total marine forecast days: {(end_time - start_time).total_seconds() / (24*3600):.1f}")

    return hourly_dataframe 