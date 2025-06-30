#!/usr/bin/env python
"""
Fetch and score surf forecasts for a specific spot.

This script fetches weather and marine data for a specified surf spot,
scores the data using the point-based scoring system, and exports
the results to an Excel file.

Usage:
    python fetch_score_forecast.py --spot "Supertubes"
    python fetch_score_forecast.py --spot "Supertubes" --output "my_forecast.xlsx"
    python fetch_score_forecast.py --list-spots
"""

import sys
import os
import argparse
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data_fetching.openmeteo import fetch_weather_data, fetch_marine_data
from src.utils.surf_spots import surf_spots
from src.utils.excel_export import export_to_excel
from src.scoring.wave_scoring import score_forecast, find_best_sessions
import pandas as pd
import pytz


def fetch_and_score_forecast(spot_name, output_file='surf_data.xlsx'):
    """
    Fetch weather and marine data for a surf spot, score it, and export to Excel.
    
    :param spot_name: Name of the surf spot (must exist in the surf_spots dictionary)
    :param output_file: Name of the output Excel file
    :return: None
    """
    # Verify the spot exists
    if spot_name not in surf_spots:
        print(f"Error: Surf spot '{spot_name}' not found. Run with --list-spots to see available spots.")
        return
    
    print(f"Fetching and scoring forecast for {spot_name}...")
    spot = surf_spots[spot_name]
    local_tz = pytz.timezone(spot['timezone'])

    # Fetch weather and marine data
    latitude = spot['lat']
    longitude = spot['lon']
    
    try:
        hourly_weather, daily_weather = fetch_weather_data(latitude, longitude)
        hourly_marine = fetch_marine_data(latitude, longitude)
    except Exception as e:
        print(f"Error fetching forecast data: {e}")
        return

    # Convert UTC times to local timezone
    hourly_weather_local = hourly_weather.copy()
    hourly_weather_local['date'] = hourly_weather['date'].dt.tz_convert(local_tz)
    
    hourly_marine_local = hourly_marine.copy()
    hourly_marine_local['date'] = hourly_marine['date'].dt.tz_convert(local_tz)
    
    daily_weather_local = daily_weather.copy()
    daily_weather_local['date'] = daily_weather['date'].dt.tz_convert(local_tz)

    # Create merged DataFrame with all required data
    merged_data = pd.DataFrame({
        "time": hourly_weather_local['date'],
        "wave_size": hourly_marine_local['wave_height'],  # Keep in meters
        "wave_direction": hourly_marine_local['wave_direction'],
        "wave_period": hourly_marine_local['wave_period'],
        "wind_strength": hourly_weather_local['wind_speed_10m'],
        "wind_angle": hourly_weather_local['wind_direction_10m']  # Use correct wind direction column
    })

    # Add daylight classification
    merged_data['daylight'] = merged_data['time'].apply(
        lambda x: classify_daylight(x, daily_weather_local)
    )

    # Score the forecast data
    scored_data = score_forecast(merged_data, spot)
    
    # Print info about scoring calculation
    print("\nScoring Information:")
    print("- Points are awarded for each factor: swell direction, wind, wave height, and wave period")
    print("- Total points is the sum of all factor points")
    print("- Overall score is normalized to a 0-10 scale based on the min/max points in the forecast")
    print(f"- Min points in this forecast: {scored_data['total_points'].min():.2f}")
    print(f"- Max points in this forecast: {scored_data['total_points'].max():.2f}")
    
    # Find the best sessions
    best_sessions = find_best_sessions(scored_data, min_points=0)
    print(f"\nFound {len(best_sessions)} potential surfing sessions")
    
    # Print top 5 sessions
    if best_sessions:
        print("\nTop surfing sessions:")
        for i, (start, end, points) in enumerate(best_sessions[:5], 1):
            duration = (end - start).total_seconds() / 3600
            print(f"Session {i}: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%H:%M')}")
            print(f"   Duration: {duration:.1f} hours, Points: {points:.2f}")
            print(f"   Day of week: {start.strftime('%A')}")
            print()
    
    # Export detailed forecast to Excel
    forecast_sheet_name = f'{spot_name} - Forecast'
    export_to_excel(scored_data, output_file, sheet_name=forecast_sheet_name)
    
    # Export daily data with sunrise/sunset times
    available_columns = [col for col in ['date', 'sunrise', 'sunset'] if col in daily_weather_local.columns]
    
    daily_weather_formatted = daily_weather_local.copy()

    if 'sunrise' in daily_weather_local.columns:
        daily_weather_formatted['sunrise'] = pd.to_datetime(
            daily_weather_local['sunrise'], unit='s', utc=True
        ).dt.tz_convert(local_tz).dt.strftime('%H:%M')

    if 'sunset' in daily_weather_local.columns:
        daily_weather_formatted['sunset'] = pd.to_datetime(
            daily_weather_local['sunset'], unit='s', utc=True
        ).dt.tz_convert(local_tz).dt.strftime('%H:%M')

    daily_sheet_name = f'{spot_name} - Daily'
    export_to_excel(
        daily_weather_formatted[available_columns], 
        output_file, 
        sheet_name=daily_sheet_name
    )
    
    # Export session summary
    if best_sessions:
        session_data = []
        for i, (start, end, points) in enumerate(best_sessions, 1):
            duration = (end - start).total_seconds() / 3600
            session_data.append({
                'Rank': i,
                'Start Date': start.strftime('%Y-%m-%d'),
                'Start Time': start.strftime('%H:%M'),
                'End Time': end.strftime('%H:%M'),
                'Duration (hrs)': round(duration, 1),
                'Day': start.strftime('%A'),
                'Points': round(points, 2)
            })
        
        sessions_df = pd.DataFrame(session_data)
        sessions_sheet_name = f'{spot_name} - Best Sessions'
        export_to_excel(sessions_df, output_file, sheet_name=sessions_sheet_name)
    
    print(f"\nForecast data exported to {output_file}")


def classify_daylight(hour, daily_weather):
    """
    Classify whether a given hour is daylight or nighttime.
    
    :param hour: Datetime object representing the hour
    :param daily_weather: DataFrame with daily weather data including sunrise/sunset
    :return: String 'Daylight' or 'Nighttime'
    """
    if 'sunrise' not in daily_weather.columns or 'sunset' not in daily_weather.columns:
        return "Unknown"
        
    # Find the corresponding day
    day = daily_weather[daily_weather['date'].dt.date == hour.date()]
    if not day.empty:
        try:
            sunrise = pd.to_datetime(day['sunrise'].values[0], unit='s', utc=True).tz_convert(hour.tz).time()
            sunset = pd.to_datetime(day['sunset'].values[0], unit='s', utc=True).tz_convert(hour.tz).time()
            if sunrise <= hour.time() <= sunset:
                return "Daylight"
        except Exception:
            pass
    return "Nighttime"


def list_available_spots():
    """Print a list of all available surf spots."""
    print("\nAvailable surf spots:")
    print("-" * 50)
    for name, spot in sorted(surf_spots.items()):
        print(f"{name} - {spot.get('location', 'Unknown location')}")
    print("-" * 50)


def main():
    """Main function to parse command line arguments and run the script."""
    parser = argparse.ArgumentParser(description='Fetch and score surf forecasts')
    parser.add_argument('--spot', type=str, help='Name of the surf spot')
    parser.add_argument('--output', type=str, default='surf_data.xlsx',
                        help='Output Excel file name (default: surf_data.xlsx)')
    parser.add_argument('--list-spots', action='store_true', 
                        help='List all available surf spots')
    
    args = parser.parse_args()
    
    if args.list_spots:
        list_available_spots()
        return
    
    if not args.spot:
        print("Error: Please specify a surf spot with --spot or use --list-spots to see available options.")
        return
    
    fetch_and_score_forecast(args.spot, args.output)


if __name__ == "__main__":
    main() 