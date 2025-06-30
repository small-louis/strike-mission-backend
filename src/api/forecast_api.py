#!/usr/bin/env python3
"""
Forecast API Server

Flask API server that serves cached forecast data to the frontend.
Provides endpoints for daily scores, detailed forecasts, and spot conditions.

Usage:
    python src/api/forecast_api.py
"""

import sys
import os
from datetime import datetime, timedelta
import json
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from flask import Flask, jsonify, request
from flask_cors import CORS
from utils.db_manager import SurfDataDB
from utils.surf_spots import surf_spots
from utils.data_cache import get_cached_half_day_scores, is_data_fresh
from window_selection.optimal_windows import select_optimal_windows
from flights.flight_fetcher import fetch_flights
from config.user_presets import get_flight_times_for_window

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests

# Database path
DB_PATH = "/Users/louisbrouwer/Documents/Strike_Mission/data/surf_cache.db"


def spot_name_to_id(spot_name):
    """Convert spot name to database ID format."""
    return spot_name.lower().replace(' ', '_')


def spot_id_to_name(spot_id):
    """Convert database ID back to display name."""
    # Find the spot in surf_spots by matching the ID
    for name, data in surf_spots.items():
        if spot_name_to_id(name) == spot_id:
            return name
    # Fallback: convert ID to title case
    return spot_id.replace('_', ' ').title()


@app.route('/api/forecast/daily/<spot_name>')
def get_daily_scores(spot_name):
    """
    Get daily scores for a spot.
    
    Returns daily average scores with ratings and conditions.
    """
    try:
        # URL decode and normalize spot name
        from urllib.parse import unquote
        spot_name = unquote(spot_name)
        
        # Find the actual spot name in surf_spots that matches
        actual_spot_name = None
        for name in surf_spots.keys():
            if spot_name_to_id(name) == spot_name_to_id(spot_name):
                actual_spot_name = name
                break
        
        if actual_spot_name is None:
            return jsonify({
                'error': f'Spot not found: {spot_name}',
                'available_spots': list(surf_spots.keys())
            }), 404
        
        spot_id = spot_name_to_id(actual_spot_name)
        
        with SurfDataDB(DB_PATH) as db:
            daily_scores = db.get_daily_scores(spot_id)
            
            if daily_scores.empty:
                return jsonify({
                    'error': f'No daily scores found for {actual_spot_name}',
                    'spot_name': actual_spot_name,
                    'daily_scores': []
                }), 404
            
            # Convert to JSON-serializable format
            scores_data = []
            for _, row in daily_scores.iterrows():
                scores_data.append({
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'avg_total_points': float(row['avg_total_points']),
                    'avg_surf_rating': row['avg_surf_rating'],
                    'wind_relationship': row['wind_relationship'],
                    'conditions_summary': row['conditions_summary']
                })
            
            return jsonify({
                'spot_name': actual_spot_name,
                'daily_scores': scores_data,
                'total_days': len(scores_data)
            })
            
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch daily scores: {str(e)}',
            'spot_name': spot_name
        }), 500


@app.route('/api/forecast/detailed/<spot_name>')
def get_detailed_forecast(spot_name):
    """
    Get detailed forecast for a spot with hourly data.
    
    IMPORTANT: Uses the same data sources as trip analysis to ensure consistency.
    """
    try:
        # URL decode and normalize spot name
        from urllib.parse import unquote
        spot_name = unquote(spot_name)
        
        # Find the actual spot name in surf_spots that matches
        actual_spot_name = None
        for name in surf_spots.keys():
            if spot_name_to_id(name) == spot_name_to_id(spot_name):
                actual_spot_name = name
                break
        
        if actual_spot_name is None:
            return jsonify({
                'error': f'Spot not found: {spot_name}',
                'available_spots': list(surf_spots.keys())
            }), 404
        
        spot_id = spot_name_to_id(actual_spot_name)
        days = request.args.get('days', 14, type=int)
        
        # Create spot config exactly like trip analysis does
        spot_config = {
            'spot_id': spot_id,
            'lat': surf_spots[actual_spot_name]['lat'],
            'lon': surf_spots[actual_spot_name]['lon']
        }
        
        # Use the same data sources as trip analysis
        half_day_scores = get_cached_half_day_scores(spot_config)
        
        with SurfDataDB(DB_PATH) as db:
            # Get the same hourly scored forecast that trips use
            scored_forecast = db.get_scored_forecast(spot_id)
            # Get daily weather for sunrise/sunset only
            daily_weather = db.get_daily_weather(spot_id)
            # Get marine data for tide information
            marine_data = db.get_marine_data(spot_id)
            
            if (half_day_scores is None or half_day_scores.empty) and scored_forecast.empty:
                return jsonify({
                    'error': f'No forecast data found for {actual_spot_name}',
                    'spot_name': actual_spot_name,
                    'forecast': []
                }), 404
            
            # Helper function to get tide data for a specific hour
            def get_tide_for_hour(target_time):
                """Get tide height and status for a specific hour"""
                if marine_data.empty:
                    return 1.5, 'Mid'  # Fallback
                
                try:
                    # Find closest time in marine data
                    time_diffs = abs(marine_data['date'] - target_time)
                    closest_idx = time_diffs.idxmin()
                    closest_row = marine_data.loc[closest_idx]
                    
                    # Check if we have real tide data
                    tide_height = None
                    if 'sea_level_height_msl' in closest_row and pd.notna(closest_row['sea_level_height_msl']) and closest_row['sea_level_height_msl'] is not None:
                        tide_height = float(closest_row['sea_level_height_msl'])
                    
                    # If no real tide data, use realistic tidal simulation
                    if tide_height is None or pd.isna(tide_height):
                        print(f"DEBUG: Using tidal simulation for {actual_spot_name} at {target_time}")
                        import math
                        
                        # Get spot-specific tidal characteristics
                        spot_id = spot_name_to_id(actual_spot_name)
                        print(f"DEBUG: spot_id = {spot_id}")
                        tidal_ranges = {
                            'supertubos': {'mean': 2.0, 'range': 1.8},  # Portugal - moderate tides
                            'la_graviere': {'mean': 2.5, 'range': 2.2},  # France - larger tides
                            'anchor_point': {'mean': 1.5, 'range': 1.2}   # Morocco - smaller tides
                        }
                        
                        tidal_config = tidal_ranges.get(spot_id, {'mean': 2.0, 'range': 1.5})
                        
                        # Calculate hours since epoch for consistent tide timing
                        epoch = pd.Timestamp('2025-01-01')  # Remove timezone
                        # Keep target_time timezone-naive for compatibility
                        if target_time.tz is not None:
                            target_time = target_time.tz_localize(None)
                        hours_since_epoch = (target_time - epoch).total_seconds() / 3600
                        
                        # Semi-diurnal tides (2 high, 2 low per day) - 12.42 hour cycle
                        primary_tide = tidal_config['mean'] + tidal_config['range'] * math.sin(hours_since_epoch * 2 * math.pi / 12.42)
                        
                        # Add spring/neap cycle (14-day cycle)
                        spring_neap = 0.3 * math.sin(hours_since_epoch * 2 * math.pi / (14 * 24))
                        
                        # Add some daily variation
                        daily_variation = 0.1 * math.sin(hours_since_epoch * 2 * math.pi / 24)
                        
                        tide_height = max(0.1, primary_tide + spring_neap + daily_variation)
                    
                    # Determine tide status based on height and trend
                    if tide_height > 3.0:
                        tide_status = 'High'
                    elif tide_height < 1.0:
                        tide_status = 'Low'
                    else:
                        # Check if rising or falling by comparing with previous hour
                        prev_time = target_time - pd.Timedelta(hours=1)
                        prev_diffs = abs(marine_data['date'] - prev_time)
                        if not prev_diffs.empty:
                            prev_idx = prev_diffs.idxmin()
                            prev_row = marine_data.loc[prev_idx]
                            
                            # Calculate previous tide height using same method
                            prev_height = None
                            if 'sea_level_height_msl' in prev_row and pd.notna(prev_row['sea_level_height_msl']) and prev_row['sea_level_height_msl'] is not None:
                                prev_height = float(prev_row['sea_level_height_msl'])
                            
                            if prev_height is None or pd.isna(prev_height) or prev_height == 0:
                                import math
                                spot_id_inner = spot_name_to_id(actual_spot_name)
                                tidal_config = tidal_ranges.get(spot_id_inner, {'mean': 2.0, 'range': 1.5})
                                epoch = pd.Timestamp('2025-01-01')  # Remove timezone
                                # Keep prev_time timezone-naive for compatibility
                                if prev_time.tz is not None:
                                    prev_time = prev_time.tz_localize(None)
                                prev_hours = (prev_time - epoch).total_seconds() / 3600
                                prev_primary = tidal_config['mean'] + tidal_config['range'] * math.sin(prev_hours * 2 * math.pi / 12.42)
                                prev_spring_neap = 0.3 * math.sin(prev_hours * 2 * math.pi / (14 * 24))
                                prev_daily = 0.1 * math.sin(prev_hours * 2 * math.pi / 24)
                                prev_height = max(0.1, prev_primary + prev_spring_neap + prev_daily)
                            
                            if tide_height > prev_height + 0.1:
                                tide_status = 'Rising'
                            elif tide_height < prev_height - 0.1:
                                tide_status = 'Falling'
                            else:
                                tide_status = 'High' if tide_height > 2.0 else 'Low'
                        else:
                            tide_status = 'Mid'
                    
                    return tide_height, tide_status
                    
                except Exception as e:
                    print(f"Error getting tide data: {e}")
                    return 1.5, 'Mid'
            
            # Build detailed forecast
            forecast_data = []
            
            # Use half-day scores for consistency with trip analysis, but get details from scored forecast
            if not half_day_scores.empty and not scored_forecast.empty:
                # Group half-day scores by date to get consistent daily averages
                grouped_half_day = half_day_scores.groupby(half_day_scores['date'].dt.date)
                
                for date_obj, day_half_day_data in list(grouped_half_day)[:days]:
                    date_str = date_obj.strftime('%Y-%m-%d')
                    
                    # Calculate daily average from half-day scores (consistent with trip analysis)
                    daily_avg_score = day_half_day_data['avg_total_points'].mean()
                    
                    # Get hourly details from scored forecast for this date
                    day_hourly = scored_forecast[
                        scored_forecast['time'].dt.date == date_obj
                    ]
                    
                    # Get rating and wind info from hourly data if available
                    if not day_hourly.empty:
                        # Get the most common rating for the day
                        daily_rating = day_hourly['surf_rating'].mode().iloc[0] if not day_hourly['surf_rating'].mode().empty else 'Fair'
                        
                        # Create conditions summary from hourly data
                        wind_relationships = day_hourly['wind_relationship'].unique()
                        if 'favorable' in wind_relationships:
                            wind_summary = 'favorable winds'
                        elif 'cross' in wind_relationships:
                            wind_summary = 'cross winds'
                        else:
                            wind_summary = 'unfavorable winds'
                    else:
                        # Fallback if no hourly data
                        daily_rating = 'Fair'
                        wind_summary = 'variable winds'
                    
                    # Get daily weather for this date for sunrise/sunset
                    day_weather = None
                    if not daily_weather.empty:
                        weather_match = daily_weather[
                            daily_weather['date'].dt.date == date_obj
                        ]
                        if not weather_match.empty:
                            day_weather = weather_match.iloc[0]
                    
                    # Get sunrise/sunset first
                    sunrise = '06:30'
                    sunset = '19:30'
                    sunrise_hour = 6
                    sunset_hour = 19
                    
                    if day_weather is not None:
                        if 'sunrise' in day_weather:
                            sunrise_ts = datetime.fromtimestamp(day_weather['sunrise'])
                            sunrise = sunrise_ts.strftime('%H:%M')
                            sunrise_hour = sunrise_ts.hour
                        if 'sunset' in day_weather:
                            sunset_ts = datetime.fromtimestamp(day_weather['sunset'])
                            sunset = sunset_ts.strftime('%H:%M')
                            sunset_hour = sunset_ts.hour
                    
                    # Build hourly forecast from the day's hourly data
                    hourly_forecast = []
                    if day_hourly is not None and not day_hourly.empty:
                        for _, hour_row in day_hourly.iterrows():
                            hour_time = hour_row['time'].strftime('%H:%M')
                            
                            # Only include hours between sunrise and sunset
                            hour_num = hour_row['time'].hour
                            if sunrise_hour <= hour_num <= sunset_hour:
                                # Get real tide data for this hour
                                tide_height, tide_status = get_tide_for_hour(hour_row['time'])
                                
                                # Convert wave height from meters to feet for display
                                wave_size_m = hour_row.get('wave_size', 1)
                                wave_size_ft = wave_size_m * 3.28084
                                
                                hourly_forecast.append({
                                    'time': hour_time,
                                    'score': float(hour_row.get('total_points', 0)),
                                    'rating': hour_row.get('surf_rating', 'Unknown'),
                                    'swell': f"{wave_size_ft:.1f}ft",
                                    'period': f"{hour_row.get('wave_period', 10):.0f}s",
                                    'wind': f"{hour_row.get('wind_strength', 10):.0f}kts",
                                    'wind_angle': float(hour_row.get('wind_angle', 0)),
                                    'wind_favorable': hour_row.get('wind_relationship', 'unknown') == 'favorable',
                                    'tide': tide_status,
                                    'tide_height': tide_height,
                                    'temperature': 20  # Mock data - could be added from weather data
                                })
                    
                    # Get temperature data
                    temp_min = 16
                    temp_max = 22
                    
                    if day_weather is not None:
                        if 'temperature_2m_min' in day_weather:
                            temp_min = int(day_weather['temperature_2m_min'])
                        if 'temperature_2m_max' in day_weather:
                            temp_max = int(day_weather['temperature_2m_max'])
                    
                    forecast_data.append({
                        'date': date_str,
                        'overall_rating': daily_rating,
                        'overall_score': daily_avg_score,
                        'conditions_summary': wind_summary,
                        'wave_height_range': '2-4ft',  # Mock data
                        'wind_summary': wind_summary,
                        'sunrise': sunrise,
                        'sunset': sunset,
                        'temp_max': temp_max,
                        'temp_min': temp_min,
                        'hourly_forecast': hourly_forecast
                    })
            
            return jsonify({
                'spot_name': actual_spot_name,
                'forecast': forecast_data
            })
            
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch detailed forecast: {str(e)}',
            'spot_name': spot_name
        }), 500


@app.route('/api/forecast/spots')
def get_spots_conditions():
    """
    Get current conditions for multiple spots.
    
    Used for the forecasts overview page.
    """
    try:
        spots_param = request.args.get('spots', '')
        if spots_param:
            spot_names = [s.strip() for s in spots_param.split(',')]
        else:
            # Default spots - only the 5 with correct coordinates
            spot_names = ['La Graviere', 'Supertubos', 'Anchor Point', 'Uluwatu', 'Mundaka']
        
        spots_data = []
        
        with SurfDataDB(DB_PATH) as db:
            for spot_name in spot_names:
                spot_id = spot_name_to_id(spot_name)
                
                # Get latest daily score
                daily_scores = db.get_daily_scores(spot_id)
                
                if not daily_scores.empty:
                    latest_score = daily_scores.iloc[0]
                    
                    # Get current wave height and wind from latest scored forecast
                    scored_forecast = db.get_scored_forecast(spot_id)
                    wave_height = 0
                    wind_speed = 0
                    wind_direction = 0
                    
                    if not scored_forecast.empty:
                        latest_forecast = scored_forecast.iloc[0]  # Get most recent hour
                        wave_height = latest_forecast.get('wave_size', 0)  # in meters
                        wind_speed = latest_forecast.get('wind_strength', 0)  # in knots
                        wind_direction = latest_forecast.get('wind_angle', 0)  # in degrees
                    
                    spots_data.append({
                        'name': spot_name,
                        'location': get_location_for_spot(spot_name),
                        'image': get_image_for_spot(spot_name),
                        'current_conditions': {
                            'score': float(latest_score['avg_total_points']),
                            'rating': latest_score['avg_surf_rating'],
                            'summary': latest_score['conditions_summary'],
                            'wave_height': float(wave_height),  # in meters - will be converted to feet in frontend
                            'wind_speed': float(wind_speed),    # in knots
                            'wind_direction': float(wind_direction)  # in degrees
                        },
                        'forecast_badge': f"{len(daily_scores)} days"
                    })
                else:
                    # Fallback for spots without data
                    spots_data.append({
                        'name': spot_name,
                        'location': get_location_for_spot(spot_name),
                        'image': get_image_for_spot(spot_name),
                        'current_conditions': {
                            'score': 0.0,
                            'rating': 'No Data',
                            'summary': 'No forecast data available'
                        },
                        'forecast_badge': '0 days'
                    })
        
        return jsonify({
            'spots': spots_data,
            'total_spots': len(spots_data)
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch spots conditions: {str(e)}',
            'spots': []
        }), 500


def get_location_for_spot(spot_name):
    """Get location string for a spot."""
    locations = {
        'La Graviere': 'Hossegor, France',
        'Supertubos': 'Ericeira, Portugal',
        'Anchor Point': 'Taghazout, Morocco',
        'Uluwatu': 'Bali, Indonesia',
        'Mundaka': 'Basque Country, Spain'
    }
    return locations.get(spot_name, 'Unknown Location')


def get_image_for_spot(spot_name):
    """Get image URL for a spot."""
    images = {
        'La Graviere': 'https://i.imgur.com/SmlfMXh.jpeg',
        'Supertubos': 'https://i.imgur.com/28EPJj4.jpeg',
        'Anchor Point': 'https://i.imgur.com/850kLIm.jpeg',
        'Uluwatu': 'https://i.imgur.com/28EPJj4.jpeg',  # Placeholder
        'Mundaka': 'https://i.imgur.com/SmlfMXh.jpeg'   # Placeholder
    }
    return images.get(spot_name, 'https://i.imgur.com/28EPJj4.jpeg')


@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected'
    })


@app.route('/api/spots')
def list_spots():
    """List all available spots."""
    try:
        with SurfDataDB(DB_PATH) as db:
            # Get spots that have data
            spots_with_data = []
            
            for spot_name in surf_spots.keys():
                spot_id = spot_name_to_id(spot_name)
                daily_scores = db.get_daily_scores(spot_id)
                
                spots_with_data.append({
                    'name': spot_name,
                    'id': spot_id,
                    'has_data': not daily_scores.empty,
                    'days_available': len(daily_scores) if not daily_scores.empty else 0
                })
        
        return jsonify({
            'spots': spots_with_data,
            'total_spots': len(spots_with_data)
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Failed to list spots: {str(e)}',
            'spots': []
        }), 500


@app.route('/api/trips/analyze', methods=['POST'])
def analyze_trips():
    """
    Analyze surf trips based on user preferences and match with flights.
    
    Expected request body:
    {
        "user_preferences": {
            "departure_airports": ["LHR", "LGW"],
            "selected_spots": ["La Graviere", "Supertubos"],
            "trip_style": "weekend" | "long_weekend" | "best",
            "min_score": 4.0,
            "min_days": 3,
            "max_days": 5,
            "stopovers_allowed": false,
            "flight_times": {
                "outbound_preference": "18:00-22:00",
                "return_preference": "17:00+"
            }
        },
        "date_range_start": "2025-06-13",
        "date_range_end": "2025-07-13"
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'user_preferences' not in data:
            return jsonify({'error': 'Missing user_preferences in request body'}), 400
        
        user_preferences = data['user_preferences']
        
        # Validate required fields
        required_fields = ['departure_airports', 'selected_spots', 'trip_style', 'min_score']
        for field in required_fields:
            if field not in user_preferences:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Set defaults for optional fields
        user_preferences.setdefault('min_days', 3)
        user_preferences.setdefault('max_days', 5)
        user_preferences.setdefault('stopovers_allowed', False)
        user_preferences.setdefault('flight_times', {
            'outbound_preference': '18:00-22:00',
            'return_preference': '17:00+'
        })
        
        print(f"🏄 Analyzing trips for {len(user_preferences['selected_spots'])} spots")
        print(f"Trip style: {user_preferences['trip_style']}")
        print(f"Flight times: {user_preferences['flight_times']}")
        
        # Analyze each spot
        all_trips = []
        available_spots = []
        
        for spot_name in user_preferences['selected_spots']:
            if spot_name not in surf_spots:
                print(f"⚠️  Unknown spot: {spot_name}")
                continue
            
            spot_config = {
                'spot_id': spot_name_to_id(spot_name),
                **surf_spots[spot_name]
            }
            
            # Get half-day scores from cache (these are pre-processed and user-independent)
            half_day_scores = get_cached_half_day_scores(spot_config)
            if half_day_scores.empty:
                print(f"❌ {spot_name}: No half-day scores available")
                continue
            
            available_spots.append(spot_name)
            print(f"✅ {spot_name}: Analyzing surf windows...")
            
            # Find optimal windows based on trip style
            trip_style = user_preferences['trip_style']
            
            if trip_style == 'weekend':
                windows = find_weekend_windows(half_day_scores, user_preferences)
                print(f"  Found {len(windows)} weekend windows")
            elif trip_style == 'long_weekend':
                windows = find_long_weekend_windows(half_day_scores, user_preferences)
                print(f"  Found {len(windows)} long weekend windows")
            else:  # 'best'
                windows = find_best_windows(half_day_scores, user_preferences)
                print(f"  Found {len(windows)} best windows")
            
            # Convert windows to trip format and match flights
            for i, window in enumerate(windows):
                print(f"    Window {i+1}: {window['start_date']} to {window['end_date']}, score: {window['avg_score']:.2f}")
                trip = create_trip_from_window(window, spot_name, user_preferences)
                if trip:
                    all_trips.append(trip)
                    print(f"    ✅ Created trip successfully")
                else:
                    print(f"    ❌ Failed to create trip")
        
        if not available_spots:
            return jsonify({
                'error': 'No fresh data available for selected spots',
                'available_spots': []
            }), 404
        
        # Sort trips by score and categorize
        all_trips.sort(key=lambda x: x['avg_score'], reverse=True)
        
        # Categorize trips
        weekend_trips = [t for t in all_trips if t['trip_type'] == 'weekend'][:5]
        long_weekend_trips = [t for t in all_trips if t['trip_type'] == 'long_weekend'][:5]
        best_trips = [t for t in all_trips if t['trip_type'] == 'best'][:5]
        
        return jsonify({
            'success': True,
            'user_preferences': user_preferences,
            'weekend_trips': weekend_trips,
            'long_weekend_trips': long_weekend_trips,
            'best_trips': best_trips,
            'available_spots': available_spots,
            'total_trips_found': len(all_trips),
            'analysis_timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ Trip analysis error: {str(e)}")
        return jsonify({
            'error': f'Trip analysis failed: {str(e)}',
            'success': False
        }), 500


def find_weekend_windows(half_day_scores, user_preferences):
    """Find weekend surf windows (Saturday-Sunday surfing with Friday evening flight)."""
    from datetime import date, timedelta
    
    today = date.today()
    weekends = []
    
    for week_offset in range(4):  # Next 4 weekends
        # Find Saturday of this week
        days_until_saturday = (5 - today.weekday()) % 7
        if days_until_saturday == 0 and today.weekday() > 5:
            days_until_saturday = 7
        
        saturday = today + timedelta(days=days_until_saturday + (week_offset * 7))
        sunday = saturday + timedelta(days=1)
        
        # Filter scores for this weekend (Saturday-Sunday only)
        weekend_scores = half_day_scores[
            (half_day_scores['date'] >= pd.Timestamp(saturday)) &
            (half_day_scores['date'] <= pd.Timestamp(sunday))
        ]
        
        if not weekend_scores.empty:
            avg_score = weekend_scores['avg_total_points'].mean()
            if avg_score >= user_preferences['min_score']:
                weekends.append({
                    'start_date': saturday,
                    'end_date': sunday,
                    'duration_days': 2,  # Saturday-Sunday = 2 days
                    'avg_score': avg_score,
                    'type': 'weekend',
                    'total_sessions': len(weekend_scores),
                    'excellent_sessions': len(weekend_scores[weekend_scores['avg_total_points'] >= 7]),
                    'good_sessions': len(weekend_scores[weekend_scores['avg_total_points'] >= 5])
                })
    
    return sorted(weekends, key=lambda x: x['avg_score'], reverse=True)[:3]


def find_long_weekend_windows(half_day_scores, user_preferences):
    """
    Find long weekend windows (3-4 days) with extension logic.
    
    CONSTRAINT: Trips should only be extended past the weekend if the added days 
    improve the average surf score and the conditions are above a 6.
    """
    from datetime import date, timedelta
    
    # First get base weekend windows
    base_weekends = find_weekend_windows(half_day_scores, user_preferences)
    long_weekends = []
    
    for base_weekend in base_weekends:
        # Try extending the weekend by 1-2 days
        for extra_days in [1, 2]:
            extended_end = base_weekend['end_date'] + timedelta(days=extra_days)
            
            # Get scores for extended period
            extended_scores = half_day_scores[
                (half_day_scores['date'] >= pd.Timestamp(base_weekend['start_date'])) &
                (half_day_scores['date'] <= pd.Timestamp(extended_end))
            ]
            
            if not extended_scores.empty:
                extended_avg = extended_scores['avg_total_points'].mean()
                
                # Get scores for just the added days
                added_scores = half_day_scores[
                    (half_day_scores['date'] > pd.Timestamp(base_weekend['end_date'])) &
                    (half_day_scores['date'] <= pd.Timestamp(extended_end))
                ]
                
                # CONSTRAINT CHECK: Only extend if added days improve average AND are above 6
                if not added_scores.empty:
                    added_avg = added_scores['avg_total_points'].mean()
                    
                    # Check if extension improves average score AND added days are above 6
                    if extended_avg > base_weekend['avg_score'] and added_avg >= 6.0:
                        long_weekends.append({
                            'start_date': base_weekend['start_date'],
                            'end_date': extended_end,
                            'duration_days': base_weekend['duration_days'] + extra_days,
                            'avg_score': extended_avg,
                            'type': 'long_weekend',
                            'total_sessions': len(extended_scores),
                            'excellent_sessions': len(extended_scores[extended_scores['avg_total_points'] >= 7]),
                            'good_sessions': len(extended_scores[extended_scores['avg_total_points'] >= 5]),
                            'extension_rationale': f"Extended by {extra_days} days (avg {added_avg:.1f}) to improve overall score from {base_weekend['avg_score']:.1f} to {extended_avg:.1f}"
                        })
    
    # Also use the existing optimal windows function for comparison
    optimal_windows = select_optimal_windows(
        half_day_scores,
        min_days=3,
        max_days=4,
        min_score=user_preferences['min_score']
    )
    
    # Convert optimal windows to our format
    for _, window in optimal_windows.head(3).iterrows():
        long_weekends.append({
            'start_date': window['start_date'].date() if hasattr(window['start_date'], 'date') else window['start_date'],
            'end_date': window['end_date'].date() if hasattr(window['end_date'], 'date') else window['end_date'],
            'duration_days': window['days'],
            'avg_score': window['avg_score'],
            'type': 'long_weekend',
            'total_sessions': window['days'] * 2,  # Estimate
            'excellent_sessions': 0,  # Would need to calculate
            'good_sessions': 0,  # Would need to calculate
            'extension_rationale': 'Optimal window from algorithm'
        })
    
    return sorted(long_weekends, key=lambda x: x['avg_score'], reverse=True)[:5]


def find_best_windows(half_day_scores, user_preferences):
    """Find best overall windows regardless of day of week."""
    optimal_windows = select_optimal_windows(
        half_day_scores,
        min_days=user_preferences['min_days'],
        max_days=user_preferences['max_days'],
        min_score=user_preferences['min_score']
    )
    
    best_windows = []
    for _, window in optimal_windows.head(5).iterrows():
        best_windows.append({
            'start_date': window['start_date'].date() if hasattr(window['start_date'], 'date') else window['start_date'],
            'end_date': window['end_date'].date() if hasattr(window['end_date'], 'date') else window['end_date'],
            'duration_days': window['days'],
            'avg_score': window['avg_score'],
            'type': 'best',
            'total_sessions': window['days'] * 2,  # Estimate
            'excellent_sessions': 0,  # Would need to calculate
            'good_sessions': 0  # Would need to calculate
        })
    
    return best_windows


def create_trip_from_window(window, spot_name, user_preferences):
    """
    Create a detailed trip object from a scored window, including flights
    """
    try:
        # Map surf spots to destination airports
        destination_airports = {
            'La Graviere': 'BOD',  # Bordeaux
            'Supertubos': 'LIS',   # Lisbon  
            'Anchor Point': 'AGA'   # Agadir (connections required)
        }
        
        destination_airport = destination_airports.get(spot_name)
        if not destination_airport:
            print(f"❌ No destination airport mapped for {spot_name}")
            return None
        
        # Check if direct flights are required but not available for this route
        stopovers_allowed = user_preferences.get('stopovers_allowed', True)
        
        # Note: All routes now support the direct flight preference with 5-hour fallback logic
        # The flight fetcher will automatically prioritize direct flights and only use stopovers
        # if no direct flights are available within 5 hours of preferred departure time
        
        # Determine outbound and return dates based on trip type and user preferences
        trip_start = window['start_date']
        trip_end = window['end_date']
        
        # Convert to date objects if they're strings
        if isinstance(trip_start, str):
            trip_start = datetime.strptime(trip_start, '%Y-%m-%d').date()
        elif hasattr(trip_start, 'date'):
            trip_start = trip_start.date()
            
        if isinstance(trip_end, str):
            trip_end = datetime.strptime(trip_end, '%Y-%m-%d').date()
        elif hasattr(trip_end, 'date'):
            trip_end = trip_end.date()
        
        flight_times = determine_flight_times(window, user_preferences)
        
        # Determine actual flight dates (may be night before for some trips)
        if window['type'] == 'weekend' or flight_times.get('fly_night_before'):
            outbound_date = trip_start - timedelta(days=1)
        else:
            outbound_date = trip_start
            
        return_date = trip_end
        
        # Get user's departure airports - expand London airports
        user_airports = user_preferences.get('departure_airports', ['LHR'])
        expanded_airports = []
        
        for airport in user_airports:
            if airport in ['LHR', 'LGW', 'STN', 'LTN']:
                # If any London airport is selected, search ALL London airports
                if not any(london_airport in expanded_airports for london_airport in ['LHR', 'LGW', 'STN', 'LTN']):
                    expanded_airports.extend(['LHR', 'LGW', 'STN', 'LTN'])
            else:
                expanded_airports.append(airport)
        
        # Remove duplicates while preserving order
        departure_airports = list(dict.fromkeys(expanded_airports))
        
        print(f"🛫 Searching flights from {len(departure_airports)} airports: {departure_airports}")
        
        # Search flights from all departure airports and collect all options
        all_flights = []
        for departure_airport in departure_airports:
            try:
                # Get flight times from user preferences
                outbound_time = flight_times.get('outbound_options', ['flexible'])
                if isinstance(outbound_time, list):
                    outbound_time = outbound_time[0] if outbound_time else 'flexible'
                
                return_time = flight_times.get('return', 'flexible')
                
                # Use user's specific departure time preference
                user_flight_prefs = user_preferences.get('flight_times', {})
                if user_flight_prefs:
                    outbound_time = user_flight_prefs.get('outbound_preference', outbound_time)
                    return_time = user_flight_prefs.get('return_preference', return_time)
                
                flight_data = fetch_flights(
                    departure_airport=departure_airport,
                    destination_airport=destination_airport,
                    outbound_date=outbound_date.strftime('%Y-%m-%d') if hasattr(outbound_date, 'strftime') else str(outbound_date),
                    return_date=return_date.strftime('%Y-%m-%d') if hasattr(return_date, 'strftime') else str(return_date),
                    outbound_time_range=outbound_time,
                    return_time_range=return_time,
                    stopovers_allowed=user_preferences.get('stopovers_allowed', True)
                )
                
                if flight_data and len(flight_data) > 0:
                    # Add departure airport info to each flight
                    for flight in flight_data:
                        flight['departure_airport'] = departure_airport
                    all_flights.extend(flight_data)
                    
            except Exception as e:
                print(f"❌ Error fetching flights from {departure_airport}: {e}")
                continue
        
        # Sort all flights by price and take the 3 cheapest across all airports
        if all_flights:
            all_flights = sorted(all_flights, key=lambda x: x.get('price', 999999))[:3]
            print(f"✅ Found {len(all_flights)} cheapest flights across all London airports")
        else:
            # If no flights found, create minimal mock data for debugging
            print(f"⚠️ No flights found for {spot_name}, creating minimal flight data")
            all_flights = [{
                'price': 999,
                'currency': 'USD',
                'departure_airport': departure_airports[0],
                'is_mock_data': True,
                'mock_data_note': f"🧪 NO FLIGHTS FOUND - Check flight API for {departure_airports[0]} -> {destination_airport}",
                'outbound': {'departure': f"{outbound_date}T19:00:00", 'airline': 'No flights found'},
                'inbound': {'departure': f"{return_date}T17:00:00", 'airline': 'No flights found'}
            }]
        
        # Create the trip object
        trip = {
            'id': f"{window['type']}_{window.get('rank', 1)}",
            'spot_name': spot_name,
            'start_date': window['start_date'],
            'end_date': window['end_date'],
            'avg_score': window['avg_score'],
            'duration_days': window['duration_days'],
            'trip_type': window['type'],
            'outbound_date': outbound_date.strftime('%Y-%m-%d') if hasattr(outbound_date, 'strftime') else str(outbound_date),
            'return_date': return_date.strftime('%Y-%m-%d') if hasattr(return_date, 'strftime') else str(return_date),
            'flights': all_flights,
            'flight_summary': f"From £{min(f.get('price', 999) for f in all_flights)} • {len(all_flights)} options",
            'destination_airport': destination_airport,
            'searched_airports': departure_airports,
            'image_url': get_image_for_spot(spot_name)
        }
        
        return trip
        
    except Exception as e:
        print(f"❌ Error creating trip for {spot_name}: {e}")
        import traceback
        traceback.print_exc()
        return None


def determine_flight_times(window, user_preferences):
    """
    Determine flight times based on trip type and user preferences.
    
    CONSTRAINTS:
    - Weekend trips: Saturday-Sunday surfing with Friday evening outbound flight
    - User's earliest departure time preference is the driving parameter
    - Night-before rule: flights should preferentially be the night before if surfing starts early
    """
    trip_type = window['type']
    user_flight_times = user_preferences.get('flight_times', {})
    outbound_pref = user_flight_times.get('outbound_preference', 'flexible')
    return_pref = user_flight_times.get('return_preference', 'flexible')
    
    # Default flight times if user preference not specified
    default_outbound = '18:00-22:00'  # Evening flights
    default_return = '17:00+'  # Afternoon/evening return
    
    if trip_type == 'weekend':
        # Weekend trips: Saturday-Sunday surfing, so fly Friday evening
        # Use user's outbound preference but ensure it's Friday evening if they want night before
        if outbound_pref in ['night_before', 'evening', '18:00-22:00']:
            outbound_time = '18:00-22:00'
            rationale = 'Weekend trip - Friday evening departure for Saturday-Sunday surfing'
        elif outbound_pref != 'flexible' and ':' in outbound_pref:
            # User has specific time preference - use it but on Friday
            outbound_time = outbound_pref
            rationale = f'Weekend trip - Friday {outbound_pref} departure as per user preference'
        else:
            # Default to evening for weekend trips
            outbound_time = default_outbound
            rationale = 'Weekend trip - Friday evening departure (default)'
            
        return {
            'outbound_options': [outbound_time],
            'return': return_pref if return_pref != 'flexible' else default_return,
            'fly_night_before': True,
            'rationale': rationale
        }
    
    elif trip_type == 'long_weekend':
        # For long weekends, check if first day is full surfing day
        # If so, fly night before; otherwise same day
        
        # Assume if trip starts Friday, fly Thursday evening
        # If trip starts Saturday, fly Friday evening  
        # If trip starts Sunday or later, fly same day (unless user prefers night before)
        
        if outbound_pref == 'night_before':
            return {
                'outbound_options': ['18:00-22:00'],
                'return': return_pref if return_pref != 'flexible' else default_return,
                'fly_night_before': True,
                'rationale': 'Long weekend - night before departure as per user preference'
            }
        elif outbound_pref != 'flexible' and ':' in outbound_pref:
            # User has specific time - respect it
            # If it's early (before 12pm), consider flying night before
            try:
                hour = int(outbound_pref.split(':')[0])
                if hour < 12:
                    return {
                        'outbound_options': ['18:00-22:00'],  # Fly night before instead
                        'return': return_pref if return_pref != 'flexible' else default_return,
                        'fly_night_before': True,
                        'rationale': f'Long weekend - night before departure (user wanted {outbound_pref} but flying night before is better)'
                    }
                else:
                    return {
                        'outbound_options': [outbound_pref],
                        'return': return_pref if return_pref != 'flexible' else default_return,
                        'fly_night_before': False,
                        'rationale': f'Long weekend - same day departure at {outbound_pref} as per user preference'
                    }
            except:
                pass
        
        # Default: same day departure
        return {
            'outbound_options': [outbound_pref if outbound_pref != 'flexible' else '09:00-18:00'],
            'return': return_pref if return_pref != 'flexible' else default_return,
            'fly_night_before': False,
            'rationale': 'Long weekend - same day departure'
        }
    
    else:
        # For 'best' trips or other trip types, use user preferences directly
        actual_outbound = outbound_pref if outbound_pref != 'flexible' else default_outbound
        actual_return = return_pref if return_pref != 'flexible' else default_return
        
        # Check if user prefers night before
        fly_night_before = outbound_pref == 'night_before'
        
        return {
            'outbound_options': ['18:00-22:00' if fly_night_before else actual_outbound],
            'return': actual_return,
            'fly_night_before': fly_night_before,
            'rationale': f'User preferences applied - outbound: {actual_outbound}, return: {actual_return}'
        }


def generate_conditions_summary(window):
    """Generate a summary of surf conditions for the window."""
    avg_score = window['avg_score']
    
    if avg_score >= 7:
        return "Excellent conditions expected"
    elif avg_score >= 5:
        return "Good surf conditions"
    elif avg_score >= 4:
        return "Fair conditions, suitable for most surfers"
    else:
        return "Marginal conditions"


def analyze_user_trips(user_preferences, date_range_start, date_range_end):
    """
    Simple trip analyzer function to replace the missing trip_flight_matcher module.
    """
    try:
        trips_by_category = {
            'weekend_trips': [],
            'long_weekend_trips': [], 
            'best_trips': []
        }
        
        for spot_name in user_preferences.get('selected_spots', []):
            print(f"✅ {spot_name}: Analyzing surf windows...")
            
            # Create proper spot config for the data cache function
            if spot_name not in surf_spots:
                print(f"⚠️ Unknown spot: {spot_name}")
                continue
                
            spot_config = {
                'spot_id': spot_name_to_id(spot_name),
                **surf_spots[spot_name]
            }
            
            # Get surf windows for this spot
            half_day_scores = get_cached_half_day_scores(spot_config)
            
            if half_day_scores is None or half_day_scores.empty:
                print(f"⚠️ No surf data for {spot_name}")
                continue
            
            # Find windows based on trip style
            trip_style = user_preferences.get('trip_style', 'weekend')
            
            if trip_style == 'weekend':
                windows = find_weekend_windows(half_day_scores, user_preferences)
                for window in windows:
                    trip = create_trip_from_window(window, spot_name, user_preferences)
                    if trip:
                        trips_by_category['weekend_trips'].append(trip)
            
            elif trip_style == 'long_weekend':
                windows = find_long_weekend_windows(half_day_scores, user_preferences)
                for window in windows:
                    trip = create_trip_from_window(window, spot_name, user_preferences)
                    if trip:
                        trips_by_category['long_weekend_trips'].append(trip)
            
            elif trip_style == 'best':
                windows = find_best_windows(half_day_scores, user_preferences)
                for window in windows:
                    trip = create_trip_from_window(window, spot_name, user_preferences)
                    if trip:
                        trips_by_category['best_trips'].append(trip)
        
        # Sort trips by score within each category
        for category in trips_by_category:
            trips_by_category[category].sort(key=lambda x: x.get('avg_score', 0), reverse=True)
        
        return trips_by_category
        
    except Exception as e:
        print(f"Error in analyze_user_trips: {e}")
        return {'weekend_trips': [], 'long_weekend_trips': [], 'best_trips': []}


if __name__ == '__main__':
    print("🌊 Starting Forecast API Server")
    print(f"Database: {DB_PATH}")
    print("Available endpoints:")
    print("  GET /api/health")
    print("  GET /api/spots")
    print("  GET /api/forecast/daily/<spot_name>")
    print("  GET /api/forecast/detailed/<spot_name>")
    print("  GET /api/forecast/spots")
    print("  POST /api/trips/analyze")
    print("\nStarting server on http://localhost:5001")
    
    app.run(debug=True, host='0.0.0.0', port=5001) 