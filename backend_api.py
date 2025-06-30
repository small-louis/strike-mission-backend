#!/usr/bin/env python3
"""
Strike Mission Backend API

FastAPI service that exposes the Python backend functionality
for the Next.js frontend to consume.
"""

import sys
import os
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import json
from datetime import datetime, date, timedelta
import uvicorn
import pandas as pd
import numpy as np

# Add the src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, 'src')
sys.path.insert(0, src_path)

try:
    from utils.surf_spots import surf_spots, get_destination_airports
    from services.data_fetcher import SurfDataFetcher
    from surf_analysis.main import analyze_surf_conditions_from_cache
    from window_selection.optimal_windows import select_optimal_windows
    from utils.data_cache import get_cached_half_day_scores, is_data_fresh, get_cached_marine_forecast, get_cached_scored_forecast
    from utils.db_manager import SurfDataDB
    from data_fetching.openmeteo import fetch_weather_data, fetch_marine_data
    from utils.data_processor import merge_weather_and_marine_data
    from scoring.wave_scoring import score_forecast
    from flights.flight_fetcher import FlightFetcher
    BACKEND_AVAILABLE = True
    print("✅ Backend modules imported successfully")
except ImportError as e:
    print(f"❌ Backend modules not available: {e}")
    print("Running in fallback mode with mock data")
    BACKEND_AVAILABLE = False
    
    # Mock data for fallback
    surf_spots = {
        "La Graviere": {"lat": 43.676, "lon": -1.442, "primary_airport": "BOD", "location": "Hossegor, France"},
        "Supertubos": {"lat": 39.362, "lon": -9.375, "primary_airport": "LIS", "location": "Peniche, Portugal"},
        "Anchor Point": {"lat": 30.518, "lon": -9.760, "primary_airport": "AGA", "location": "Taghazout, Morocco"}
    }
    
    def get_destination_airports():
        return ["BOD", "LIS", "AGA", "BIA", "LPA"]
    
    def get_cached_half_day_scores(spot_config):
        return pd.DataFrame()
    
    def get_cached_marine_forecast(lat, lon, spot_config=None):
        return pd.DataFrame()
    
    def get_cached_scored_forecast(spot_config):
        return pd.DataFrame()
    
    def select_optimal_windows(half_day_scores, min_days=3, max_days=5, min_score=1.0):
        return []
    
    class SurfDataFetcher:
        def force_update_spot(self, spot_name):
            return {"errors": ["Mock mode"]}
    
    def is_data_fresh(spot_config, data_type='half_day'):
        return False

app = FastAPI(title="Strike Mission API", version="1.0.0")

# Configure CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://localhost:3001", 
        "https://*.vercel.app",
        "https://*.railway.app",
        "https://*.onrender.com",
        "https://*.netlify.app",
        "*"  # Allow all origins for now - replace with your actual domain
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class UserPreferences(BaseModel):
    departure_airports: List[str]
    selected_spots: List[str]
    trip_style: str
    min_score: float
    min_days: int
    max_days: int
    stopovers_allowed: bool

class TripRequest(BaseModel):
    user_preferences: UserPreferences
    date_range_start: str  # YYYY-MM-DD
    date_range_end: str    # YYYY-MM-DD

class SurfSpotInfo(BaseModel):
    name: str
    location: str
    primary_airport: str
    coordinates: Dict[str, float]

class TripWindow(BaseModel):
    spot_name: str
    start_date: str
    end_date: str
    duration_days: int
    avg_score: float
    total_score: float
    flights: List[Dict[str, Any]]

# Helper functions

def fetch_flights_for_trip(spot_name: str, start_date: str, end_date: str, user_preferences):
    """Fetch flights for a specific trip"""
    try:
        print(f"🛫 [DEBUG] Starting flight search for {spot_name}: {start_date} to {end_date}")
        
        # Initialize flight fetcher
        flight_fetcher = FlightFetcher()
        print(f"🛫 [DEBUG] Flight fetcher initialized, API enabled: {flight_fetcher.api_enabled}")
        
        # Map surf spots to destination airports
        destination_airports = {
            'La Graviere': 'BOD',  # Bordeaux
            'Supertubos': 'LIS',   # Lisbon  
            'Anchor Point': 'AGA'   # Agadir
        }
        
        destination_airport = destination_airports.get(spot_name)
        if not destination_airport:
            print(f"❌ No destination airport mapped for {spot_name}")
            return []
        
        print(f"🛫 [DEBUG] Destination airport: {destination_airport}")
        
        # Get user departure airports
        departure_airports = user_preferences.departure_airports
        print(f"🛫 [DEBUG] Departure airports: {departure_airports}")
        
        # Determine flight dates
        from datetime import datetime, timedelta
        trip_start = datetime.strptime(start_date, '%Y-%m-%d').date()
        trip_end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # For weekend trips, fly the night before
        outbound_date = trip_start - timedelta(days=1)
        return_date = trip_end
        
        print(f"🛫 [DEBUG] Flight dates: {outbound_date} to {return_date}")
        
        # Get flight preferences
        user_flight_times = getattr(user_preferences, 'flight_times', {})
        outbound_time = user_flight_times.get('outbound_preference', '19:30')
        return_time = user_flight_times.get('return_preference', '17:00')
        
        print(f"🛫 [DEBUG] Preferred times: {outbound_time} outbound, {return_time} return")
        
        # Search flights from all departure airports
        all_flights = []
        for departure_airport in departure_airports:
            try:
                print(f"🛫 [DEBUG] Searching flights: {departure_airport} → {destination_airport}")
                flights = flight_fetcher.fetch_flights(
                    departure_airport=departure_airport,
                    destination_airport=destination_airport,
                    outbound_date=outbound_date.strftime('%Y-%m-%d'),
                    return_date=return_date.strftime('%Y-%m-%d'),
                    outbound_time_range=outbound_time,
                    return_time_range=return_time,
                    stopovers_allowed=user_preferences.stopovers_allowed
                )
                
                print(f"🛫 [DEBUG] fetch_flights returned {len(flights) if flights else 0} flights")
                
                if flights:
                    # Add departure airport info to each flight
                    for flight in flights:
                        flight['departure_airport'] = departure_airport
                    all_flights.extend(flights)
                    print(f"🛫 [DEBUG] Added {len(flights)} flights from {departure_airport}")
                else:
                    print(f"🛫 [DEBUG] No flights found from {departure_airport}")
                    
            except Exception as e:
                print(f"❌ Error fetching flights from {departure_airport}: {e}")
                import traceback
                print(f"❌ Traceback: {traceback.format_exc()}")
                continue
        
        # Sort by price and return top 3
        if all_flights:
            all_flights = sorted(all_flights, key=lambda x: x.get('price', 999999))[:3]
            print(f"✅ [DEBUG] FINAL: Found {len(all_flights)} flights for {spot_name}")
            print(f"✅ [DEBUG] First flight: £{all_flights[0].get('price')} from {all_flights[0].get('departure_airport')}")
        else:
            print(f"⚠️ [DEBUG] FINAL: No flights found for {spot_name}")
        
        return all_flights
        
    except Exception as e:
        print(f"❌ Error in fetch_flights_for_trip: {e}")
        import traceback
        print(f"❌ Full traceback: {traceback.format_exc()}")
        return []

# API Endpoints

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Strike Mission Backend API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/surf-spots", response_model=List[SurfSpotInfo])
async def get_surf_spots():
    """Get all available surf spots with their information"""
    spots = []
    for name, data in surf_spots.items():
        spots.append(SurfSpotInfo(
            name=name,
            location=data.get("location", ""),
            primary_airport=data["primary_airport"],
            coordinates={
                "lat": data["lat"],
                "lon": data["lon"]
            }
        ))
    return spots

@app.get("/airports")
async def get_airports():
    """Get all destination airports"""
    return {
        "destination_airports": get_destination_airports(),
        "departure_airports": ["LHR", "LGW", "STN", "MAN", "EDI"]  # Common UK airports
    }

@app.post("/analyze-trips")
async def analyze_trips(request: TripRequest, background_tasks: BackgroundTasks):
    """
    Analyze surf conditions and find optimal trip windows
    """
    try:
        # Convert request to analysis parameters
        params = {
            "departure_airports": request.user_preferences.departure_airports,
            "selected_spots": request.user_preferences.selected_spots,
            "trip_style": request.user_preferences.trip_style,
            "min_score": request.user_preferences.min_score,
            "min_days": request.user_preferences.min_days,
            "max_days": request.user_preferences.max_days,
            "stopovers_allowed": request.user_preferences.stopovers_allowed,
            "date_range": {
                "start": request.date_range_start,
                "end": request.date_range_end
            }
        }
        
        # Check data freshness and fetch if needed (but don't wait for it)
        background_tasks.add_task(ensure_fresh_data, request.user_preferences.selected_spots)
        print(f"🔄 Started background data refresh for {len(request.user_preferences.selected_spots)} spots")
        
        # Analyze surf conditions for each spot
        all_trips = []
        
        for spot_name in request.user_preferences.selected_spots:
            if spot_name not in surf_spots:
                continue
                
            try:
                # Get cached scores for this spot
                spot_config = {
                    'spot_id': spot_name.lower().replace(' ', '_'),
                    **surf_spots[spot_name]
                }
                half_day_scores = get_cached_half_day_scores(spot_config)
                
                if half_day_scores is not None and not half_day_scores.empty:
                    print(f"✅ Using cached data for {spot_name}: {len(half_day_scores)} half-day scores")
                    # Find optimal windows
                    windows = select_optimal_windows(
                        half_day_scores,
                        min_days=params["min_days"],
                        max_days=params["max_days"],
                        min_score=params["min_score"]
                    )
                    
                    # Convert to trip format
                    print(f"   Found {len(windows)} windows for {spot_name}")
                    for i, (_, window) in enumerate(windows.head(3).iterrows()):  # Top 3 windows per spot
                        # Convert pandas Series to dict
                        window_dict = window.to_dict()
                        
                        # Convert start/end dates to strings if they're datetime objects
                        start_date = window_dict["start_date"]
                        end_date = window_dict["end_date"]
                        if hasattr(start_date, 'strftime'):
                            start_date = start_date.strftime("%Y-%m-%d")
                        if hasattr(end_date, 'strftime'):
                            end_date = end_date.strftime("%Y-%m-%d")
                        
                        # Use 'days' or 'duration_days' depending on what's available
                        duration = window_dict.get("duration_days", window_dict.get("days", 3))
                        
                        # Fetch flights for this trip
                        flights = fetch_flights_for_trip(spot_name, start_date, end_date, request.user_preferences)
                        
                        trip = TripWindow(
                            spot_name=spot_name,
                            start_date=start_date,
                            end_date=end_date,
                            duration_days=duration,
                            avg_score=round(window_dict["avg_score"], 1),
                            total_score=round(window_dict["total_score"], 1),
                            flights=flights
                        )
                        all_trips.append(trip)
                        print(f"     Trip {i+1}: {start_date} to {end_date}, score: {window_dict['avg_score']:.2f}")
                        
            except Exception as e:
                print(f"❌ Error analyzing {spot_name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Sort by average score
        all_trips.sort(key=lambda x: x.avg_score, reverse=True)
        
        print(f"🎯 Final result: {len(all_trips)} total trips found")
        for i, trip in enumerate(all_trips[:5]):
            print(f"   Trip {i+1}: {trip.spot_name} {trip.start_date}-{trip.end_date} score:{trip.avg_score}")
        
        # Convert to dict format for JSON serialization
        trips_list = []
        for trip in all_trips[:10]:
            trips_list.append({
                "spot_name": trip.spot_name,
                "start_date": trip.start_date,
                "end_date": trip.end_date,
                "duration_days": trip.duration_days,
                "avg_score": trip.avg_score,
                "total_score": trip.total_score,
                "flights": trip.flights
            })
        
        return {
            "success": True,
            "trips": trips_list,
            "analysis_params": params,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.get("/spot-forecast/{spot_name}")
async def get_spot_forecast(spot_name: str, days: int = 7):
    """Get detailed forecast for a specific spot including tide data"""
    if spot_name not in surf_spots:
        raise HTTPException(status_code=404, detail="Surf spot not found")
    
    try:
        # Get cached scores
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        half_day_scores = get_cached_half_day_scores(spot_config)
        
        if half_day_scores is None or half_day_scores.empty:
            raise HTTPException(status_code=404, detail="No forecast data available")
        
        # Get detailed scored forecast for ratings
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        
        # Get hourly scored forecast with ratings
        from src.utils.data_cache import get_cached_scored_forecast
        scored_forecast = get_cached_scored_forecast(spot_config)
        
        # Get marine data for tide information
        marine_df = get_cached_marine_forecast(
            surf_spots[spot_name]['lat'], 
            surf_spots[spot_name]['lon'], 
            spot_config
        )
        
        # Convert to frontend format
        forecast_data = []
        processed_dates = set()
        
        for _, row in half_day_scores.head(days * 2).iterrows():  # 2 half-days per day
            date_str = row["date"] if isinstance(row["date"], str) else row["date"].strftime("%Y-%m-%d")
            
            # Process tide data once per day
            tide_info = None
            if date_str not in processed_dates and not marine_df.empty:
                tide_info = process_tide_data(marine_df, date_str)
                processed_dates.add(date_str)
            
            # Convert half_day format (morning/afternoon -> AM/PM)
            half_day_display = row["half_day"]
            half_day_api = "AM" if row["half_day"] == "morning" else "PM"
            
            # Get rating information for this half-day from scored forecast
            rating_info = get_half_day_rating_info(scored_forecast, date_str, half_day_api)
            
            forecast_data.append({
                "date": date_str,
                "half_day": half_day_api,  # Use AM/PM format for frontend
                "score": round(row["avg_total_points"], 1),  # Use correct column name
                "wave_height": row.get("wave_height", 0),
                "wave_period": row.get("wave_period", 0),
                "wind_speed": row.get("wind_speed", 0),
                "wind_direction": row.get("wind_direction", 0),
                "tide_info": tide_info,
                "surf_rating": rating_info.get("surf_rating", "Unknown"),
                "wind_relationship": rating_info.get("wind_relationship", "unknown"),
                "conditions_summary": rating_info.get("conditions_summary", "N/A")
            })
        
        return {
            "spot_name": spot_name,
            "forecast": forecast_data,
            "spot_info": surf_spots[spot_name]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Forecast fetch failed: {str(e)}")

@app.get("/spot-forecast-detailed/{spot_name}")
async def get_detailed_spot_forecast(spot_name: str, days: int = 7):
    """Get detailed hourly forecast for a specific spot matching frontend structure"""
    if spot_name not in surf_spots:
        raise HTTPException(status_code=404, detail="Surf spot not found")
    
    try:
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        
        # Get hourly scored forecast with ratings
        from src.utils.data_cache import get_cached_scored_forecast
        scored_forecast = get_cached_scored_forecast(spot_config)
        
        if scored_forecast.empty:
            print(f"❌ No forecast data available for {spot_name}")
            raise HTTPException(status_code=404, detail="No forecast data available")
        
        print(f"✅ Using cached forecast data for {spot_name}: {len(scored_forecast)} hourly records")
        
        # Get marine data for tide information
        marine_df = get_cached_marine_forecast(
            surf_spots[spot_name]['lat'], 
            surf_spots[spot_name]['lon'], 
            spot_config
        )
        
        # Group data by date and half-day
        forecast_data = []
        
        # Get unique dates from the forecast
        scored_forecast['date_only'] = scored_forecast['time'].dt.date
        unique_dates = sorted(scored_forecast['date_only'].unique())[:days]
        
        for date in unique_dates:
            date_str = date.strftime("%Y-%m-%d")
            day_data = scored_forecast[scored_forecast['date_only'] == date].copy()
            
            # Process tide data for this date
            tide_info = process_tide_data(marine_df, date_str) if not marine_df.empty else None
            
            # Split into AM (6-12) and PM (12-18) periods
            for period, (start_hour, end_hour) in [("AM", (6, 12)), ("PM", (12, 18))]:
                period_data = day_data[
                    (day_data['time'].dt.hour >= start_hour) & 
                    (day_data['time'].dt.hour < end_hour)
                ].copy()
                
                if period_data.empty:
                    continue
                
                # Get overall half-day rating (best hour approach)
                best_hour_idx = period_data['total_points'].idxmax()
                best_hour = period_data.loc[best_hour_idx]
                
                # Calculate average score for half-day
                avg_score = period_data['total_points'].mean()
                
                # Build hourly breakdown
                hourly_forecast = []
                for _, hour_row in period_data.iterrows():
                    # Convert wave height to feet and format swell info
                    wave_height_ft = hour_row['wave_size'] * 3.28084
                    swell_info = f"{wave_height_ft:.0f}-{wave_height_ft+1:.0f}ft"
                    period_info = f"{hour_row['wave_period']:.0f}s"
                    
                    # Format wind info
                    wind_speed = hour_row['wind_strength']
                    wind_dir_text = get_wind_direction_text(hour_row['wind_angle'])
                    wind_info = f"{wind_speed:.0f}kts {wind_dir_text}"
                    
                    # Get tide height for this hour
                    tide_height = get_tide_height_for_hour(marine_df, hour_row['time'])
                    
                    hourly_forecast.append({
                        "time": hour_row['time'].strftime("%H:%M"),
                        "score": round(hour_row['total_points'], 1),
                        "rating": hour_row.get('surf_rating', 'Unknown'),
                        "swell": swell_info,
                        "period": period_info,
                        "wind": wind_info,
                        "wind_favorable": hour_row.get('wind_relationship', 'unknown') == 'favorable',
                        "tide": f"{tide_height:.1f}m" if tide_height is not None else "N/A"
                    })
                
                forecast_data.append({
                    "date": date_str,
                    "half_day": period,
                    "overall_rating": best_hour.get('surf_rating', 'Unknown'),
                    "overall_score": round(avg_score, 1),
                    "conditions_summary": best_hour.get('conditions_summary', 'N/A'),
                    "tide_info": tide_info,
                    "hourly_forecast": hourly_forecast
                })
        
        return {
            "spot_name": spot_name,
            "forecast": forecast_data,
            "spot_info": surf_spots[spot_name]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detailed forecast fetch failed: {str(e)}")

@app.post("/refresh-data")
async def refresh_data(background_tasks: BackgroundTasks, spots: Optional[List[str]] = None):
    """Refresh surf data for specified spots (or all spots if none specified)"""
    if spots is None:
        spots = list(surf_spots.keys())
    
    background_tasks.add_task(fetch_fresh_data, spots)
    
    return {
        "message": f"Data refresh initiated for {len(spots)} spots",
        "spots": spots,
        "timestamp": datetime.now().isoformat()
    }

# Background tasks
async def ensure_fresh_data(selected_spots: List[str]):
    """Ensure we have fresh data for selected spots"""
    fetcher = SurfDataFetcher()
    
    for spot_name in selected_spots:
        if spot_name not in surf_spots:
            continue
            
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        
        # Check if data is very stale (more than 7 days) before trying to refresh  
        from utils.data_cache import is_data_fresh
        if not is_data_fresh(spot_config, data_type='half_day', hours_threshold=168):  # 7 days
            try:
                print(f"🔄 Data is very stale for {spot_name}, attempting refresh...")
                fetcher.force_update_spot(spot_name)
                print(f"✅ Refreshed data for {spot_name}")
            except Exception as e:
                print(f"❌ Failed to refresh {spot_name}, using cached data: {e}")
        else:
            print(f"📋 Data for {spot_name} is acceptable (less than 7 days old)")

async def fetch_fresh_data(spots: List[str]):
    """Background task to fetch fresh data"""
    fetcher = SurfDataFetcher()
    
    for spot_name in spots:
        try:
            result = fetcher.force_update_spot(spot_name)
            if result and not result.get('errors'):
                print(f"✅ Successfully refreshed {spot_name}")
            else:
                print(f"❌ Failed to refresh {spot_name}")
        except Exception as e:
            print(f"❌ Error refreshing {spot_name}: {e}")

def get_half_day_rating_info(scored_forecast: pd.DataFrame, date_str: str, half_day: str) -> Dict[str, Any]:
    """
    Extract rating information for a specific half-day from the scored forecast.
    
    :param scored_forecast: DataFrame with hourly scored forecast including ratings
    :param date_str: Date string in YYYY-MM-DD format
    :param half_day: 'AM' or 'PM'
    :return: Dictionary with rating information
    """
    try:
        if scored_forecast.empty or 'time' not in scored_forecast.columns:
            return {"surf_rating": "Unknown", "wind_relationship": "unknown", "conditions_summary": "N/A"}
        
        # Filter data for the specific date
        target_date = pd.to_datetime(date_str).date()
        day_data = scored_forecast[scored_forecast['time'].dt.date == target_date].copy()
        
        if day_data.empty:
            return {"surf_rating": "Unknown", "wind_relationship": "unknown", "conditions_summary": "N/A"}
        
        # Filter by half-day (AM: 6-12, PM: 12-18)
        if half_day == "AM":
            half_day_data = day_data[(day_data['time'].dt.hour >= 6) & (day_data['time'].dt.hour < 12)]
        else:  # PM
            half_day_data = day_data[(day_data['time'].dt.hour >= 12) & (day_data['time'].dt.hour < 18)]
        
        if half_day_data.empty:
            return {"surf_rating": "Unknown", "wind_relationship": "unknown", "conditions_summary": "N/A"}
        
        # Get the most common rating for this half-day, or the best rating if tied
        rating_columns = ['surf_rating', 'wind_relationship', 'conditions_summary']
        available_columns = [col for col in rating_columns if col in half_day_data.columns]
        
        if not available_columns:
            return {"surf_rating": "Unknown", "wind_relationship": "unknown", "conditions_summary": "N/A"}
        
        # Use the rating from the hour with the highest total_points
        if 'total_points' in half_day_data.columns:
            best_hour = half_day_data.loc[half_day_data['total_points'].idxmax()]
        else:
            best_hour = half_day_data.iloc[0]  # Fallback to first hour
        
        return {
            "surf_rating": best_hour.get('surf_rating', 'Unknown'),
            "wind_relationship": best_hour.get('wind_relationship', 'unknown'),
            "conditions_summary": best_hour.get('conditions_summary', 'N/A')
        }
        
    except Exception as e:
        print(f"Error extracting rating info for {date_str} {half_day}: {e}")
        return {"surf_rating": "Unknown", "wind_relationship": "unknown", "conditions_summary": "N/A"}

def get_wind_direction_text(wind_angle: float) -> str:
    """Convert wind angle to compass direction text"""
    directions = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
    ]
    index = int((wind_angle + 11.25) / 22.5) % 16
    return directions[index]

def get_tide_height_for_hour(marine_df: pd.DataFrame, target_time: pd.Timestamp) -> float:
    """Get tide height for a specific hour"""
    try:
        if marine_df.empty or 'sea_level_height_msl' not in marine_df.columns:
            return None
        
        # Find the closest time in marine data
        marine_df['time_diff'] = abs(marine_df['date'] - target_time)
        closest_idx = marine_df['time_diff'].idxmin()
        
        return marine_df.loc[closest_idx, 'sea_level_height_msl']
    except:
        return None

def process_tide_data(marine_df: pd.DataFrame, date_str: str) -> Dict[str, Any]:
    """
    Process tide data for a specific date and return high/low tide info.
    
    :param marine_df: DataFrame with marine data including sea_level_height_msl
    :param date_str: Date string in YYYY-MM-DD format
    :return: Dictionary with tide information
    """
    try:
        # Filter data for the specific date
        target_date = pd.to_datetime(date_str).date()
        day_data = marine_df[marine_df['date'].dt.date == target_date].copy()
        
        if day_data.empty or 'sea_level_height_msl' not in day_data.columns:
            return {
                "high_tide": {"time": "N/A", "height": "N/A"},
                "low_tide": {"time": "N/A", "height": "N/A"}
            }
        
        # Remove NaN values
        day_data = day_data.dropna(subset=['sea_level_height_msl'])
        
        if day_data.empty:
            return {
                "high_tide": {"time": "N/A", "height": "N/A"},
                "low_tide": {"time": "N/A", "height": "N/A"}
            }
        
        # Find high and low tide
        tide_values = day_data['sea_level_height_msl'].values
        tide_times = day_data['date'].values
        
        # Find peaks and troughs (simplified approach)
        high_idx = np.argmax(tide_values)
        low_idx = np.argmin(tide_values)
        
        high_time = pd.to_datetime(tide_times[high_idx])
        low_time = pd.to_datetime(tide_times[low_idx])
        
        return {
            "high_tide": {
                "time": high_time.strftime("%H:%M"),
                "height": f"{tide_values[high_idx]:.1f}m"
            },
            "low_tide": {
                "time": low_time.strftime("%H:%M"),
                "height": f"{tide_values[low_idx]:.1f}m"
            }
        }
        
    except Exception as e:
        print(f"Error processing tide data for {date_str}: {e}")
        return {
            "high_tide": {"time": "N/A", "height": "N/A"},
            "low_tide": {"time": "N/A", "height": "N/A"}
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True) 