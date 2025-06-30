import requests
import json
import os
from datetime import datetime, timedelta
import time

def build_kiwi_headers():
    api_key = os.getenv('KIWI_API_KEY')
    if not api_key:
        raise ValueError("KIWI_API_KEY environment variable not set")
    return {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "kiwi-com-cheap-flights.p.rapidapi.com"
    }

def fetch_flights(departure_airport, destination_airport, outbound_date, return_date, 
                 outbound_time_range=None, return_time_range=None, stopovers_allowed=True):
    """
    Fetch flight data from Kiwi.com API with proper error handling
    """
    fetcher = FlightFetcher()
    return fetcher.fetch_flights(departure_airport, destination_airport, outbound_date, return_date, 
                                outbound_time_range, return_time_range, stopovers_allowed)

class FlightFetcher:
    def __init__(self):
        # Try environment variable first, then fallback to direct key for Railway deployment
        self.api_key = os.getenv('KIWI_API_KEY') or "481a550d02msh234f431534bff22p1ab62bjsn7b3bb79de1a8"
        self.base_url = "https://kiwi-com-cheap-flights.p.rapidapi.com"
        
        # Check if API is available, but don't fail if not
        if self.api_key:
            self.headers = {
                'X-RapidAPI-Key': self.api_key,
                'X-RapidAPI-Host': 'kiwi-com-cheap-flights.p.rapidapi.com'
            }
            self.api_enabled = True
            print(f"✅ Flight API enabled with key: {self.api_key[:10]}...")
            
            # Test API connection with a simple request
            try:
                test_response = requests.get(
                    f"{self.base_url}/locations/query",
                    headers=self.headers,
                    params={'term': 'LHR', 'locale': 'en'},
                    timeout=10
                )
                if test_response.status_code == 200:
                    print("✅ Kiwi API connection test successful")
                else:
                    print(f"⚠️ Kiwi API test failed: {test_response.status_code} - {test_response.text[:100]}")
                    
            except Exception as e:
                print(f"⚠️ Kiwi API test error: {e}")
        else:
            self.api_enabled = False
            print("⚠️ Flight API disabled - using mock data fallback")
    
    def fetch_flights(self, departure_airport, destination_airport, outbound_date, return_date, 
                     outbound_time_range=None, return_time_range=None, stopovers_allowed=True):
        """
        Fetch flights with new logic:
        1. Always try direct flights first for ALL routes
        2. Only include stopover flights if no direct flights within 5 hours of preferred departure time
        3. Rank by user time preferences then price
        """
        try:
            # If API is not enabled, return mock data
            if not self.api_enabled:
                return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
            
            # Always try direct flights first
            direct_flights = self._fetch_flights_with_stopovers(
                departure_airport, destination_airport, outbound_date, return_date, 
                outbound_time_range, return_time_range, stopovers_allowed=False
            )
            
            # Parse user preferred departure time for 5-hour window check
            preferred_hour = self._parse_preferred_hour(outbound_time_range, default=19)
            direct_flights_in_window = self._filter_flights_by_time_window(direct_flights, preferred_hour, window_hours=5)
            
            print(f"✅ Found {len(direct_flights)} total direct flights, {len(direct_flights_in_window)} within 5h of {preferred_hour:02d}:00")
            
            # If we have direct flights within 5 hours, use only those
            if direct_flights_in_window:
                print(f"🎯 Using {len(direct_flights_in_window)} direct flights within 5-hour window")
                ranked_flights = self._rank_flights_by_preferences(direct_flights_in_window, outbound_time_range, return_time_range)
                return ranked_flights
            
            # If we have direct flights but not in window, use all direct flights
            elif direct_flights:
                print(f"⏰ No direct flights within 5h window, using all {len(direct_flights)} direct flights")
                ranked_flights = self._rank_flights_by_preferences(direct_flights, outbound_time_range, return_time_range)
                return ranked_flights
            
            # Only if no direct flights available and stopovers are allowed, try with connections
            elif stopovers_allowed:
                print(f"🔄 No direct flights found, searching with stopovers...")
                stopover_flights = self._fetch_flights_with_stopovers(
                    departure_airport, destination_airport, outbound_date, return_date, 
                    outbound_time_range, return_time_range, stopovers_allowed=True
                )
                
                if stopover_flights:
                    print(f"✅ Found {len(stopover_flights)} flights with stopovers")
                    ranked_flights = self._rank_flights_by_preferences(stopover_flights, outbound_time_range, return_time_range)
                    return ranked_flights
            
            # No flights found
            print(f"❌ No flights found for {departure_airport} -> {destination_airport}")
            return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
            
        except Exception as e:
            print(f"❌ Error in fetch_flights: {e}")
            return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)

    def _fetch_flights_with_stopovers(self, departure_airport, destination_airport, outbound_date, return_date, 
                                     outbound_time_range=None, return_time_range=None, stopovers_allowed=True):
        """Separate method to handle the actual API call with or without stopovers"""
        try:
            print(f"✅ Searching real flights: {departure_airport} -> {destination_airport}")
            
            # Build search parameters with broader time window
            params = self._build_params(departure_airport, destination_airport, outbound_date, return_date, 
                                      outbound_time_range, return_time_range, stopovers_allowed)
            
            print(f"🔍 [DEBUG] API call parameters: {params}")
            
            # Make API request
            search_url = f"{self.base_url}/v2/search"
            print(f"🔍 [DEBUG] Making request to: {search_url}")
            print(f"🔍 [DEBUG] Headers: {dict(self.headers)}")
            
            response = requests.get(search_url, headers=self.headers, params=params, timeout=30)
            
            print(f"🔍 [DEBUG] Response status: {response.status_code}")
            print(f"🔍 [DEBUG] Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"🔍 [DEBUG] Response data keys: {list(data.keys()) if data else 'No data'}")
                
                if 'data' in data:
                    print(f"🔍 [DEBUG] Number of flights in response: {len(data['data'])}")
                    if data['data']:
                        print(f"🔍 [DEBUG] Sample flight keys: {list(data['data'][0].keys())}")
                else:
                    print(f"🔍 [DEBUG] No 'data' key in response: {data}")
                
                flights = self._process_flight_data(data)
                
                if flights:
                    flight_type = "direct flights" if not stopovers_allowed else "flights with connections"
                    print(f"✅ Found {len(flights)} real {flight_type} for {departure_airport} -> {destination_airport}")
                    return flights
                else:
                    print(f"⚠️ API returned no flights for {departure_airport} -> {destination_airport}")
                    print(f"⚠️ Falling back to mock data")
                    return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
                    
            elif response.status_code == 429:
                print(f"⚠️ Rate limited for {departure_airport} -> {destination_airport}")
                print(f"⚠️ Response: {response.text[:200]}")
                return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
            elif response.status_code == 401:
                print(f"⚠️ Unauthorized for {departure_airport} -> {destination_airport}")
                print(f"⚠️ Response: {response.text[:200]}")
                return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
            else:
                print(f"⚠️ API error {response.status_code} for {departure_airport} -> {destination_airport}")
                print(f"⚠️ Response: {response.text[:200]}")
                return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
                
        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout for {departure_airport} -> {destination_airport}")
            return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error for {departure_airport} -> {destination_airport}: {e}")
            return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)
        except Exception as e:
            print(f"❌ Unexpected error for {departure_airport} -> {destination_airport}: {e}")
            import traceback
            print(f"❌ Traceback: {traceback.format_exc()}")
            return self._create_mock_flights(departure_airport, destination_airport, outbound_date, return_date, outbound_time_range, return_time_range)

    def _filter_flights_by_time_window(self, flights, preferred_hour, window_hours=5):
        """Filter flights to only those within the specified time window of preferred departure"""
        if not flights:
            return flights
            
        filtered_flights = []
        for flight in flights:
            outbound_hour = self._extract_hour_from_flight_time(flight.get('outbound', {}).get('departure', ''))
            if outbound_hour is None:
                continue
                
            # Calculate time difference (handle day wrap-around)
            time_diff = min(abs(outbound_hour - preferred_hour), 
                           abs(outbound_hour - preferred_hour + 24),
                           abs(outbound_hour - preferred_hour - 24))
            
            if time_diff <= window_hours:
                filtered_flights.append(flight)
                
        return filtered_flights

    def _add_duration_to_time(self, time_str, duration_str):
        """Add duration to time string (HH:MM)"""
        try:
            # Parse the time
            time_obj = datetime.strptime(time_str, "%H:%M")
            
            # Parse duration (assumes format like "2h 30m")
            hours = 0
            minutes = 0
            if 'h' in duration_str:
                hours = int(duration_str.split('h')[0].strip())
            if 'm' in duration_str:
                minutes_part = duration_str.split('h')[-1] if 'h' in duration_str else duration_str
                if 'm' in minutes_part:
                    minutes = int(minutes_part.split('m')[0].strip())
            
            # Add duration
            result_time = time_obj + timedelta(hours=hours, minutes=minutes)
            return result_time.strftime("%H:%M")
        except:
            return "Unknown"

    def _build_params(self, departure_airport, destination_airport, outbound_date, return_date, 
                     outbound_time_range, return_time_range, stopovers_allowed=True):
        """Build API request parameters"""
        
        params = {
            'fly_from': departure_airport,
            'fly_to': destination_airport,
            'date_from': outbound_date,
            'date_to': outbound_date,
            'return_from': return_date,
            'return_to': return_date,
            'flight_type': 'round',
            'adults': 1,
            'children': 0,
            'infants': 0,
            'selected_cabins': 'M',  # Economy
            'mix_with_cabins': 'M',
            'adult_hold_bag': '1',
            'adult_hand_bag': '1',
            'curr': 'GBP',
            'locale': 'en',
            'limit': 200  # Get more results for better selection
        }
        
        # Set max stopovers
        if not stopovers_allowed:
            params['max_stopovers'] = 0
        else:
            params['max_stopovers'] = 3
        
        # Add time preferences if specified
        if outbound_time_range and outbound_time_range != 'flexible':
            try:
                if ':' in outbound_time_range:
                    # Specific time like "19:30"
                    hour = int(outbound_time_range.split(':')[0])
                    params['depart_after'] = f"{hour-2:02d}:00"
                    params['depart_before'] = f"{hour+2:02d}:00"
                elif outbound_time_range in ['morning', 'afternoon', 'evening']:
                    time_windows = {
                        'morning': ('06:00', '11:59'),
                        'afternoon': ('12:00', '17:59'), 
                        'evening': ('18:00', '23:59')
                    }
                    params['depart_after'], params['depart_before'] = time_windows[outbound_time_range]
            except:
                pass
        
        if return_time_range and return_time_range != 'flexible':
            try:
                if ':' in return_time_range:
                    # Specific time like "17:00"
                    hour = int(return_time_range.split(':')[0])
                    params['return_after'] = f"{hour-2:02d}:00"
                    params['return_before'] = f"{hour+2:02d}:00"
                elif return_time_range in ['morning', 'afternoon', 'evening']:
                    time_windows = {
                        'morning': ('06:00', '11:59'),
                        'afternoon': ('12:00', '17:59'),
                        'evening': ('18:00', '23:59')
                    }
                    params['return_after'], params['return_before'] = time_windows[return_time_range]
            except:
                pass
        
        return params

    def _process_flight_data(self, data):
        """Process the API response into our format"""
        if 'data' not in data or not data['data']:
            return []
        
        flights = []
        for flight_data in data['data']:
            try:
                # Extract basic info
                price = flight_data.get('price', 0)
                currency = flight_data.get('currency', 'GBP')
                booking_token = flight_data.get('booking_token', '')
                
                # Process route segments
                route = flight_data.get('route', [])
                if len(route) < 2:
                    continue
                
                # Find outbound and inbound segments
                outbound_segments = []
                inbound_segments = []
                
                for segment in route:
                    if segment.get('return') == 0:
                        outbound_segments.append(segment)
                    else:
                        inbound_segments.append(segment)
                
                if not outbound_segments or not inbound_segments:
                    continue
                
                # Process outbound flight
                outbound = self._process_flight_segment(outbound_segments)
                if not outbound:
                    continue
                
                # Process inbound flight  
                inbound = self._process_flight_segment(inbound_segments)
                if not inbound:
                    continue
                
                # Calculate total duration
                total_duration = self._calculate_total_duration(outbound_segments + inbound_segments)
                
                # Create booking URL
                booking_url = f"https://www.kiwi.com/deep?from={outbound_segments[0]['flyFrom']}&to={outbound_segments[-1]['flyTo']}&departure={outbound_segments[0]['dTimeUTC'][:10]}&return={inbound_segments[0]['dTimeUTC'][:10]}&token={booking_token}"
                
                flight = {
                    'price': price,
                    'currency': currency,
                    'booking_url': booking_url,
                    'total_duration': total_duration,
                    'departure_airport': outbound_segments[0]['flyFrom'],
                    'outbound': outbound,
                    'inbound': inbound,
                    'is_mock_data': False
                }
                
                flights.append(flight)
                
            except Exception as e:
                print(f"⚠️ Error processing flight: {e}")
                continue
        
        return flights

    def _process_flight_segment(self, segments):
        """Process a flight segment (outbound or inbound)"""
        if not segments:
            return None
        
        try:
            first_segment = segments[0]
            last_segment = segments[-1]
            
            # Get departure and arrival times (convert from UTC)
            departure_utc = datetime.fromisoformat(first_segment['dTimeUTC'].replace('Z', '+00:00'))
            arrival_utc = datetime.fromisoformat(last_segment['aTimeUTC'].replace('Z', '+00:00'))
            
            # Convert to local time (approximate - using UTC+1 for Europe)
            departure_local = departure_utc + timedelta(hours=1)
            arrival_local = arrival_utc + timedelta(hours=1)
            
            # Calculate total duration
            total_duration = arrival_utc - departure_utc
            duration_str = self._format_duration(int(total_duration.total_seconds()))
            
            # Determine airline (use first segment's airline)
            airline_code = first_segment.get('airline', 'Unknown')
            airline_name = self._get_airline_name(airline_code)
            
            # Count stops and determine connection info
            stops = len(segments) - 1
            via = None
            if stops > 0:
                # Get connection airports
                connection_airports = []
                for i in range(len(segments) - 1):
                    connection_airports.append(segments[i]['flyTo'])
                via = ', '.join(connection_airports)
            
            return {
                'departure': departure_local.strftime('%H:%M'),
                'arrival': arrival_local.strftime('%H:%M'),
                'duration': duration_str,
                'airline': airline_name,
                'stops': stops,
                'via': via
            }
            
        except Exception as e:
            print(f"⚠️ Error processing segment: {e}")
            return None

    def _format_duration(self, duration_seconds):
        """Format duration from seconds to human readable format"""
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        return f"{hours}h {minutes:02d}m"

    def _rank_flights_by_preferences(self, flights, outbound_time_range, return_time_range):
        """
        Rank flights by user preferences:
        1. First flight: Cheapest that's closest to preferred outbound time
        2. Next 2 flights: Cheapest within 3 hours of preferred time, or any time if none available
        """
        if not flights:
            return []
        
        # Parse preferred times
        preferred_outbound_hour = self._parse_preferred_hour(outbound_time_range, default=19)
        preferred_return_hour = self._parse_preferred_hour(return_time_range, default=20)
        
        print(f"🎯 Ranking flights by preferences: Outbound {preferred_outbound_hour:02d}:00, Return {preferred_return_hour:02d}:00")
        
        # Score each flight
        scored_flights = []
        for flight in flights:
            try:
                # Extract outbound and return times
                outbound_hour = self._extract_hour_from_flight_time(flight['outbound']['departure'])
                return_hour = self._extract_hour_from_flight_time(flight['inbound']['departure'])
                
                # Calculate time scores (lower is better - closer to preferred time)
                outbound_time_score = self._calculate_time_score(outbound_hour, preferred_outbound_hour)
                return_time_score = self._calculate_time_score(return_hour, preferred_return_hour)
                
                # Combine time score (outbound is more important)
                total_time_score = (outbound_time_score * 2) + return_time_score
                
                # Price score (normalized)
                price_score = flight['price']
                
                scored_flights.append({
                    'flight': flight,
                    'time_score': total_time_score,
                    'price_score': price_score,
                    'outbound_hour': outbound_hour,
                    'return_hour': return_hour
                })
                
            except Exception as e:
                print(f"⚠️ Error scoring flight: {e}")
                continue
        
        if not scored_flights:
            return []
        
        # Sort by time preference first, then price
        scored_flights.sort(key=lambda x: (x['time_score'], x['price_score']))
        
        # Get first flight (best time match + cheapest)
        result_flights = [scored_flights[0]['flight']]
        
        # Get next 2 cheapest flights within 3 hours of preferred time
        remaining_flights = scored_flights[1:]
        within_window = [f for f in remaining_flights if f['time_score'] <= 3 * 60]  # 3 hours in minutes
        
        if len(within_window) >= 2:
            # Sort by price and take cheapest 2
            within_window.sort(key=lambda x: x['price_score'])
            result_flights.extend([f['flight'] for f in within_window[:2]])
        else:
            # Take what we have within window, then cheapest overall
            result_flights.extend([f['flight'] for f in within_window])
            
            # Fill remaining slots with cheapest flights overall
            remaining_needed = 3 - len(result_flights)
            if remaining_needed > 0:
                all_remaining = [f for f in remaining_flights if f not in within_window]
                all_remaining.sort(key=lambda x: x['price_score'])
                result_flights.extend([f['flight'] for f in all_remaining[:remaining_needed]])
        
        # Log the ranking results
        for i, flight in enumerate(result_flights[:3]):
            outbound_time = flight['outbound']['departure']
            price = flight['price']
            airline = flight['outbound']['airline']
            print(f"  Flight {i+1}: {outbound_time} departure, £{price}, {airline}")
        
        return result_flights[:3]  # Return max 3 flights

    def _calculate_time_score(self, actual_hour, preferred_hour):
        """Calculate time preference score (lower is better)"""
        if actual_hour is None:
            return 999  # Penalty for unknown time
        
        # Calculate minimum distance considering 24-hour wrap
        diff1 = abs(actual_hour - preferred_hour)
        diff2 = abs(actual_hour - preferred_hour + 24)
        diff3 = abs(actual_hour - preferred_hour - 24)
        
        return min(diff1, diff2, diff3) * 60  # Convert to minutes

    def _calculate_total_duration(self, segments):
        """Calculate total journey duration"""
        if not segments:
            return "Unknown"
        
        try:
            first_departure = datetime.fromisoformat(segments[0]['dTimeUTC'].replace('Z', '+00:00'))
            last_arrival = datetime.fromisoformat(segments[-1]['aTimeUTC'].replace('Z', '+00:00'))
            
            total_duration = last_arrival - first_departure
            return self._format_duration(int(total_duration.total_seconds()))
        except:
            return "Unknown"

    def _get_airline_name(self, airline_code):
        """Convert airline code to full name"""
        airline_names = {
            'FR': 'Ryanair', 'U2': 'easyJet', 'BA': 'British Airways',
            'TP': 'TAP Air Portugal', 'AF': 'Air France', 'KL': 'KLM',
            'LH': 'Lufthansa', 'IB': 'Iberia', 'AT': 'Royal Air Maroc',
            'VS': 'Virgin Atlantic', 'W6': 'Wizz Air', 'VY': 'Vueling'
        }
        return airline_names.get(airline_code, airline_code)

    def _parse_preferred_hour(self, time_preference, default=19):
        """Parse user time preference to hour"""
        if not time_preference or time_preference == 'flexible':
            return default
        
        if ':' in time_preference:
            try:
                return int(time_preference.split(':')[0])
            except:
                return default
        
        # Handle named time slots
        time_slots = {
            'morning': 9,
            'afternoon': 14,
            'evening': 19
        }
        return time_slots.get(time_preference.lower(), default)

    def _extract_hour_from_flight_time(self, time_str):
        """Extract hour from time string like '19:30'"""
        try:
            if ':' in time_str:
                return int(time_str.split(':')[0])
        except:
            pass
        return None

    def _create_mock_flights(self, departure_airport, destination_airport, outbound_date, return_date, 
                           outbound_time_range=None, return_time_range=None):
        """Create realistic mock flight data when API is unavailable"""
        import random
        
        # Parse preferred times
        outbound_hour = self._parse_preferred_hour(outbound_time_range, default=19)
        return_hour = self._parse_preferred_hour(return_time_range, default=17)
        
        # Define realistic route mappings with connections where needed
        realistic_routes = {
            # LHR routes - mostly require connections to European surf spots
            ('LHR', 'BOD'): {  # Bordeaux (for La Graviere)
                'airlines': ['Air France', 'British Airways'],
                'via': ['CDG', 'MAD'],  # Connect via Paris or Madrid
                'connection_time': 90,  # 1.5 hours layover
                'flight_time': 120,     # 2 hours each leg
                'exists_direct': False
            },
            ('LHR', 'LIS'): {  # Lisbon (for Supertubos)
                'airlines': ['TAP Air Portugal', 'British Airways'],
                'via': [None],  # Direct flights exist
                'connection_time': 0,
                'flight_time': 150,     # 2.5 hours direct
                'exists_direct': True
            },
            ('LHR', 'AGA'): {  # Agadir (for Anchor Point in Taghazout, Morocco)
                'airlines': ['British Airways', 'Royal Air Maroc'],
                'via': ['MAD', 'CMN'],  # Connect via Madrid or Casablanca
                'connection_time': 120, # 2 hours layover
                'flight_time': 180,     # 3 hours each leg
                'exists_direct': False
            },
            
            # LGW routes - budget airlines with some direct services
            ('LGW', 'BOD'): {
                'airlines': ['easyJet'],
                'via': [None],  # Direct flights exist
                'connection_time': 0,
                'flight_time': 105,     # 1h 45m direct
                'exists_direct': True
            },
            ('LGW', 'LIS'): {
                'airlines': ['easyJet', 'TAP Air Portugal'],
                'via': [None],
                'connection_time': 0,
                'flight_time': 135,     # 2h 15m direct
                'exists_direct': True
            },
            ('LGW', 'AGA'): {
                'airlines': ['easyJet', 'Royal Air Maroc'],
                'via': ['MAD'],  # Connect via Madrid
                'connection_time': 90,
                'flight_time': 150,     # 2.5 hours each leg
                'exists_direct': False
            }
        }
        
        # Get route info or default
        route_key = (departure_airport, destination_airport)
        route_info = realistic_routes.get(route_key, {
            'airlines': ['British Airways', 'Air France', 'KLM'],
            'via': ['CDG'],  # Default to Paris connection
            'connection_time': 90,
            'flight_time': 120,
            'exists_direct': False
        })
        
        mock_flights = []
        
        # Create 2-3 realistic flights
        for i in range(2):
            airline = random.choice(route_info['airlines'])
            has_connection = route_info['via'][0] is not None
            via_airport = random.choice(route_info['via']) if has_connection else None
            
            # Calculate realistic times
            if has_connection:
                # Flight 1: Departure to connection
                first_leg_departure = f'{outbound_hour:02d}:{random.randint(0, 59):02d}'
                first_leg_arrival_hour = (outbound_hour + route_info['flight_time'] // 60) % 24
                first_leg_arrival = f'{first_leg_arrival_hour:02d}:{random.randint(0, 59):02d}'
                
                # Flight 2: Connection to destination (after layover)
                second_leg_departure_hour = (first_leg_arrival_hour + route_info['connection_time'] // 60) % 24
                second_leg_departure = f'{second_leg_departure_hour:02d}:{random.randint(0, 59):02d}'
                final_arrival_hour = (second_leg_departure_hour + route_info['flight_time'] // 60) % 24
                final_arrival = f'{final_arrival_hour:02d}:{random.randint(0, 59):02d}'
                
                # Total duration including layover
                total_minutes = (route_info['flight_time'] * 2) + route_info['connection_time']
                total_duration = f'{total_minutes // 60}h {total_minutes % 60}m'
                
                outbound_info = {
                    'departure': first_leg_departure,
                    'arrival': final_arrival,
                    'duration': total_duration,
                    'airline': airline,
                    'stops': 1,
                    'via': via_airport
                }
                
                # Return flight with connection
                return_first_departure = f'{return_hour:02d}:{random.randint(0, 59):02d}'
                return_final_arrival_hour = (return_hour + total_minutes // 60) % 24
                return_final_arrival = f'{return_final_arrival_hour:02d}:{random.randint(0, 59):02d}'
                
                inbound_info = {
                    'departure': return_first_departure,
                    'arrival': return_final_arrival,
                    'duration': total_duration,
                    'airline': airline,
                    'stops': 1,
                    'via': via_airport
                }
                
                # Price is higher for connections
                base_price = random.randint(180, 280)
                
            else:
                # Direct flight
                outbound_departure = f'{outbound_hour:02d}:{random.randint(0, 59):02d}'
                outbound_arrival_hour = (outbound_hour + route_info['flight_time'] // 60) % 24
                outbound_arrival = f'{outbound_arrival_hour:02d}:{random.randint(0, 59):02d}'
                
                flight_duration = f'{route_info["flight_time"] // 60}h {route_info["flight_time"] % 60}m'
                
                outbound_info = {
                    'departure': outbound_departure,
                    'arrival': outbound_arrival,
                    'duration': flight_duration,
                    'airline': airline,
                    'stops': 0,
                    'via': None
                }
                
                # Return flight
                return_departure = f'{return_hour:02d}:{random.randint(0, 59):02d}'
                return_arrival_hour = (return_hour + route_info['flight_time'] // 60) % 24
                return_arrival = f'{return_arrival_hour:02d}:{random.randint(0, 59):02d}'
                
                inbound_info = {
                    'departure': return_departure,
                    'arrival': return_arrival,
                    'duration': flight_duration,
                    'airline': airline,
                    'stops': 0,
                    'via': None
                }
                
                # Direct flights are generally cheaper
                base_price = random.randint(120, 200)
            
            # Add some price variation
            price_variation = random.randint(-30, 50) if i > 0 else 0
            final_price = base_price + price_variation
            
            mock_flights.append({
                'price': final_price,
                'currency': 'GBP',
                'booking_url': f'https://example.com/book?from={departure_airport}&to={destination_airport}',
                'total_duration': outbound_info['duration'],
                'departure_airport': departure_airport,
                'outbound': outbound_info,
                'inbound': inbound_info,
                'mock_data_note': 'Flight API unavailable - showing realistic sample data'
            })
        
        # Log the realistic flights created
        route_type = "direct" if not has_connection else f"via {via_airport}"
        print(f"✅ Created {len(mock_flights)} realistic mock flights for {departure_airport} -> {destination_airport} ({route_type})")
        print(f"   Airlines: {[f['outbound']['airline'] for f in mock_flights]}")
        print(f"   Prices: £{[f['price'] for f in mock_flights]}")
        print(f"   Connections: {[f['outbound']['stops'] for f in mock_flights]} stops")
        
        return mock_flights 