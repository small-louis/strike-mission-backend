"""
Simple User Presets - Frontend Data Structure

Basic user preferences that mirror what would be submitted from a front-end form.
This structure can easily be replaced by actual user input data.
"""

# Available surf spots (should match what's in src/utils/surf_spots.py)
AVAILABLE_SPOTS = [
    "La Graviere", "Supertubes", "Uluwatu", "Anchor Point", "Thurso East",
    "Hossegor", "Ericeira", "Mundaka", "Sopelana", "Croyde", "Bundoran"
]

# Available airports (common departure points)
DEPARTURE_AIRPORTS = {
    "london": ["LHR", "LGW", "STN", "LTN"],
    "amsterdam": ["AMS"],
    "dublin": ["DUB"],
    "paris": ["CDG", "ORY", "BVA"],
    "zurich": ["ZUR"],
    "budget": ["STN", "BVA", "LTN"]  # Budget airports
}

# Standard flight time preferences (same for all users for now)
FLIGHT_TIMES = {
    "outbound_full_day": "19:00+",  # After 7pm night before if window starts with full day
    "outbound_afternoon": ["19:00+", "09:00"],  # 7pm+ night before OR up to 9am day of
    "return": "17:00+"  # After 5pm on departure day
}

# Simple user presets - mimics frontend form submission
USER_PRESETS = {
    "london_weekend": {
        "name": "London Weekend Warrior",
        "departure_airports": ["LHR", "LGW", "STN"],
        "selected_spots": ["La Graviere", "Hossegor", "Supertubes", "Ericeira"],
        "min_score": 4.0,
        "min_days": 3,
        "max_days": 5,
        "flight_times": FLIGHT_TIMES,
        "stopovers_allowed": False
    },
    
    "amsterdam_explorer": {
        "name": "Amsterdam Explorer", 
        "departure_airports": ["AMS"],
        "selected_spots": ["La Graviere", "Supertubes", "Ericeira", "Mundaka"],
        "min_score": 3.5,
        "min_days": 6,
        "max_days": 10,
        "flight_times": FLIGHT_TIMES,
        "stopovers_allowed": False
    },
    
    "dublin_surfer": {
        "name": "Dublin Surfer",
        "departure_airports": ["DUB"],
        "selected_spots": ["La Graviere", "Supertubes", "Bundoran", "Croyde"],
        "min_score": 3.8,
        "min_days": 4,
        "max_days": 8,
        "flight_times": FLIGHT_TIMES,
        "stopovers_allowed": False
    },
    
    "premium_chaser": {
        "name": "Premium Wave Chaser",
        "departure_airports": ["ZUR", "LHR", "CDG"],
        "selected_spots": ["Uluwatu", "Anchor Point", "Supertubes", "Thurso East"],
        "min_score": 4.5,
        "min_days": 7,
        "max_days": 14,
        "flight_times": FLIGHT_TIMES,
        "stopovers_allowed": False
    },
    
    "budget_student": {
        "name": "Budget Student",
        "departure_airports": ["STN", "BVA", "LTN"],
        "selected_spots": ["Ericeira", "Supertubes", "Anchor Point"],
        "min_score": 3.0,
        "min_days": 5,
        "max_days": 12,
        "flight_times": FLIGHT_TIMES,
        "stopovers_allowed": False
    }
}


def get_preset(preset_name):
    """Get a user preset by name."""
    return USER_PRESETS.get(preset_name)


def list_presets():
    """List all available presets."""
    return {name: preset["name"] for name, preset in USER_PRESETS.items()}


def validate_preset(preset_data):
    """
    Validate preset data structure (same validation we'd use for frontend data).
    
    :param preset_data: Dictionary with user preferences
    :return: Boolean indicating if valid
    """
    required_fields = ["departure_airports", "selected_spots", "min_score", "min_days", "max_days", "flight_times", "stopovers_allowed"]
    
    for field in required_fields:
        if field not in preset_data:
            return False, f"Missing required field: {field}"
    
    if not isinstance(preset_data["departure_airports"], list) or not preset_data["departure_airports"]:
        return False, "departure_airports must be a non-empty list"
    
    if not isinstance(preset_data["selected_spots"], list) or not preset_data["selected_spots"]:
        return False, "selected_spots must be a non-empty list"
    
    # Check if selected spots are valid
    invalid_spots = [spot for spot in preset_data["selected_spots"] if spot not in AVAILABLE_SPOTS]
    if invalid_spots:
        return False, f"Invalid spots: {invalid_spots}. Available: {AVAILABLE_SPOTS}"
    
    if not isinstance(preset_data["min_score"], (int, float)) or preset_data["min_score"] < 0:
        return False, "min_score must be a positive number"
    
    if not isinstance(preset_data["min_days"], int) or preset_data["min_days"] < 1:
        return False, "min_days must be a positive integer"
    
    if not isinstance(preset_data["max_days"], int) or preset_data["max_days"] < preset_data["min_days"]:
        return False, "max_days must be >= min_days"
    
    if not isinstance(preset_data["flight_times"], dict):
        return False, "flight_times must be a dictionary"
    
    if not isinstance(preset_data["stopovers_allowed"], bool):
        return False, "stopovers_allowed must be a boolean"
    
    return True, "Valid"


def create_analysis_params(preset_data):
    """
    Convert preset data to analysis parameters.
    This function works with both preset names and direct preset data.
    """
    # If it's a preset name, get the preset data
    if isinstance(preset_data, str):
        preset_data = get_preset(preset_data)
        if not preset_data:
            raise ValueError(f"Unknown preset: {preset_data}")
    
    # Validate the data
    is_valid, message = validate_preset(preset_data)
    if not is_valid:
        raise ValueError(f"Invalid preset data: {message}")
    
    # Return simple analysis parameters
    return {
        "departure_airports": preset_data["departure_airports"],
        "selected_spots": preset_data["selected_spots"], 
        "min_score": preset_data["min_score"],
        "min_days": preset_data["min_days"],
        "max_days": preset_data["max_days"],
        "flight_times": preset_data["flight_times"],
        "stopovers_allowed": preset_data["stopovers_allowed"]
    }


def get_flight_times_for_window(window_start_half_day):
    """
    Get appropriate flight times based on when the surf window starts.
    
    :param window_start_half_day: "AM" or "PM" indicating when window starts
    :return: Dict with outbound and return flight time constraints
    """
    if window_start_half_day == "AM":
        # Full day of surfing - fly after 7pm night before
        return {
            "outbound_options": [FLIGHT_TIMES["outbound_full_day"]],
            "return": FLIGHT_TIMES["return"],
            "reasoning": "Window starts with full day - fly night before after 7pm"
        }
    else:
        # Afternoon start - can fly night before or morning of
        return {
            "outbound_options": FLIGHT_TIMES["outbound_afternoon"],
            "return": FLIGHT_TIMES["return"],
            "reasoning": "Window starts afternoon - fly night before 7pm+ OR morning up to 9am"
        }


if __name__ == "__main__":
    print("Simple User Presets with Flight Times")
    print("=" * 45)
    print(f"Available spots: {len(AVAILABLE_SPOTS)}")
    print(f"Available presets: {len(USER_PRESETS)}")
    
    # Show flight time rules
    print(f"\nFlight Time Rules (same for all users):")
    print(f"  • Full day start: Fly after {FLIGHT_TIMES['outbound_full_day']} night before")
    print(f"  • Afternoon start: Fly after {FLIGHT_TIMES['outbound_afternoon'][0]} night before OR up to {FLIGHT_TIMES['outbound_afternoon'][1]} day of")
    print(f"  • Return flights: After {FLIGHT_TIMES['return']}")
    
    for preset_key, preset_name in list_presets().items():
        print(f"\n{preset_name}:")
        preset = get_preset(preset_key)
        print(f"  Airports: {', '.join(preset['departure_airports'])}")
        print(f"  Spots: {', '.join(preset['selected_spots'])}")
        print(f"  Min Score: {preset['min_score']}")
        print(f"  Duration: {preset['min_days']}-{preset['max_days']} days")
        print(f"  Flight times: ✅ Standard rules")
        print(f"  Stopovers: {'✅ Allowed' if preset['stopovers_allowed'] else '❌ Direct only'}")
        
        # Test validation
        is_valid, msg = validate_preset(preset)
        print(f"  Valid: {is_valid}")
        
        # Test parameter conversion
        try:
            params = create_analysis_params(preset_key)
            print(f"  Analysis params: ✅")
            
            # Test flight time logic
            am_times = get_flight_times_for_window("AM")
            pm_times = get_flight_times_for_window("PM")
            print(f"  AM start: {am_times['outbound_options']}")
            print(f"  PM start: {pm_times['outbound_options']}")
        except Exception as e:
            print(f"  Analysis params: ❌ {e}") 