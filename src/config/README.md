# Configuration System

This directory contains the configuration files for surf destinations and user presets.

## Files

### `surf_destinations.py`
Comprehensive surf spot database with:
- Geographic coordinates and timezone
- Swell and wind direction preferences  
- Destination airports (primary + alternatives)
- Drive times from airports
- Currently includes **11 surf spots** (9 European + 2 international)

### `user_presets.py` 
Simple user preference structure that mirrors frontend form data:
- **Departure airports**: Where user flies from
- **Selected spots**: Which surf spots to consider
- **Minimum score**: Quality threshold for trips
- **Trip duration**: Min/max days

## Usage

### Testing Presets
```bash
# Test all configurations
python test_user_presets.py

# Interactive testing
python test_user_presets.py --interactive
```

### Using in Code
```python
from config.user_presets import get_preset, create_analysis_params

# Get a preset
preset = get_preset("london_weekend")

# Convert to analysis parameters
params = create_analysis_params("london_weekend")

# Or use custom data (simulating frontend)
custom_data = {
    "departure_airports": ["LHR", "LGW"],
    "selected_spots": ["La Graviere", "Supertubes"],
    "min_score": 4.0,
    "min_days": 3,
    "max_days": 5
}
params = create_analysis_params(custom_data)
```

## Current Presets

1. **London Weekend Warrior**: Short European trips from London airports
2. **Amsterdam Explorer**: Week-long trips from Amsterdam 
3. **Dublin Surfer**: Mixed European spots from Dublin
4. **Premium Wave Chaser**: High-end travel, international spots
5. **Budget Student**: Budget-friendly options

## Frontend Integration

The `user_presets.py` structure is designed to be easily replaced by frontend form data. The validation and parameter conversion functions work with both preset names and direct user data dictionaries. 