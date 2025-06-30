# Strike Mission - Surf Trip Planner

A surf trip planning application that matches good surf opportunities with cheap flight prices.

## Project Structure

```
src/
├── flights/                    # Flight data fetching and processing
│   ├── flight_fetcher.py      # Kiwi.com API calls (functional approach)
│   └── test_flight_fetcher.py # Flight data export to Excel/JSON
│
├── surf_analysis/             # Surf forecasting and trip planning
│   ├── main.py               # Main surf analysis pipeline
│   └── window_selection.py   # Optimal surf window selection
│
├── data_fetching/             # Weather and marine data APIs
│   └── openmeteo.py          # OpenMeteo API integration
│
├── scoring/                   # Surf condition scoring algorithms
│   ├── wave_scoring.py       # Wave quality scoring
│   └── half_daily_scoring.py # Half-day session scoring
│
├── utils/                     # Utility functions
│   ├── surf_spots.py         # Surf spot definitions
│   ├── data_processor.py     # Data processing utilities
│   ├── export_processor.py   # Export handling
│   └── excel_export.py       # Excel export utilities
│
├── scripts/                   # Standalone scripts
│   └── fetch_score_forecast.py # Weather fetching script
│
└── data/                      # Data files (Excel, JSON)
    ├── surf_data.xlsx         # Surf forecast data
    ├── flight_data_raw.xlsx   # Flight data (organized)
    └── flight_data_raw.json   # Flight data (raw API response)
```

## Quick Start

### Flight Data
```bash
cd src/flights
export KIWI_API_KEY="your_api_key_here"
python test_flight_fetcher.py
```

### Surf Analysis
```bash
cd src/surf_analysis  
python main.py
```

## Key Features

- **Flight Data**: Fetches direct flights from London to European surf destinations via Kiwi.com API
- **Surf Forecasting**: Integrates OpenMeteo weather/marine data with custom scoring algorithms
- **Trip Optimization**: Identifies optimal surf windows and matches them with flight availability
- **Excel Export**: All data exported to organized Excel sheets for analysis

## Configuration

- **Flight API**: Uses Kiwi.com via RapidAPI
- **Weather API**: Uses OpenMeteo (free, no API key required)
- **Data Format**: Excel files with multiple sheets for different data views
- **File Structure**: Fixed filenames (no timestamps) for easy iteration

## Current Status

✅ Flight data fetching working (London ↔ Lisbon/Amsterdam)  
✅ Surf forecasting and scoring algorithms implemented  
✅ Excel export system functional  
✅ Window selection algorithms complete  
🔄 Flight-surf matching integration (in progress) 