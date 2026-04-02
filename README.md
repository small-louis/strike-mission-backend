# Strike Mission

Python backend for a surf trip planning app. Fetches real-time wave, wind, and weather forecasts for surf spots across Europe and North Africa, scores conditions against spot-specific criteria, and finds optimal travel windows with flight pricing.

Built with FastAPI. Deployed on Railway with Docker.

## How it works

1. **Forecast data** — Pulls marine and weather forecasts from the Open-Meteo API for each configured surf spot (wave height, period, direction, swell components, wind, temperature).

2. **Scoring engine** — Rates each half-day window on a scale from "Flat" to "Epic" based on wave size, swell period, wind direction (offshore vs onshore), and spot-specific thresholds.

3. **Trip windows** — Identifies the best multi-day travel windows by aggregating half-day scores and factoring in consistency.

4. **Flight search** — Finds flights from your home airport to destination airports near each spot, so you can compare trip cost against forecast quality.

## API

```
GET  /api/spots                          — List all configured surf spots
GET  /api/forecast/{spot_name}           — Scored forecast for a spot
GET  /api/optimal-windows/{spot_name}    — Best travel windows
GET  /api/flights                        — Flight prices to surf destinations
POST /api/refresh                        — Refresh cached forecast data
```

## Project structure

```
backend_api.py              — FastAPI app and route handlers
run_production.py           — Production server entry point
Dockerfile                  — Container config for Railway
src/
  scoring/                  — Wave scoring and surf rating logic
  data_fetching/            — Open-Meteo API client
  flights/                  — Flight price fetcher
  surf_analysis/            — Condition analysis
  window_selection/         — Optimal travel window selection
  services/                 — Data refresh orchestration
  utils/                    — Caching, DB, spot configs, data processing
  config/                   — User presets
```

## Setup

```bash
pip install -r requirements-backend.txt
python backend_api.py
```

The API runs at `http://localhost:8000`. Interactive docs at `/docs`.

## Deployment

```bash
docker build -t strike-mission .
docker run -p 8000:8000 strike-mission
```

## More

Project page: [louisbrouwer.com/supergreen.html](https://louisbrouwer.com/supergreen.html)
