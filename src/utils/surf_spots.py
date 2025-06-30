"""
Surf Spots Configuration

All surf spots with their geographic data, scoring parameters, and primary airport.
"""

surf_spots = {
    "La Graviere": {
        "lat": 43.676,
        "lon": -1.445,
        "swell_dir_range": (200, 340),
        "wind_dir_range": (45, 135),
        "timezone": "Europe/Paris",
        "primary_airport": "BOD"  # Bordeaux
    },
    "Supertubos": {
        "lat": 39.604,
        "lon": -9.366,
        "swell_dir_range": (280, 320),
        "wind_dir_range": (10, 130),
        "timezone": "Europe/Lisbon",
        "primary_airport": "LIS"  # Lisbon
    },
    "Uluwatu": {
        "lat": -8.814518,
        "lon": 115.086847,
        "swell_dir_range": (180, 270),
        "wind_dir_range": (45, 135),
        "timezone": "Asia/Jakarta",
        "primary_airport": "DPS"  # Denpasar
    },
    "Anchor Point": {
        "lat": 30.544176,
        "lon": -9.727859,
        "swell_dir_range": (260, 350),
        "wind_dir_range": (340, 60),
        "timezone": "Africa/Casablanca",
        "primary_airport": "AGA"  # Agadir
    },
    "Mundaka": {
        "lat": 43.408,
        "lon": -2.691,
        "swell_dir_range": (280, 340),
        "wind_dir_range": (90, 180),
        "timezone": "Europe/Madrid",
        "primary_airport": "BIO"  # Bilbao
    }
}


def get_destination_airports():
    """
    Get all unique destination airports from surf spots.
    Simple function for flight API integration.
    """
    airports = set()
    for spot_data in surf_spots.values():
        airports.add(spot_data["primary_airport"])
    return sorted(list(airports))


if __name__ == "__main__":
    print("Surf Spots with Primary Airports")
    print("=" * 40)
    
    for spot_name, spot_data in surf_spots.items():
        airport = spot_data["primary_airport"]
        print(f"{spot_name} → {airport}")
    
    print(f"\nDestination airports: {', '.join(get_destination_airports())}")
    print(f"Total: {len(get_destination_airports())} unique airports") 