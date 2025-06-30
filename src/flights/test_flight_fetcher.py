from flight_fetcher import fetch_flights
import json
import requests
import pandas as pd
from datetime import datetime

def export_raw_data(data):
    """
    Export raw flight data to Excel and JSON for easy exploration.
    No analysis, just clear display of the raw structure.
    """
    if not data:
        print("No data to export")
        return
    
    # Save complete raw JSON (overwrite existing)
    json_filename = "../data/flight_data_raw.json"
    with open(json_filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved raw JSON to: {json_filename}")
    
    # Create Excel file with raw data structure (overwrite existing)
    excel_filename = "../data/flight_data_raw.xlsx"
    
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        
        # Sheet 1: Refined Data (NEW)
        if 'itineraries' in data and data['itineraries']:
            refined_data = []
            for i, itinerary in enumerate(data['itineraries']):
                try:
                    # Basic info
                    price = float(itinerary['price']['amount'])
                    
                    # Outbound flight info
                    outbound_segment = itinerary['outbound']['sectorSegments'][0]['segment']
                    outbound_dep_airport = outbound_segment['source']['station']['code']
                    outbound_dep_time = outbound_segment['source']['localTime']
                    outbound_arr_airport = outbound_segment['destination']['station']['code']
                    outbound_arr_time = outbound_segment['destination']['localTime']
                    outbound_carrier = outbound_segment['carrier']['name']
                    
                    # Inbound flight info
                    inbound_segment = itinerary['inbound']['sectorSegments'][0]['segment']
                    inbound_dep_airport = inbound_segment['source']['station']['code']
                    inbound_dep_time = inbound_segment['source']['localTime']
                    inbound_arr_airport = inbound_segment['destination']['station']['code']
                    inbound_arr_time = inbound_segment['destination']['localTime']
                    inbound_carrier = inbound_segment['carrier']['name']
                    
                    refined_data.append({
                        'Price_USD': price,
                        'Outbound_Departure_Airport': outbound_dep_airport,
                        'Outbound_Departure_Time': outbound_dep_time,
                        'Outbound_Arrival_Airport': outbound_arr_airport,
                        'Outbound_Arrival_Time': outbound_arr_time,
                        'Outbound_Carrier': outbound_carrier,
                        'Inbound_Departure_Airport': inbound_dep_airport,
                        'Inbound_Departure_Time': inbound_dep_time,
                        'Inbound_Arrival_Airport': inbound_arr_airport,
                        'Inbound_Arrival_Time': inbound_arr_time,
                        'Inbound_Carrier': inbound_carrier
                    })
                except (KeyError, IndexError, ValueError) as e:
                    continue
            
            if refined_data:
                df_refined = pd.DataFrame(refined_data)
                df_refined.to_excel(writer, sheet_name='Refined_Data', index=False)
        
        # Sheet 2: Top-level overview
        overview_data = []
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                overview_data.append({
                    'Key': key,
                    'Type': type(value).__name__,
                    'Length_or_Count': len(value) if hasattr(value, '__len__') else 'N/A',
                    'Sample_Value': str(value)[:100] + '...' if len(str(value)) > 100 else str(value)
                })
            else:
                overview_data.append({
                    'Key': key,
                    'Type': type(value).__name__,
                    'Length_or_Count': 'N/A',
                    'Sample_Value': str(value)
                })
        
        df_overview = pd.DataFrame(overview_data)
        df_overview.to_excel(writer, sheet_name='Data_Overview', index=False)
        
        # Sheet 3: Raw itineraries (if present)
        if 'itineraries' in data and data['itineraries']:
            itineraries_raw = []
            for i, itinerary in enumerate(data['itineraries']):
                # Flatten the itinerary structure for Excel viewing
                flat_row = {'Itinerary_Index': i}
                
                def flatten_dict(d, parent_key='', sep='_'):
                    items = []
                    for k, v in d.items():
                        new_key = f"{parent_key}{sep}{k}" if parent_key else k
                        if isinstance(v, dict):
                            items.extend(flatten_dict(v, new_key, sep=sep).items())
                        elif isinstance(v, list) and v and isinstance(v[0], dict):
                            # For lists of dicts, just take the first item or count
                            items.append((new_key + '_count', len(v)))
                            if v:
                                items.extend(flatten_dict(v[0], new_key + '_first', sep=sep).items())
                        else:
                            items.append((new_key, str(v) if v is not None else ''))
                    return dict(items)
                
                flat_itinerary = flatten_dict(itinerary)
                flat_row.update(flat_itinerary)
                itineraries_raw.append(flat_row)
            
            if itineraries_raw:
                df_itineraries = pd.DataFrame(itineraries_raw)
                df_itineraries.to_excel(writer, sheet_name='Raw_Itineraries', index=False)
        
        # Sheet 4: Metadata (if present)
        if 'metadata' in data and data['metadata']:
            metadata_items = []
            
            def extract_metadata(obj, prefix=''):
                items = []
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        full_key = f"{prefix}_{key}" if prefix else key
                        if isinstance(value, (dict, list)):
                            items.extend(extract_metadata(value, full_key))
                        else:
                            items.append({
                                'Field': full_key,
                                'Value': str(value),
                                'Type': type(value).__name__
                            })
                elif isinstance(obj, list):
                    items.append({
                        'Field': prefix,
                        'Value': f"List with {len(obj)} items",
                        'Type': 'list'
                    })
                    if obj and not isinstance(obj[0], (dict, list)):
                        items.append({
                            'Field': f"{prefix}_sample",
                            'Value': str(obj[0]) if obj else '',
                            'Type': 'sample'
                        })
                return items
            
            metadata_items = extract_metadata(data['metadata'])
            if metadata_items:
                df_metadata = pd.DataFrame(metadata_items)
                df_metadata.to_excel(writer, sheet_name='Raw_Metadata', index=False)
    
    print(f"✅ Exported raw data structure to: {excel_filename}")
    
    # Simple summary
    if 'itineraries' in data:
        print(f"📊 Contains {len(data['itineraries'])} itineraries")
    print(f"🔍 Data has {len(data.keys())} top-level keys: {list(data.keys())}")

def main():
    # Fetch flights
    print("🔍 Fetching flight data...")
    data = fetch_flights()

    if data:
        print(f"📥 Received data")
        export_raw_data(data)
    else:
        print("❌ No data returned from API")

if __name__ == '__main__':
    main() 