try:
    import xlwings as xw
    XLWINGS_AVAILABLE = True
except ImportError:
    XLWINGS_AVAILABLE = False
    xw = None

import pandas as pd
import os
from pathlib import Path


def _export_with_pandas_fallback(spot_results: dict, filename: str):
    """Pandas-only fallback for Excel export when xlwings is not available"""
    with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
        # Summary sheet
        summary_data = []
        for spot_name, data in spot_results.items():
            windows = data['optimal_windows']
            if not windows.empty:
                best_window = windows.iloc[0]
                summary_data.append({
                    'Spot': spot_name,
                    'Best Start': best_window['start_date'].strftime('%Y-%m-%d'),
                    'Best End': best_window['end_date'].strftime('%Y-%m-%d'),
                    'Duration': f"{best_window['days']} days",
                    'Avg Score': f"{best_window['avg_score']:.1f}"
                })
            else:
                summary_data.append({
                    'Spot': spot_name,
                    'Best Start': 'No suitable windows',
                    'Best End': '', 'Duration': '', 'Avg Score': ''
                })
        
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # Individual spot sheets
        for spot_name, data in spot_results.items():
            if 'half_day_scores' in data and not data['half_day_scores'].empty:
                safe_sheet_name = spot_name.replace(' ', '_')[:31]
                data['half_day_scores'].to_excel(writer, sheet_name=safe_sheet_name, index=False)
                
                hourly_count = len(data['hourly_scored_data']) if 'hourly_scored_data' in data else 0
                print(f"Pandas: Added sheet for {spot_name} with {len(data['half_day_scores'])} half-day scores and {hourly_count} hourly scores")
    
    print(f"Surf results exported to {filename} using pandas fallback")


def export_surf_results_to_excel(spot_results: dict, filename: str):
    """
    Export surf analysis results to Excel with multiple sheets.
    
    :param spot_results: Dictionary with spot data and optimal windows
    :param filename: The name of the Excel file to create
    """
    # Ensure we use the correct absolute path in the data folder
    if not os.path.isabs(filename):
        # Use absolute path to project root
        project_root = Path("/Users/louisbrouwer/Documents/Strike_Mission")
        # Always save to data folder
        filename = str(project_root / "data" / os.path.basename(filename))
    
    print(f"Excel file will be saved to: {filename}")
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    wb = None
    
    # Skip xlwings if not available
    if not XLWINGS_AVAILABLE:
        print("xlwings not available, using pandas fallback...")
        _export_with_pandas_fallback(spot_results, filename)
        return
    
    try:
        # Close any existing workbooks
        file_basename = os.path.basename(filename)
        for book in xw.books:
            if book.name == file_basename:
                book.close()
        
        # Create/open workbook
        if os.path.exists(filename):
            wb = xw.Book(filename)
        else:
            wb = xw.Book()
            wb.save(filename)
        
        # Clear all existing sheets and create summary
        for sheet in wb.sheets:
            sheet.delete()
        
        # Create summary sheet
        summary_sheet = wb.sheets.add('Summary')
        summary_data = []
        
        for spot_name, data in spot_results.items():
            windows = data['optimal_windows']
            if not windows.empty:
                best_window = windows.iloc[0]
                summary_data.append({
                    'Spot': spot_name,
                    'Best Start': best_window['start_date'].strftime('%Y-%m-%d'),
                    'Best End': best_window['end_date'].strftime('%Y-%m-%d'),
                    'Duration': f"{best_window['days']} days",
                    'Avg Score': f"{best_window['avg_score']:.1f}",
                    'Data Source': 'Cached' if data['cache_hit'] else 'Fresh'
                })
            else:
                summary_data.append({
                    'Spot': spot_name,
                    'Best Start': 'No suitable windows',
                    'Best End': '',
                    'Duration': '',
                    'Avg Score': '',
                    'Data Source': 'Cached' if data['cache_hit'] else 'Fresh'
                })
        
        summary_df = pd.DataFrame(summary_data)
        summary_sheet.range('A1').value = summary_df
        
        # Create individual sheets for each spot
        for spot_name, data in spot_results.items():
            if 'half_day_scores' in data and not data['half_day_scores'].empty:
                # Create safe sheet name (Excel has 31 char limit and no special chars)
                safe_sheet_name = spot_name.replace(' ', '_')[:31]
                spot_sheet = wb.sheets.add(safe_sheet_name)
                
                current_row = 1
                
                # Half-day scores
                half_day_data = data['half_day_scores'].copy()
                half_day_data['date'] = pd.to_datetime(half_day_data['date'])
                
                spot_sheet.range(f'A{current_row}').value = [['Half-Day Scores']]
                spot_sheet.range(f'A{current_row + 1}').value = half_day_data
                current_row += len(half_day_data) + 3
                
                # Optimal windows (if any)
                if 'optimal_windows' in data and not data['optimal_windows'].empty:
                    windows_data = data['optimal_windows'].copy()
                    # Handle datetime columns for Excel
                    for col in ['start_date', 'end_date']:
                        if col in windows_data.columns:
                            windows_data[col] = pd.to_datetime(windows_data[col])
                    
                    spot_sheet.range(f'A{current_row}').value = [['Optimal Windows']]
                    spot_sheet.range(f'A{current_row + 1}').value = windows_data
                    current_row += len(windows_data) + 3
                
                # Add hourly scored data if available
                if 'hourly_scored_data' in data and not data['hourly_scored_data'].empty:
                    hourly_data = data['hourly_scored_data'].copy()
                    # Handle datetime column for Excel
                    if 'time' in hourly_data.columns:
                        hourly_data['time'] = pd.to_datetime(hourly_data['time'])
                    
                    spot_sheet.range(f'A{current_row}').value = [['Hourly Scored Forecast']]
                    spot_sheet.range(f'A{current_row + 1}').value = hourly_data
                    
                    print(f"Added sheet for {spot_name} with {len(half_day_data)} half-day scores and {len(hourly_data)} hourly scores")
                else:
                    print(f"Added sheet for {spot_name} with {len(half_day_data)} half-day scores")
        
        wb.save(filename)
        print(f"Surf results exported to {filename}")
        
    except Exception as e:
        print(f"xlwings failed: {e}. Falling back to pandas...")
        if wb:
            try:
                wb.close()
            except:
                pass
        
        # Pandas fallback
        with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
            # Summary sheet
            summary_data = []
            for spot_name, data in spot_results.items():
                windows = data['optimal_windows']
                if not windows.empty:
                    best_window = windows.iloc[0]
                    summary_data.append({
                        'Spot': spot_name,
                        'Best Start': best_window['start_date'].strftime('%Y-%m-%d'),
                        'Best End': best_window['end_date'].strftime('%Y-%m-%d'),
                        'Duration': f"{best_window['days']} days",
                        'Avg Score': f"{best_window['avg_score']:.1f}"
                    })
                else:
                    summary_data.append({
                        'Spot': spot_name,
                        'Best Start': 'No suitable windows',
                        'Best End': '', 'Duration': '', 'Avg Score': ''
                    })
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Individual spot sheets
            for spot_name, data in spot_results.items():
                if 'half_day_scores' in data and not data['half_day_scores'].empty:
                    safe_sheet_name = spot_name.replace(' ', '_')[:31]
                    
                    # Create workbook sheet with multiple sections
                    with pd.ExcelWriter(filename.replace('.xlsx', f'_{safe_sheet_name}_detail.xlsx'), engine='openpyxl') as detail_writer:
                        # Half-day scores
                        data['half_day_scores'].to_excel(detail_writer, sheet_name='Half_Day_Scores', index=False)
                        
                        # Optimal windows
                        if 'optimal_windows' in data and not data['optimal_windows'].empty:
                            data['optimal_windows'].to_excel(detail_writer, sheet_name='Optimal_Windows', index=False)
                        
                        # Hourly scored data
                        if 'hourly_scored_data' in data and not data['hourly_scored_data'].empty:
                            data['hourly_scored_data'].to_excel(detail_writer, sheet_name='Hourly_Scores', index=False)
                    
                    # Also add summary to main file
                    data['half_day_scores'].to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    
                    hourly_count = len(data['hourly_scored_data']) if 'hourly_scored_data' in data else 0
                    print(f"Pandas: Added sheet for {spot_name} with {len(data['half_day_scores'])} half-day scores and {hourly_count} hourly scores")


def export_to_excel(data: pd.DataFrame, filename: str, sheet_name: str = 'Sheet1'):
    """
    Export a pandas DataFrame to an Excel file using xlwings for live updates.
    Falls back to pandas if xlwings is not available.

    :param data: DataFrame containing the data to export.
    :param filename: The name of the Excel file to create.
    :param sheet_name: The name of the sheet in the Excel file.
    """
    # Use pandas fallback if xlwings not available
    if not XLWINGS_AVAILABLE:
        print("xlwings not available, using pandas for Excel export...")
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Data exported to {filename} using pandas")
        return
    # Ensure we use the correct absolute path
    if not os.path.isabs(filename):
        # Always save to project root
        project_root = Path(__file__).parent.parent.parent
        filename = str(project_root / filename)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Create a copy to avoid modifying the original data
    data_copy = data.copy()
    
    # Convert timezone-aware datetime columns to timezone-naive (for Excel compatibility)
    for col in data_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(data_copy[col]):
            if hasattr(data_copy[col].dtype, 'tz') and data_copy[col].dtype.tz is not None:
                # Convert to UTC then remove timezone info
                data_copy[col] = data_copy[col].dt.tz_convert('UTC').dt.tz_localize(None)
    
    wb = None
    try:
        # Close any existing workbooks with the same name to avoid conflicts
        file_basename = os.path.basename(filename)
        for book in xw.books:
            if book.name == file_basename:
                book.close()
        
        # Try to open existing workbook first
        if os.path.exists(filename):
            wb = xw.Book(filename)
        else:
            # Create new workbook and save it immediately
            wb = xw.Book()
            wb.save(filename)
        
        # Check if the sheet exists, if not, create it
        sheet_names = [sheet.name for sheet in wb.sheets]
        if sheet_name in sheet_names:
            sheet = wb.sheets[sheet_name]
            # Clear existing content
            sheet.clear()
        else:
            sheet = wb.sheets.add(sheet_name)

        # Update the data
        sheet.range('A1').value = data_copy
        
        # Save the workbook
        wb.save(filename)
        
        print(f"Data exported to {filename}, sheet '{sheet_name}'")
        
    except Exception as e:
        print(f"xlwings failed: {e}. Falling back to pandas...")
        # Close any workbook that might have been opened
        if wb:
            try:
                wb.close()
            except:
                pass
        
        # Fallback to pandas ExcelWriter
        file_exists = os.path.exists(filename)
        
        if file_exists:
            # Read existing file to preserve other sheets
            try:
                with pd.ExcelFile(filename) as xls:
                    existing_sheets = {}
                    for existing_sheet in xls.sheet_names:
                        if existing_sheet != sheet_name:  # Don't preserve the sheet we're updating
                            existing_sheets[existing_sheet] = pd.read_excel(xls, sheet_name=existing_sheet)
                
                # Write all sheets (existing + new/updated)
                with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                    # Write the new/updated sheet first
                    data_copy.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Write existing sheets
                    for sheet, df in existing_sheets.items():
                        df.to_excel(writer, sheet_name=sheet, index=False)
                        
            except Exception as fallback_error:
                print(f"Warning: Could not preserve existing sheets: {fallback_error}")
                # Final fallback: just write the new sheet
                with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                    data_copy.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            # Create new file
            with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                data_copy.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"Data exported to {filename}, sheet '{sheet_name}' (pandas fallback)")


# Example usage
if __name__ == "__main__":
    # Create a sample DataFrame
    df = pd.DataFrame({
        'Date': ['2023-10-01', '2023-10-02'],
        'Swell Height': [1.5, 2.0],
        'Wind Speed': [2000, 12]
    })
    
    # Export to Excel
    export_to_excel(df, 'surf_data.xlsx') 