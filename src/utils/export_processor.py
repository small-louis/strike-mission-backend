from .excel_export import export_to_excel

def export_surf_data(scored_data=None, half_day_scores=None, daily_weather=None, spot_name=None, output_file='../data/surf_data.xlsx', windows_df=None):
    """
    Export surf data to Excel.
    
    :param scored_data: DataFrame with scored hourly forecast data
    :param half_day_scores: DataFrame with half-day scores
    :param daily_weather: DataFrame with daily weather data
    :param spot_name: Name of the surf spot
    :param output_file: Name of the output Excel file
    :param windows_df: Optional DataFrame with optimal surf windows to export
    :return: None
    """
    # Only export basic spot data if they're provided and spot_name is provided
    if spot_name is not None:
        # 1. Hourly forecast with ratings
        if scored_data is not None:
            export_to_excel(scored_data, output_file, f'{spot_name}_Forecast')
        
        # 2. Half-daily ratings
        if half_day_scores is not None:
            export_to_excel(half_day_scores, output_file, f'{spot_name}_Half_Day')
        
        # 3. Daily weather data
        if daily_weather is not None:
            export_to_excel(daily_weather, output_file, f'{spot_name}_Daily')
        
        if scored_data is not None or half_day_scores is not None or daily_weather is not None:
            print(f"Data exported for {spot_name} to {output_file}")
    
    # 4. Optimal windows (if provided)
    if windows_df is not None and not windows_df.empty:
        export_to_excel(windows_df, output_file, 'Optimal_Windows')
        print(f"Optimal windows exported to {output_file} in sheet 'Optimal_Windows'") 