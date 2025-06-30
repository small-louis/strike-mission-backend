import pandas as pd
import numpy as np
from datetime import timedelta


def find_weekend_windows(half_day_scores, strict_weekend=False):
    """
    Find weekend windows (Friday to Monday) in the half-day scores.
    
    :param half_day_scores: DataFrame with half-day scores
    :param strict_weekend: If True, only include Friday-Sunday (2 days), otherwise Friday-Monday (3 days)
    :return: List of dictionaries with window information (start_date, end_date, avg_score, days)
    """
    # Convert date to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(half_day_scores['date']):
        half_day_scores['date'] = pd.to_datetime(half_day_scores['date'])

    # Get unique dates and sort them
    unique_dates = sorted(half_day_scores['date'].dt.date.unique())
    
    weekend_windows = []
    
    for i, date in enumerate(unique_dates):
        # Convert to pandas datetime for easier manipulation
        current_date = pd.to_datetime(date)
        
        # Check if it's a Friday (weekday=4)
        if current_date.weekday() == 4:  # Friday
            # Determine end date (Sunday or Monday)
            if strict_weekend:
                end_date = current_date + timedelta(days=2)  # Sunday
            else:
                end_date = current_date + timedelta(days=3)  # Monday
            
            # Convert end_date to date object for comparison
            end_date = end_date.date()
            
            # Check if the end date is in our dataset
            if end_date in unique_dates:
                # Filter scores for this window
                window_start = current_date.date()
                window_scores = half_day_scores[
                    (half_day_scores['date'].dt.date >= window_start) & 
                    (half_day_scores['date'].dt.date <= end_date)
                ]
                
                # Calculate average score
                avg_score = window_scores['avg_total_points'].mean()
                
                # Calculate number of days
                days = (end_date - window_start).days + 1
                
                weekend_windows.append({
                    'start_date': window_start,
                    'end_date': end_date,
                    'avg_score': round(avg_score, 2),
                    'type': 'weekend',
                    'days': days
                })
    
    # Sort windows by average score
    weekend_windows.sort(key=lambda x: x['avg_score'], reverse=True)
    
    return weekend_windows


def find_best_window(half_day_scores, max_window_days=7):
    """
    Find the best surf window in the forecast period.
    
    Algorithm:
    1. Start with the highest 2-day average score
    2. Expand window as long as surrounding days are within 1.5 points of the initial average
    3. Cap the window at the minimum of:
       a. 7 days (max_window_days)
       b. The initial 2-day average score (rounded DOWN to nearest integer)
    
    :param half_day_scores: DataFrame with half-day scores
    :param max_window_days: Maximum window length in days
    :return: Dictionary with the best window information (start_date, end_date, avg_score, days)
    """
    # Convert date to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(half_day_scores['date']):
        half_day_scores['date'] = pd.to_datetime(half_day_scores['date'])

    # Get unique dates and sort them
    unique_dates = sorted(half_day_scores['date'].dt.date.unique())
    
    # If we have less than 2 days, we can't find a window
    if len(unique_dates) < 2:
        return None
    
    best_window = None
    best_avg_score = -1
    
    # Try each possible 2-day window as a starting point
    for i in range(len(unique_dates) - 1):
        start_date = unique_dates[i]
        next_date = unique_dates[i + 1]
        
        # Check if these dates are consecutive
        if (next_date - start_date).days != 1:
            continue
            
        # Get scores for initial 2-day window
        initial_window_scores = half_day_scores[
            (half_day_scores['date'].dt.date >= start_date) & 
            (half_day_scores['date'].dt.date <= next_date)
        ]
        
        initial_avg_score = initial_window_scores['avg_total_points'].mean()
        
        # Calculate the max days allowed for this window based on the initial score
        # Floor the initial average score to get the max days
        score_based_max_days = int(np.floor(initial_avg_score))
        
        # Use the more restrictive of the two max days
        effective_max_days = min(max_window_days, score_based_max_days)
        
        # If this initial window is better than our current best, explore expanding it
        if initial_avg_score > best_avg_score:
            # Start with the 2-day window
            current_start = start_date
            current_end = next_date
            current_scores = initial_window_scores
            current_avg = initial_avg_score
            threshold = initial_avg_score - 1.5  # Threshold for inclusion
            
            # Try expanding forward
            for j in range(i + 2, len(unique_dates)):
                possible_end = unique_dates[j]
                
                # Check if we've reached the maximum window size (the smaller of max_window_days or initial score)
                if (possible_end - current_start).days >= effective_max_days:
                    break
                    
                # Check if the date is consecutive
                if (possible_end - current_end).days != 1:
                    break
                
                # Get scores for the next day
                next_day_scores = half_day_scores[half_day_scores['date'].dt.date == possible_end]
                
                # Check if this day meets our threshold
                next_day_avg = next_day_scores['avg_total_points'].mean()
                if next_day_avg >= threshold:
                    # Add this day to our window
                    current_end = possible_end
                    current_scores = pd.concat([current_scores, next_day_scores])
                    current_avg = current_scores['avg_total_points'].mean()
                else:
                    break
            
            # Try expanding backward
            for j in range(i - 1, -1, -1):
                possible_start = unique_dates[j]
                
                # Check if we've reached the maximum window size (the smaller of max_window_days or initial score)
                if (current_end - possible_start).days >= effective_max_days:
                    break
                    
                # Check if the date is consecutive
                if (current_start - possible_start).days != 1:
                    break
                
                # Get scores for the previous day
                prev_day_scores = half_day_scores[half_day_scores['date'].dt.date == possible_start]
                
                # Check if this day meets our threshold
                prev_day_avg = prev_day_scores['avg_total_points'].mean()
                if prev_day_avg >= threshold:
                    # Add this day to our window
                    current_start = possible_start
                    current_scores = pd.concat([prev_day_scores, current_scores])
                    current_avg = current_scores['avg_total_points'].mean()
                else:
                    break
            
            # Update best window if this one is better
            window_days = (current_end - current_start).days + 1
            if current_avg > best_avg_score and 2 <= window_days <= effective_max_days:
                best_avg_score = current_avg
                best_window = {
                    'start_date': current_start,
                    'end_date': current_end,
                    'avg_score': round(current_avg, 2),
                    'type': 'best',
                    'days': window_days
                }
    
    return best_window


def identify_surf_windows(half_day_scores, spot_name, strict_weekend=False, max_window_days=7):
    """
    Identify the best surf windows for a spot.
    
    This includes:
    1. Weekend windows (Friday to Monday or Friday to Sunday)
    2. Best overall window in the forecast period
    
    :param half_day_scores: DataFrame with half-day scores
    :param spot_name: Name of the surf spot
    :param strict_weekend: If True, only include Friday-Sunday, otherwise Friday-Monday
    :param max_window_days: Maximum window length in days
    :return: List of dictionaries with window information
    """
    # Find weekend windows
    weekend_windows = find_weekend_windows(half_day_scores, strict_weekend)
    
    # Find best overall window
    best_window = find_best_window(half_day_scores, max_window_days)
    
    # Combine windows
    all_windows = weekend_windows.copy()
    if best_window:
        all_windows.append(best_window)
    
    # Sort windows by average score
    all_windows.sort(key=lambda x: x['avg_score'], reverse=True)
    
    # Add spot information
    for window in all_windows:
        window['spot'] = spot_name
    
    return all_windows


def select_optimal_trips(spot_data_dict, strict_weekend=False, max_window_days=7):
    """
    Select optimal trips across all surf spots.
    
    :param spot_data_dict: Dictionary with spot names as keys and half-day scores as values
    :param strict_weekend: If True, only include Friday-Sunday, otherwise Friday-Monday
    :param max_window_days: Maximum window length in days
    :return: DataFrame with optimal trips
    """
    all_windows = []
    
    for spot_name, half_day_scores in spot_data_dict.items():
        spot_windows = identify_surf_windows(
            half_day_scores, 
            spot_name, 
            strict_weekend, 
            max_window_days
        )
        all_windows.extend(spot_windows)
    
    # Convert to DataFrame
    if all_windows:
        windows_df = pd.DataFrame(all_windows)
        
        # Sort by average score descending
        windows_df = windows_df.sort_values('avg_score', ascending=False)
        
        return windows_df
    else:
        return pd.DataFrame(columns=[
            'spot', 'start_date', 'end_date', 'avg_score', 'type', 'days'
        ])


def load_half_day_scores(file_path='../data/surf_data.xlsx'):
    """
    Load half-day scores for all spots from an Excel file.
    
    :param file_path: Path to the Excel file
    :return: Dictionary with spot names as keys and half-day scores as values
    """
    # Get the sheet names
    xls = pd.ExcelFile(file_path)
    half_day_sheets = [s for s in xls.sheet_names if '_Half_Day' in s]
    
    spot_data_dict = {}
    
    for sheet_name in half_day_sheets:
        # Extract spot name
        spot_name = sheet_name.replace('_Half_Day', '')
        
        # Load data
        half_day_scores = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Store in dictionary
        spot_data_dict[spot_name] = half_day_scores
    
    return spot_data_dict


# Example usage
if __name__ == "__main__":
    # Load data for all spots
    spot_data_dict = load_half_day_scores()
    
    # Select optimal trips
    trips_df = select_optimal_trips(spot_data_dict)
    
    # Print results
    print("\nOptimal Surf Windows:")
    for i, row in trips_df.iterrows():
        print(f"{i+1}. {row['spot']} - {row['start_date'].strftime('%Y-%m-%d')} to "
              f"{row['end_date'].strftime('%Y-%m-%d')} ({row['days']} days)")
        print(f"   Type: {row['type']}, Average Score: {row['avg_score']}") 