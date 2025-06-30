"""
Optimal Window Selection for Surf Trips

This module takes pre-calculated half-day scores and selects optimal
surf trip windows based on user constraints like trip duration.
This is the user-specific computation that varies per user.
"""

import pandas as pd
from datetime import datetime, timedelta


def select_optimal_windows(half_day_scores, min_days=5, max_days=12, min_score=4.0):
    """
    Select optimal surf trip windows from half-day scores.
    
    :param half_day_scores: DataFrame with columns ['date', 'half_day', 'avg_total_points']
    :param min_days: Minimum trip duration in days
    :param max_days: Maximum trip duration in days
    :param min_score: Minimum average score threshold
    :return: DataFrame with optimal windows sorted by score
    """
    if half_day_scores.empty:
        return pd.DataFrame(columns=['start_date', 'end_date', 'days', 'avg_score', 'total_score'])
    
    # Ensure data is sorted by date and half_day
    half_day_scores = half_day_scores.sort_values(['date', 'half_day']).reset_index(drop=True)
    
    # Convert to daily scores (average of AM/PM for each day)
    daily_scores = half_day_scores.groupby('date')['avg_total_points'].mean().reset_index()
    daily_scores.columns = ['date', 'daily_score']
    
    windows = []
    
    # Generate all possible windows within duration constraints
    for duration in range(min_days, max_days + 1):
        for start_idx in range(len(daily_scores) - duration + 1):
            start_date = daily_scores.iloc[start_idx]['date']
            end_date = daily_scores.iloc[start_idx + duration - 1]['date']
            
            # Get scores for this window
            window_scores = daily_scores.iloc[start_idx:start_idx + duration]['daily_score']
            avg_score = window_scores.mean()
            total_score = window_scores.sum()
            
            # Only include windows above minimum score threshold
            if avg_score >= min_score:
                windows.append({
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': duration,
                    'avg_score': avg_score,
                    'total_score': total_score,
                    'consistency': window_scores.std()  # Lower std = more consistent
                })
    
    if not windows:
        return pd.DataFrame(columns=['start_date', 'end_date', 'days', 'avg_score', 'total_score'])
    
    windows_df = pd.DataFrame(windows)
    
    # Sort by average score (descending), then by consistency (ascending)
    windows_df = windows_df.sort_values(['avg_score', 'consistency'], ascending=[False, True])
    
    # Remove overlapping windows (keep the best one)
    windows_df = _remove_overlapping_windows(windows_df)
    
    return windows_df.head(10)  # Return top 10 windows


def _remove_overlapping_windows(windows_df, max_overlap_days=2):
    """
    Remove overlapping windows, keeping the best scoring ones.
    
    :param windows_df: DataFrame with window information
    :param max_overlap_days: Maximum allowed overlap in days
    :return: DataFrame with non-overlapping windows
    """
    if windows_df.empty:
        return windows_df
    
    # Sort by score to prioritize best windows
    windows_df = windows_df.sort_values('avg_score', ascending=False).reset_index(drop=True)
    
    selected_windows = []
    
    for _, window in windows_df.iterrows():
        # Check if this window significantly overlaps with any selected window
        overlaps = False
        
        for selected in selected_windows:
            overlap_days = _calculate_overlap_days(
                window['start_date'], window['end_date'],
                selected['start_date'], selected['end_date']
            )
            
            if overlap_days > max_overlap_days:
                overlaps = True
                break
        
        if not overlaps:
            selected_windows.append(window)
    
    return pd.DataFrame(selected_windows)


def _calculate_overlap_days(start1, end1, start2, end2):
    """
    Calculate the number of overlapping days between two date ranges.
    
    :param start1, end1: First date range
    :param start2, end2: Second date range
    :return: Number of overlapping days
    """
    # Convert to datetime if needed
    if hasattr(start1, 'date'):
        start1 = start1.date()
    if hasattr(end1, 'date'):
        end1 = end1.date()
    if hasattr(start2, 'date'):
        start2 = start2.date()
    if hasattr(end2, 'date'):
        end2 = end2.date()
    
    # Calculate overlap
    overlap_start = max(start1, start2)
    overlap_end = min(end1, end2)
    
    if overlap_start <= overlap_end:
        return (overlap_end - overlap_start).days + 1
    else:
        return 0


def select_weekend_windows(half_day_scores, min_days=2, max_days=4):
    """
    Select optimal weekend surf trips (Friday-Sunday focus).
    
    :param half_day_scores: DataFrame with half-day scores
    :param min_days: Minimum weekend duration
    :param max_days: Maximum weekend duration
    :return: DataFrame with weekend windows
    """
    if half_day_scores.empty:
        return pd.DataFrame()
    
    # Add weekday information
    half_day_scores = half_day_scores.copy()
    half_day_scores['weekday'] = half_day_scores['date'].dt.day_name()
    
    # Filter for Friday-Sunday periods
    weekend_days = ['Friday', 'Saturday', 'Sunday']
    weekend_scores = half_day_scores[half_day_scores['weekday'].isin(weekend_days)]
    
    return select_optimal_windows(weekend_scores, min_days, max_days, min_score=3.0)


def get_window_details(half_day_scores, start_date, end_date):
    """
    Get detailed information for a specific window.
    
    :param half_day_scores: DataFrame with half-day scores
    :param start_date: Window start date
    :param end_date: Window end date
    :return: Dictionary with detailed window information
    """
    # Convert to pandas Timestamp for comparison
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Filter data for the window
    window_data = half_day_scores[
        (half_day_scores['date'] >= start_date) & 
        (half_day_scores['date'] <= end_date)
    ].copy()
    
    if window_data.empty:
        return {}
    
    # Calculate statistics
    avg_score = window_data['avg_total_points'].mean()
    min_score = window_data['avg_total_points'].min()
    max_score = window_data['avg_total_points'].max()
    total_periods = len(window_data)
    good_periods = len(window_data[window_data['avg_total_points'] >= 5.0])
    
    # Daily breakdown
    daily_breakdown = window_data.groupby('date').agg({
        'avg_total_points': ['mean', 'count']
    }).round(1)
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'duration_days': (end_date - start_date).days + 1,
        'avg_score': avg_score,
        'min_score': min_score,
        'max_score': max_score,
        'total_periods': total_periods,
        'good_periods': good_periods,
        'good_percentage': (good_periods / total_periods * 100) if total_periods > 0 else 0,
        'daily_breakdown': daily_breakdown,
        'window_data': window_data
    } 