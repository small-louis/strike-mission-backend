"""
Enhanced Surf Analysis Pipeline using Cached Processed Data

This optimized pipeline leverages the caching system to avoid expensive computation:
1. Uses pre-calculated half-day scores from cache (global computation done once)
2. Performs only user-specific window selection based on flight preferences
3. Exports results to Excel

Architecture:
- Global cache: Raw data → Scored forecasts → Half-day scores (shared)
- User analysis: Cached half-day scores → Window selection → Excel export
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.surf_spots import surf_spots
from utils.data_cache import get_cached_half_day_scores, get_cached_scored_forecast, is_data_fresh
from window_selection.optimal_windows import select_optimal_windows
from utils.excel_export import export_surf_results_to_excel


def analyze_surf_conditions_from_cache(output_file=None, min_days=5, max_days=12):
    """
    Analyze surf conditions using cached processed data.
    
    :param output_file: Path to Excel output file
    :param min_days: Minimum trip duration
    :param max_days: Maximum trip duration
    :return: Dictionary with results for each spot
    """
    print("🏄 Surf Analysis Pipeline (Enhanced Caching)")
    print("=" * 50)
    
    # Check cache freshness for all spots
    print("\n📊 Checking cache status...")
    cache_status = {}
    for spot_name in surf_spots.keys():
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        cache_status[spot_name] = is_data_fresh(spot_config, data_type='half_day')
    
    fresh_spots = sum(cache_status.values())
    total_spots = len(cache_status)
    
    print(f"Cache Status: {fresh_spots}/{total_spots} spots have fresh half-day scores")
    
    if fresh_spots == 0:
        print("⚠️  No fresh data available. Please run the data fetcher service first:")
        print("   python src/services/data_fetcher.py --force")
        return {}
    
    if fresh_spots < total_spots:
        stale_spots = [name for name, fresh in cache_status.items() if not fresh]
        print(f"⚠️  Stale data for: {', '.join(stale_spots)}")
        print("   Consider running: python src/services/data_fetcher.py")
    
    # Load half-day scores from cache
    print("\n🗄️  Loading processed data from cache...")
    spot_results = {}
    
    for spot_name in surf_spots.keys():
        if not cache_status[spot_name]:
            print(f"⏭️  Skipping {spot_name} (stale data)")
            continue
            
        spot_config = {
            'spot_id': spot_name.lower().replace(' ', '_'),
            **surf_spots[spot_name]
        }
        
        print(f"📈 Loading {spot_name}...")
        
        # Get half-day scores from cache
        half_day_scores = get_cached_half_day_scores(spot_config)
        
        if half_day_scores.empty:
            print(f"❌ No half-day scores available for {spot_name}")
            continue
        
        # Get hourly scored data from cache
        hourly_scored_data = get_cached_scored_forecast(spot_config)
        
        # Display data range
        date_range = half_day_scores['date'].agg(['min', 'max'])
        print(f"   📅 Data range: {date_range['min'].strftime('%Y-%m-%d')} to {date_range['max'].strftime('%Y-%m-%d')}")
        print(f"   📊 {len(half_day_scores)} half-day periods, {len(hourly_scored_data)} hourly forecasts")
        
        # Select optimal windows (user-specific computation)
        print(f"🎯 Selecting optimal windows for {spot_name}...")
        windows = select_optimal_windows(
            half_day_scores, 
            min_days=min_days, 
            max_days=max_days
        )
        
        spot_results[spot_name] = {
            'half_day_scores': half_day_scores,
            'hourly_scored_data': hourly_scored_data,
            'optimal_windows': windows,
            'data_freshness': 'Fresh',
            'cache_hit': True
        }
        
        if not windows.empty:
            best_window = windows.iloc[0]
            print(f"   🏆 Best window: {best_window['start_date'].strftime('%Y-%m-%d')} to {best_window['end_date'].strftime('%Y-%m-%d')} "
                  f"(Score: {best_window['avg_score']:.1f})")
        else:
            print(f"   ❌ No suitable windows found")
    
    # Export to Excel
    if output_file and spot_results:
        print(f"\n📊 Exporting results to {output_file}...")
        export_surf_results_to_excel(spot_results, output_file)
        print(f"✅ Results exported successfully!")
    
    # Summary
    print(f"\n📋 Analysis Summary:")
    print(f"   • Analyzed: {len(spot_results)} spots")
    print(f"   • Cache hits: {len(spot_results)} (100% cached data)")
    print(f"   • Heavy computation avoided: ✅")
    print(f"   • User-specific window selection: ✅")
    
    return spot_results


def quick_spot_check(spot_name, days=7):
    """
    Quick check of a single spot's conditions.
    
    :param spot_name: Name of the surf spot
    :param days: Number of days to show
    :return: DataFrame with recent conditions
    """
    if spot_name not in surf_spots:
        print(f"❌ Spot '{spot_name}' not found. Available: {list(surf_spots.keys())}")
        return pd.DataFrame()
    
    spot_config = {
        'spot_id': spot_name.lower().replace(' ', '_'),
        **surf_spots[spot_name]
    }
    
    # Check cache freshness
    is_fresh = is_data_fresh(spot_config)
    if not is_fresh:
        print(f"⚠️  Data for {spot_name} is stale. Run data fetcher for fresh data.")
    
    # Get half-day scores
    half_day_scores = get_cached_half_day_scores(spot_config)
    
    if half_day_scores.empty:
        print(f"❌ No data available for {spot_name}")
        return pd.DataFrame()
    
    # Show recent conditions
    cutoff_date = datetime.now().date()
    recent_scores = half_day_scores[
        half_day_scores['date'].dt.date >= cutoff_date
    ].head(days * 2)  # 2 half-days per day
    
    print(f"\n🏄 {spot_name} - Next {days} days:")
    print("-" * 40)
    
    for _, row in recent_scores.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        score = row['avg_total_points']
        half_day = row['half_day']
        quality = "🟢 Excellent" if score >= 7 else "🟡 Good" if score >= 5 else "🔴 Poor"
        print(f"{date_str} {half_day:>2}: {score:.1f} {quality}")
    
    return recent_scores


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Surf Analysis using Cached Data')
    parser.add_argument('--output', type=str, 
                       default='surf_data.xlsx',
                       help='Excel output file path')
    parser.add_argument('--min-days', type=int, default=5,
                       help='Minimum trip duration in days')
    parser.add_argument('--max-days', type=int, default=12,
                       help='Maximum trip duration in days')
    parser.add_argument('--spot', type=str,
                       help='Quick check for specific spot')
    parser.add_argument('--quick-days', type=int, default=7,
                       help='Days to show in quick spot check')
    
    args = parser.parse_args()
    
    if args.spot:
        # Quick spot check
        quick_spot_check(args.spot, args.quick_days)
    else:
        # Full analysis
        results = analyze_surf_conditions_from_cache(
            output_file=args.output,
            min_days=args.min_days,
            max_days=args.max_days
        )
        
        if results:
            print(f"\n🎉 Analysis complete! Check {args.output} for detailed results.")
        else:
            print("\n❌ Analysis failed. Check cache status and data availability.") 