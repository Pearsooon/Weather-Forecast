"""
Weather Data Daily Extraction Script
Incremental daily load - extracts yesterday's data
Designed for Airflow automation
"""
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.extractor import WeatherDataExtractor, get_vietnam_locations


def main():
    """Main daily extraction function"""
    AIRFLOW_BASE_DIR = "/opt/airflow"
    
    print("\n" + "🌤️ " * 20)
    print("         WEATHER FORECASTING - DAILY EXTRACTION")
    print("🌤️ " * 20 + "\n")
    
    extractor = WeatherDataExtractor()
    locations = get_vietnam_locations()
    
    # Calculate yesterday's date (completed day)
    yesterday = datetime.now() - timedelta(days=1)
    extract_date = yesterday.strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("📋 DAILY EXTRACTION CONFIGURATION")
    print("=" * 80)
    print(f"   📅 Extract Date:  {extract_date} (Yesterday)")
    print(f"   📍 Locations:     {len(locations)}")
    print(f"   📊 Expected:      ~{len(locations) * 24:,} records (24 hours × {len(locations)} locations)")
    print(f"   🕒 Run Time:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    print(f"\n🚀 Starting daily extraction...\n")
    
    # Extract yesterday's data (start_date = end_date for single day)
    df = extractor.extract_multiple_locations(
        locations, 
        start_date=extract_date, 
        end_date=extract_date,
        verbose=True
    )
    
    if df is not None and len(df) > 0:
        # Prepare for Snowflake
        df = extractor.prepare_for_snowflake(df)
        
        # Save to CSV (daily file)
        raw_dir = os.path.join(AIRFLOW_BASE_DIR, 'data', 'raw')
        os.makedirs(raw_dir, exist_ok=True)
        output_path = os.path.join(raw_dir, f'weather_daily_{extract_date}.csv')
        
        print(f"\n💾 Saving data to: {output_path}")
        df.to_csv(output_path, index=False)
        
        # Calculate statistics
        file_size_kb = os.path.getsize(output_path) / 1024
        locations_count = df['location_name'].nunique()
        
        print("\n" + "=" * 80)
        print("✅ DAILY EXTRACTION COMPLETED!")
        print("=" * 80)
        print(f"\n📊 Summary:")
        print(f"   📁 File:           {output_path}")
        print(f"   📦 Size:           {file_size_kb:.2f} KB")
        print(f"   📊 Records:        {len(df):,}")
        print(f"   📍 Locations:      {locations_count}")
        print(f"   📅 Date:           {extract_date}")
        print(f"   ⏰ Time Range:     {df['datetime'].min()} to {df['datetime'].max()}")
        
        print(f"\n📍 Records per location:")
        location_counts = df['location_name'].value_counts().sort_index()
        for location, count in location_counts.items():
            print(f"   - {location:20s}: {count:3,} records")
        
        # Data quality check
        print(f"\n🔍 Data Quality Check:")
        missing_pct = (df.isnull().sum() / len(df) * 100)
        if missing_pct.max() > 0:
            print(f"   ⚠️  Missing values detected:")
            for col in missing_pct[missing_pct > 0].index:
                print(f"      - {col}: {missing_pct[col]:.2f}%")
        else:
            print(f"   ✅ No missing values")
        
        # Check for expected record count
        expected_records = len(locations) * 24
        if len(df) < expected_records:
            print(f"\n   ⚠️  Warning: Expected {expected_records} records, got {len(df)}")
        else:
            print(f"\n   ✅ Record count matches expectation ({expected_records})")
        
        print("\n" + "=" * 80)
        print("🎯 File ready for Snowflake loading")
        print("=" * 80 + "\n")
        
        return output_path  # Return path for Airflow
        
    else:
        print("\n" + "=" * 80)
        print("❌ DAILY EXTRACTION FAILED - NO DATA")
        print("=" * 80)
        print("\nPossible reasons:")
        print("   - API did not return data for yesterday")
        print("   - Network connectivity issues")
        print("   - API rate limiting")
        print("\nPlease check the error messages above.")
        print("=" * 80 + "\n")
        raise Exception(f"No data extracted for {extract_date}")


if __name__ == "__main__":
    main()