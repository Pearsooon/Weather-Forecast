"""
Weather Data Backfill Script
Manual extraction of 2 years historical data
Run once for initial data load
"""
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.extractor import WeatherDataExtractor, get_vietnam_locations


def main():
    """Main backfill function"""
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    print("\n" + "🌤️ " * 20)
    print("         WEATHER FORECASTING - BACKFILL (2 YEARS)")
    print("🌤️ " * 20 + "\n")
    
    extractor = WeatherDataExtractor()
    locations = get_vietnam_locations()
    
    # Calculate date range: 2 years of historical data
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("📋 BACKFILL CONFIGURATION")
    print("=" * 80)
    print(f"   📅 Start Date:   {start_date}")
    print(f"   📅 End Date:     {end_date}")
    print(f"   📍 Locations:    {len(locations)}")
    print(f"   📊 Expected:     ~{len(locations) * 730 * 24:,} records (2 years × 24 hours)")
    print("=" * 80)
    
    # Ask for confirmation
    proceed = input("\n▶️  Proceed with backfill extraction? (yes/no): ").strip().lower()
    
    if proceed != 'yes':
        print("❌ Backfill cancelled")
        sys.exit(0)
    
    print(f"\n🚀 Starting backfill extraction...\n")
    
    # Extract data
    df = extractor.extract_multiple_locations(locations, start_date, end_date, verbose=True)
    
    if df is not None:
        # Prepare for Snowflake
        df = extractor.prepare_for_snowflake(df)
        
        # Save to CSV
        raw_dir = os.path.join(BASE_DIR, 'data', 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        output_path = os.path.join(raw_dir, 'weather_raw_data.csv')
        
        print(f"\n💾 Saving data to CSV...")
        df.to_csv(output_path, index=False)
        
        # Calculate statistics
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        locations_count = df['location_name'].nunique()
        date_range = (df['datetime'].max() - df['datetime'].min()).days
        
        print("\n" + "=" * 80)
        print("✅ BACKFILL COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"\n📊 Summary:")
        print(f"   📁 File:           {output_path}")
        print(f"   📦 Size:           {file_size_mb:.2f} MB")
        print(f"   📊 Records:        {len(df):,}")
        print(f"   📍 Locations:      {locations_count}")
        print(f"   📅 Date Range:     {date_range} days ({df['datetime'].min()} to {df['datetime'].max()})")
        print(f"   📋 Columns:        {len(df.columns)}")
        
        print(f"\n📋 Columns: {', '.join(df.columns)}")
        
        print(f"\n📍 Records per location:")
        location_counts = df['location_name'].value_counts().sort_index()
        for location, count in location_counts.items():
            print(f"   - {location:20s}: {count:6,} records")
        
        # Data quality check
        print(f"\n🔍 Data Quality:")
        missing_pct = (df.isnull().sum() / len(df) * 100)
        if missing_pct.max() > 0:
            print(f"   ⚠️  Missing values detected:")
            for col in missing_pct[missing_pct > 0].index:
                print(f"      - {col}: {missing_pct[col]:.2f}%")
        else:
            print(f"   ✅ No missing values")
        
        print("\n" + "=" * 80)
        print("🎯 NEXT STEPS:")
        print("=" * 80)
        print("   1. Review the extracted data:")
        print("      - Open data/raw/weather_raw_data.csv")
        print("      - Check for any anomalies or missing data")
        print()
        print("   2. Load data to Snowflake:")
        print("      - Run: python scripts/load_to_snowflake.py")
        print("      - Choose 'yes' for database setup (first time)")
        print("      - Choose 'yes' for data loading")
        print()
        print("   3. Setup Airflow for daily incremental loads:")
        print("      - Verify docker-compose.yml configuration")
        print("      - Run: docker-compose up -d")
        print()
        print("=" * 80 + "\n")
        
    else:
        print("\n" + "=" * 80)
        print("❌ BACKFILL FAILED")
        print("=" * 80)
        print("\nPossible reasons:")
        print("   - Network connectivity issues")
        print("   - API rate limiting")
        print("   - Invalid location coordinates")
        print("\nPlease check the error messages above and try again.")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()