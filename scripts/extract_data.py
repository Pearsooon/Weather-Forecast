import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import load_snowflake_config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class WeatherDataExtractor:
    def __init__(self):
        # Setup cache & retry
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)
        
    def extract_historical_data(self, latitude, longitude, start_date, end_date):
        """
        Extract historical weather data from Open-Meteo API
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with hourly weather data
        """
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": [
                "temperature_2m", 
                "relative_humidity_2m", 
                "precipitation",
                "surface_pressure",
                "wind_speed_10m",
                "wind_direction_10m",
                "cloud_cover"
            ],
            "timezone": "Asia/Ho_Chi_Minh"
        }
        
        try:
            responses = self.client.weather_api(url, params=params)
            response = responses[0]
            
            # Process hourly data
            hourly = response.Hourly()
            hourly_data = {
                "datetime": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                "temperature": hourly.Variables(0).ValuesAsNumpy(),
                "humidity": hourly.Variables(1).ValuesAsNumpy(),
                "precipitation": hourly.Variables(2).ValuesAsNumpy(),
                "pressure": hourly.Variables(3).ValuesAsNumpy(),
                "wind_speed": hourly.Variables(4).ValuesAsNumpy(),
                "wind_direction": hourly.Variables(5).ValuesAsNumpy(),
                "cloud_cover": hourly.Variables(6).ValuesAsNumpy(),
            }
            
            df_hourly = pd.DataFrame(hourly_data)
            df_hourly['latitude'] = latitude
            df_hourly['longitude'] = longitude
            
            return df_hourly
            
        except Exception as e:
            print(f"❌ Error extracting data: {str(e)}")
            return None
    
    def extract_multiple_locations(self, locations, start_date, end_date):
        """
        Extract data for multiple locations
        
        Args:
            locations: list of dict [{"name": "Hanoi", "lat": 21.0285, "lon": 105.8542}, ...]
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            Combined DataFrame for all locations
        """
        all_data = []
        total_locations = len(locations)
        
        print(f"\n📍 Extracting data for {total_locations} locations...")
        print("=" * 80)
        
        for idx, loc in enumerate(locations, 1):
            print(f"\n[{idx}/{total_locations}] 🌍 {loc['name']} (Lat: {loc['lat']}, Lon: {loc['lon']})")
            
            df = self.extract_historical_data(
                loc['lat'], 
                loc['lon'], 
                start_date, 
                end_date
            )
            
            if df is not None:
                df['location_name'] = loc['name']
                all_data.append(df)
                print(f"         ✅ Extracted {len(df):,} records")
                print(f"         📅 Date range: {df['datetime'].min()} to {df['datetime'].max()}")
            else:
                print(f"         ❌ Failed to extract data")
        
        print("\n" + "=" * 80)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"\n✅ Total extracted: {len(combined_df):,} records from {len(all_data)} locations")
            return combined_df
        else:
            print("\n❌ No data extracted")
            return None

# Usage
if __name__ == "__main__":
    print("\n" + "🌤️ " * 20)
    print("         WEATHER FORECASTING PROJECT - DATA EXTRACTOR")
    print("🌤️ " * 20 + "\n")
    
    extractor = WeatherDataExtractor()
    
    # Define locations - Vietnam major cities
    locations = [
        {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542},
        {"name": "Ho Chi Minh City", "lat": 10.8231, "lon": 106.6297},
        {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022},
        {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469},
        {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881},
    ]
    
    # Extract 2 years of historical data
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("📋 EXTRACTION CONFIGURATION")
    print("=" * 80)
    print(f"   📅 Start Date:   {start_date}")
    print(f"   📅 End Date:     {end_date}")
    print(f"   📍 Locations:    {len(locations)}")
    print(f"   🔢 Expected:     ~{len(locations) * 730 * 24:,} records (2 years × 24 hours)")
    print("=" * 80)
    
    # Ask for confirmation
    proceed = input("\n▶️  Proceed with extraction? (yes/no): ").strip().lower()
    
    if proceed != 'yes':
        print("❌ Extraction cancelled")
        sys.exit(0)
    
    print(f"\n🚀 Starting data extraction...\n")
    
    df = extractor.extract_multiple_locations(locations, start_date, end_date)
    
    if df is not None:
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
        print("✅ EXTRACTION COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"\n📊 Summary:")
        print(f"   📁 File:           {output_path}")
        print(f"   📏 Size:           {file_size_mb:.2f} MB")
        print(f"   📝 Records:        {len(df):,}")
        print(f"   📍 Locations:      {locations_count}")
        print(f"   📅 Date Range:     {date_range} days ({df['datetime'].min()} to {df['datetime'].max()})")
        print(f"   📊 Columns:        {len(df.columns)}")
        
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
        print("   3. Run dbt transformations:")
        print("      - cd dbt_project")
        print("      - dbt run")
        print()
        print("=" * 80 + "\n")
        
    else:
        print("\n" + "=" * 80)
        print("❌ EXTRACTION FAILED")
        print("=" * 80)
        print("\nPossible reasons:")
        print("   - Network connectivity issues")
        print("   - API rate limiting")
        print("   - Invalid location coordinates")
        print("\nPlease check the error messages above and try again.")
        print("=" * 80 + "\n")