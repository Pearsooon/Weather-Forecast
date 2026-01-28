"""
Core Weather Data Extractor
Shared logic for both backfill and daily extraction
"""
import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime, timedelta


class WeatherDataExtractor:
    """Core extractor class with shared API logic"""
    
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
    
    def extract_multiple_locations(self, locations, start_date, end_date, verbose=True):
        """
        Extract data for multiple locations
        
        Args:
            locations: list of dict [{"name": "Hanoi", "lat": 21.0285, "lon": 105.8542}, ...]
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            verbose: Print progress
        
        Returns:
            Combined DataFrame for all locations
        """
        all_data = []
        total_locations = len(locations)
        
        if verbose:
            print(f"\n🔍 Extracting data for {total_locations} locations...")
            print("=" * 80)
        
        for idx, loc in enumerate(locations, 1):
            if verbose:
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
                if verbose:
                    print(f"         ✅ Extracted {len(df):,} records")
                    print(f"         📅 Date range: {df['datetime'].min()} to {df['datetime'].max()}")
            else:
                if verbose:
                    print(f"         ❌ Failed to extract data")
        
        if verbose:
            print("\n" + "=" * 80)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            if verbose:
                print(f"\n✅ Total extracted: {len(combined_df):,} records from {len(all_data)} locations")
            return combined_df
        else:
            if verbose:
                print("\n❌ No data extracted")
            return None
    
    def prepare_for_snowflake(self, df):
        """
        Prepare DataFrame for Snowflake loading
        - Add metadata columns
        - Create unique record_id
        - Reorder columns
        
        Args:
            df: Raw DataFrame from API
        
        Returns:
            DataFrame ready for Snowflake
        """
        # Add metadata
        df['extract_date'] = pd.to_datetime('today').date()
        df['record_id'] = df['location_name'] + '_' + df['datetime'].astype(str)
        
        # Reorder columns to match table structure
        column_order = [
            'record_id', 'datetime', 'location_name', 'latitude', 'longitude',
            'temperature', 'humidity', 'precipitation', 'pressure',
            'wind_speed', 'wind_direction', 'cloud_cover', 'extract_date'
        ]
        
        return df[column_order]


def get_vietnam_locations():
    """Get standard list of Vietnam locations"""
    return [
        {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542},
        {"name": "Ho Chi Minh City", "lat": 10.8231, "lon": 106.6297},
        {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022},
        {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469},
        {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881},
    ]