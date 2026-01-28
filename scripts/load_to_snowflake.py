"""
Snowflake Data Loader
Supports both backfill (full load) and daily incremental loads
Handles duplicate prevention via record_id
"""
import pandas as pd
import sys
import os
import re
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import get_snowflake_connection, upload_dataframe

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class SnowflakeLoader:
    def __init__(self, config_path=None):
        self.config_path = config_path or os.path.join(
            BASE_DIR, 'config', 'snowflake_config.json'
        )
    
    def clean_sql_content(self, sql_content):
        """Clean SQL content by removing comments and empty lines"""
        sql_content = re.sub(r'--[^\n]*', '', sql_content)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        lines = [line.strip() for line in sql_content.split('\n') if line.strip()]
        sql_content = ' '.join(lines)
        return sql_content
    
    def split_sql_statements(self, sql_content):
        """Split SQL content into individual statements"""
        sql_content = self.clean_sql_content(sql_content)
        statements = sql_content.split(';')
        statements = [s.strip() for s in statements if s.strip()]
        return statements
        
    def execute_sql_file(self, sql_file_path):
        """Execute SQL file with improved parsing"""
        print(f"\n📄 Executing SQL file: {os.path.basename(sql_file_path)}")
        
        with open(sql_file_path, encoding='utf-8', errors='ignore') as f:
            sql_content = f.read()
        
        statements = self.split_sql_statements(sql_content)
        
        conn = get_snowflake_connection(self.config_path)
        cursor = conn.cursor()
        
        success_count = 0
        error_count = 0
        
        try:
            for idx, statement in enumerate(statements, 1):
                if not statement:
                    continue
                    
                try:
                    cursor.execute(statement)
                    success_count += 1
                    if any(keyword in statement.upper() for keyword in ['CREATE', 'ALTER', 'GRANT', 'USE']):
                        print(f"   ✅ Statement {idx}: {statement[:50]}...")
                except Exception as e:
                    error_count += 1
                    error_msg = str(e)
                    
                    if 'does not exist' in error_msg:
                        print(f"   ⚠️  Statement {idx}: Object does not exist (expected on first run)")
                    elif 'Unsupported feature' in error_msg:
                        print(f"   ⚠️  Statement {idx}: Feature not supported (non-critical)")
                    elif 'already exists' in error_msg:
                        print(f"   ℹ️  Statement {idx}: Object already exists (skipping)")
                    else:
                        print(f"   ❌ Statement {idx}: {error_msg[:100]}")
            
            conn.commit()
            print(f"\n✅ Completed: {success_count} successful, {error_count} warnings\n")
            
        except Exception as e:
            print(f"❌ Critical error: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def setup_database_structure(self):
        """Run all setup SQL scripts in order"""
        sql_dir = os.path.join(BASE_DIR, 'sql')
        
        setup_scripts = [
            '00_setup_environment.sql',
            '01_create_warehouse.sql',
            '02_create_database.sql',
            '03_create_tables.sql',
            '04_grant_permissions.sql'
        ]
        
        print("\n" + "=" * 80)
        print("🚀 WEATHER FORECASTING - DATABASE SETUP")
        print("=" * 80)
        
        for script in setup_scripts:
            script_path = os.path.join(sql_dir, script)
            if os.path.exists(script_path):
                self.execute_sql_file(script_path)
            else:
                print(f"❌ Script not found: {script_path}")
        
        print("=" * 80)
        print("✅ DATABASE STRUCTURE SETUP COMPLETED!")
        print("=" * 80 + "\n")
    
    def verify_setup(self):
        """Verify database setup is correct"""
        print("\n" + "=" * 80)
        print("🔍 VERIFYING DATABASE SETUP")
        print("=" * 80 + "\n")
        
        conn = get_snowflake_connection(self.config_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SHOW DATABASES LIKE 'WEATHER_DB'")
            if cursor.fetchone():
                print("✅ Database WEATHER_DB exists")
            else:
                print("❌ Database WEATHER_DB not found")
            
            cursor.execute("SHOW WAREHOUSES LIKE 'WEATHER_WH'")
            if cursor.fetchone():
                print("✅ Warehouse WEATHER_WH exists")
            else:
                print("❌ Warehouse WEATHER_WH not found")
            
            cursor.execute("SHOW SCHEMAS IN DATABASE WEATHER_DB")
            schemas = cursor.fetchall()
            schema_names = [s[1] for s in schemas]
            
            required_schemas = ['RAW', 'STAGING', 'INTERMEDIATE', 'MARTS', 'ANALYTICS']
            for schema in required_schemas:
                if schema in schema_names:
                    print(f"✅ Schema {schema} exists")
                else:
                    print(f"❌ Schema {schema} not found")
            
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM WEATHER_DB.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA IN ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS', 'ANALYTICS')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables = cursor.fetchall()
            
            if tables:
                print(f"\n✅ Found {len(tables)} tables:")
                for schema, table in tables:
                    print(f"   - {schema}.{table}")
            else:
                print("\n⚠️  No tables found")
            
        except Exception as e:
            print(f"❌ Verification error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
        
        print("\n" + "=" * 80 + "\n")
    
    def get_existing_record_ids(self, date_filter=None):
        """
        Get existing record_ids from Snowflake to prevent duplicates
        
        Args:
            date_filter: Optional date (YYYY-MM-DD) to filter records
        
        Returns:
            Set of existing record_ids
        """
        conn = get_snowflake_connection(self.config_path)
        cursor = conn.cursor()
        
        try:
            if date_filter:
                query = f"""
                    SELECT record_id 
                    FROM RAW.WEATHER_RAW 
                    WHERE DATE(datetime) = '{date_filter}'
                """
            else:
                query = "SELECT record_id FROM RAW.WEATHER_RAW"
            
            cursor.execute(query)
            existing_ids = {row[0] for row in cursor.fetchall()}
            
            return existing_ids
            
        except Exception as e:
            print(f"⚠️  Could not fetch existing records: {str(e)}")
            return set()
        finally:
            cursor.close()
            conn.close()
    
    def load_data_incremental(self, df, extract_date=None):
        """
        Load DataFrame to Snowflake with duplicate prevention
        
        Args:
            df: DataFrame to load
            extract_date: Optional date to check for duplicates (YYYY-MM-DD)
        """
        try:
            print(f"\n📥 Loading data to Snowflake (incremental mode)...")
            print(f"   Total records to load: {len(df):,}")
            
            # Get existing record_ids for the date
            if extract_date:
                print(f"   Checking for duplicates on date: {extract_date}")
                existing_ids = self.get_existing_record_ids(extract_date)
            else:
                print(f"   Checking for duplicates (full scan)...")
                existing_ids = self.get_existing_record_ids()
            
            if existing_ids:
                print(f"   Found {len(existing_ids):,} existing records")
                
                # Filter out duplicates
                df_filtered = df[~df['record_id'].isin(existing_ids)].copy()
                duplicates = len(df) - len(df_filtered)
                
                if duplicates > 0:
                    print(f"   ⚠️  Skipping {duplicates:,} duplicate records")
                
                if len(df_filtered) == 0:
                    print(f"   ℹ️  All records already exist in database - nothing to load")
                    return
                
                df = df_filtered
                print(f"   📊 New records to insert: {len(df):,}")
            else:
                print(f"   ℹ️  No existing records found (first load)")
            
            # Upload to Snowflake
            upload_dataframe(
                df, 
                'WEATHER_RAW', 
                if_exists='append',
                config_path=self.config_path,
                verbose=False
            )
            
            print(f"✅ Successfully loaded {len(df):,} new records to WEATHER_RAW")
            
        except Exception as e:
            print(f"❌ Error loading data: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def load_data_full(self, df):
        """
        Load DataFrame to Snowflake (full replace mode - for backfill)
        WARNING: This truncates existing data
        """
        try:
            print(f"\n📥 Loading data to Snowflake (full replace mode)...")
            print(f"   ⚠️  This will REPLACE all existing data")
            
            proceed = input("   Continue? (yes/no): ").strip().lower()
            if proceed != 'yes':
                print("   ❌ Load cancelled")
                return
            
            # Upload to Snowflake (replace mode)
            upload_dataframe(
                df, 
                'WEATHER_RAW', 
                if_exists='replace',
                config_path=self.config_path,
                verbose=False
            )
            
            print(f"✅ Successfully loaded {len(df):,} records to WEATHER_RAW")
            
        except Exception as e:
            print(f"❌ Error loading data: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def validate_load(self):
        """Validate data loaded successfully"""
        conn = get_snowflake_connection(self.config_path)
        cursor = conn.cursor()
        
        try:
            print("\n📊 Validating loaded data...")
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT location_name) as locations,
                    MIN(datetime) as min_date,
                    MAX(datetime) as max_date,
                    COUNT(DISTINCT DATE(datetime)) as unique_days
                FROM RAW.WEATHER_RAW
            """)
            
            result = cursor.fetchone()
            
            print("\n" + "=" * 80)
            print("✅ DATA VALIDATION RESULTS")
            print("=" * 80)
            print(f"   📊 Total records:     {result[0]:,}")
            print(f"   📍 Unique locations:  {result[1]}")
            print(f"   📅 Unique days:       {result[4]:,}")
            print(f"   📅 Date range:        {result[2]} to {result[3]}")
            print("=" * 80 + "\n")
            
            # Check by location
            cursor.execute("""
                SELECT 
                    location_name,
                    COUNT(*) as records,
                    MIN(DATE(datetime)) as first_date,
                    MAX(DATE(datetime)) as last_date
                FROM RAW.WEATHER_RAW
                GROUP BY location_name
                ORDER BY location_name
            """)
            
            print("📍 Records by location:")
            for row in cursor.fetchall():
                print(f"   - {row[0]:20s}: {row[1]:6,} records ({row[2]} to {row[3]})")
            
            print()
            
        except Exception as e:
            print(f"❌ Error validating data: {str(e)}")
        finally:
            cursor.close()
            conn.close()


def main():
    """Main loader function"""
    loader = SnowflakeLoader()
    
    print("\n" + "🌤️ " * 20)
    print("         WEATHER FORECASTING PROJECT - DATA LOADER")
    print("🌤️ " * 20 + "\n")
    
    # Option 1: Setup database structure (run once)
    print("=" * 80)
    print("OPTION 1: Setup Database Structure")
    print("=" * 80)
    print("This will create:")
    print("  - Roles and permissions")
    print("  - Warehouse (WEATHER_WH)")
    print("  - Database (WEATHER_DB)")
    print("  - Schemas (RAW, STAGING, INTERMEDIATE, MARTS, ANALYTICS)")
    print("  - Tables (WEATHER_RAW, etc.)")
    print()
    
    setup_choice = input("Do you want to run database setup? (yes/no): ").strip().lower()
    
    if setup_choice == 'yes':
        loader.setup_database_structure()
        loader.verify_setup()
    
    # Option 2: Load data
    print("\n" + "=" * 80)
    print("OPTION 2: Load Data")
    print("=" * 80)
    print("Choose load mode:")
    print("  1. Backfill (full replace) - loads weather_raw_data.csv")
    print("  2. Daily (incremental) - loads weather_daily_YYYY-MM-DD.csv")
    print()
    
    load_choice = input("Select mode (1/2) or 'no' to skip: ").strip()
    
    if load_choice in ['1', '2']:
        if load_choice == '1':
            # Backfill mode
            csv_path = os.path.join(BASE_DIR, 'data', 'raw', 'weather_raw_data.csv')
            load_mode = 'full'
        else:
            # Daily mode - find most recent daily file
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            csv_path = os.path.join(BASE_DIR, 'data', 'raw', f'weather_daily_{yesterday}.csv')
            load_mode = 'incremental'
        
        if os.path.exists(csv_path):
            print(f"\n📂 Reading data from: {csv_path}")
            df = pd.read_csv(csv_path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            print(f"✅ Loaded {len(df):,} records from CSV")
            print(f"   Columns: {list(df.columns)}")
            
            if load_mode == 'full':
                loader.load_data_full(df)
            else:
                # Extract date from datetime for duplicate checking
                extract_date = df['datetime'].dt.date.iloc[0].strftime("%Y-%m-%d")
                loader.load_data_incremental(df, extract_date)
            
            loader.validate_load()
        else:
            print(f"❌ File not found: {csv_path}")
            if load_mode == 'incremental':
                print("   Please run: python scripts/extract/extract_daily.py")
            else:
                print("   Please run: python scripts/extract/extract_backfill.py")
    
    print("\n" + "=" * 80)
    print("✅ PROCESS COMPLETED!")
    print("=" * 80)
    print("\nNext steps:")
    print("  1. Run dbt models: cd dbt_project && dbt run")
    print("  2. Setup Airflow: docker-compose up -d")
    print("  3. Open Jupyter notebooks for analysis")
    print("\n" + "🌤️ " * 20 + "\n")


if __name__ == "__main__":
    main()