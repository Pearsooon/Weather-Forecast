import json
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
import os

def load_snowflake_config(config_path='config/snowflake_config.json'):
    """Load Snowflake configuration from JSON file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"❌ Config file not found: {config_path}\n"
            f"   Please create config/snowflake_config.json with your Snowflake credentials"
        )
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_fields = ['user', 'password', 'account', 'warehouse', 'database', 'schema']
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise ValueError(
            f"❌ Missing required fields in config: {', '.join(missing_fields)}"
        )
    
    return config

def get_snowflake_connection(config_path='config/snowflake_config.json'):
    """Create Snowflake connection with error handling"""
    try:
        config = load_snowflake_config(config_path)
        
        conn = connect(
            user=config['user'],
            password=config['password'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            session_parameters={
                'QUERY_TAG': 'weather_forecasting_project',
            }
        )
        
        return conn
        
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {str(e)}")
        raise

def get_snowflake_engine(config_path='config/snowflake_config.json'):
    """Create SQLAlchemy engine for Snowflake with error handling"""
    try:
        config = load_snowflake_config(config_path)
        
        engine = create_engine(URL(
            account=config['account'],
            user=config['user'],
            password=config['password'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema']
        ))
        
        return engine
        
    except Exception as e:
        print(f"❌ Failed to create Snowflake engine: {str(e)}")
        raise

def execute_query(query, config_path='config/snowflake_config.json', verbose=True):
    """
    Execute SQL query and return results as DataFrame
    
    Args:
        query: SQL query string
        config_path: Path to Snowflake config file
        verbose: Print query info
    
    Returns:
        DataFrame with query results
    """
    try:
        if verbose:
            print(f"🔍 Executing query...")
            
        engine = get_snowflake_engine(config_path)
        df = pd.read_sql(query, engine)
        engine.dispose()
        
        if verbose:
            print(f"✅ Query returned {len(df):,} rows")
        
        return df
        
    except Exception as e:
        print(f"❌ Query execution failed: {str(e)}")
        raise

def upload_dataframe(df, table_name, if_exists='append', config_path='config/snowflake_config.json', verbose=True):
    """
    Upload DataFrame to Snowflake table
    
    Args:
        df: pandas DataFrame
        table_name: Target table name (will be uppercased)
        if_exists: 'append', 'replace', or 'fail'
        config_path: Path to Snowflake config file
        verbose: Print upload info
    """
    try:
        if verbose:
            print(f"📤 Uploading {len(df):,} records to {table_name}...")
        
        # Ensure table name is uppercase
        table_name = table_name.upper()
        
        # Get engine
        engine = get_snowflake_engine(config_path)
        
        # Upload data
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=5000
        )
        
        engine.dispose()
        
        if verbose:
            print(f"✅ Successfully uploaded {len(df):,} records to {table_name}")
        
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        raise

def test_connection(config_path='config/snowflake_config.json'):
    """Test Snowflake connection"""
    print("\n" + "=" * 80)
    print("🧪 TESTING SNOWFLAKE CONNECTION")
    print("=" * 80 + "\n")
    
    try:
        conn = get_snowflake_connection(config_path)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print("✅ Connection successful!")
        print(f"\n📌 Connection Details:")
        print(f"   - Snowflake Version: {result[0]}")
        print(f"   - User:              {result[1]}")
        print(f"   - Role:              {result[2]}")
        print(f"   - Database:          {result[3]}")
        print(f"   - Warehouse:         {result[4]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 80 + "\n")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("\n" + "=" * 80 + "\n")
        return False

def create_sample_config():
    """Create a sample config file"""
    sample_config = {
        "user": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "account": "YOUR_ACCOUNT",
        "warehouse": "WEATHER_WH",
        "database": "WEATHER_DB",
        "schema": "RAW"
    }
    
    config_dir = 'config'
    os.makedirs(config_dir, exist_ok=True)
    
    config_path = os.path.join(config_dir, 'snowflake_config_sample.json')
    
    with open(config_path, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"✅ Sample config created: {config_path}")
    print(f"   Please copy to 'snowflake_config.json' and update with your credentials")

# Main function for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Snowflake utility functions')
    parser.add_argument('--test', action='store_true', help='Test connection')
    parser.add_argument('--create-config', action='store_true', help='Create sample config')
    
    args = parser.parse_args()
    
    if args.create_config:
        create_sample_config()
    elif args.test:
        test_connection()
    else:
        print("Usage:")
        print("  python utils.py --test            # Test connection")
        print("  python utils.py --create-config   # Create sample config")