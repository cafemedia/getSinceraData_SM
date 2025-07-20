#!/usr/bin/env python3
"""
Step 2: Collect Sincera Data
=============================
This script:
1. Loads the domain sample from pickle
2. Fetches comprehensive metrics from Sincera API for each domain
3. Handles rate limiting and errors properly
4. Saves results incrementally and final dataset to pickle
5. Optionally uploads to Snowflake

Usage:
    python 2_collect_sincera_data.py --test          # Test mode (10 domains per network)
    python 2_collect_sincera_data.py --full          # Full mode (all domains)
    python 2_collect_sincera_data.py --no-snowflake  # Skip Snowflake upload
    python 2_collect_sincera_data.py                 # Interactive mode selection
"""

import pandas as pd
import numpy as np
import requests
import time
import json
import os
import sys
from datetime import datetime, timedelta
from collections import deque
import warnings
warnings.filterwarnings('ignore')

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add current directory to Python path for imports
sys.path.append(os.getcwd())
from snowpark_connect import func_create_snowpark_session

# Sincera API Configuration
SINCERA_API_KEY = os.environ.get('SINCERA_API_KEY')
if not SINCERA_API_KEY:
    raise ValueError("SINCERA_API_KEY environment variable not set")

API_BASE_URL = 'https://open.sincera.io/api/publishers'
HEADERS = {'Authorization': f'Bearer {SINCERA_API_KEY}'}

# Rate limiting constants
REQUESTS_PER_MINUTE = 45
REQUESTS_PER_DAY = 5000
REQUEST_TIMEOUT = 30

# Rate limiting state
_request_times = deque()
_requests_today = 0
_current_day = datetime.now().date()

def respect_rate_limits():
    """Sleep as needed to honor API rate limits."""
    global _requests_today, _current_day
    
    now = time.time()
    today = datetime.now().date()
    
    # Reset daily counter if new day
    if today != _current_day:
        _current_day = today
        _requests_today = 0
        _request_times.clear()
    
    # Check daily limit
    if _requests_today >= REQUESTS_PER_DAY:
        print(f"Daily limit reached ({REQUESTS_PER_DAY}). Waiting until tomorrow...")
        tomorrow = datetime.combine(today + timedelta(days=1), datetime.min.time())
        wait_seconds = (tomorrow - datetime.now()).total_seconds()
        time.sleep(max(0, wait_seconds))
        _current_day = datetime.now().date()
        _requests_today = 0
        _request_times.clear()
        now = time.time()
    
    # Remove requests older than 1 minute
    while _request_times and now - _request_times[0] >= 60:
        _request_times.popleft()
    
    # Check minute limit
    if len(_request_times) >= REQUESTS_PER_MINUTE:
        wait_time = 60 - (now - _request_times[0])
        if wait_time > 0:
            print(f"⏳ Rate limit: waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
        now = time.time()
        while _request_times and now - _request_times[0] >= 60:
            _request_times.popleft()
    
    _request_times.append(time.time())
    _requests_today += 1

def fetch_domain_metrics(domain, max_retries=3):
    """
    Fetch comprehensive metrics for a domain from Sincera API
    
    Args:
        domain: Domain to fetch data for
        max_retries: Maximum retry attempts
        
    Returns:
        dict: Metrics data or error information
    """
    
    for attempt in range(max_retries):
        try:
            respect_rate_limits()
            
            response = requests.get(
                API_BASE_URL,
                params={'domain': domain},
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract all available metrics matching actual API response structure
                metrics = {
                    'domain': domain,
                    'publisher_id': data.get('publisher_id'),
                    'name': data.get('name'),
                    'visit_enabled': data.get('visit_enabled'),
                    'status': data.get('status'),
                    'primary_supply_type': data.get('primary_supply_type'),
                    'pub_description': data.get('pub_description'),
                    'categories': data.get('categories'),  # IAB categories list
                    'slug': data.get('slug'),
                    
                    # Core quality metrics
                    'avg_ads_to_content_ratio': data.get('avg_ads_to_content_ratio'),
                    'avg_ads_in_view': data.get('avg_ads_in_view'),
                    'avg_ad_refresh': data.get('avg_ad_refresh'),
                    'avg_page_weight': data.get('avg_page_weight'),
                    'avg_cpu': data.get('avg_cpu'),
                    
                    # Supply chain metrics  
                    'total_supply_paths': data.get('total_supply_paths'),
                    'reseller_count': data.get('reseller_count'),
                    'total_unique_gpids': data.get('total_unique_gpids'),
                    'id_absorption_rate': data.get('id_absorption_rate'),
                    'owner_domain': data.get('owner_domain'),
                    
                    # Metadata
                    'updated_at': data.get('updated_at'),
                    'api_success': True,
                    'api_error': None,
                    'fetched_at': datetime.now().isoformat()
                }
                
                return metrics
            
            elif response.status_code == 404:
                return {
                    'domain': domain,
                    'api_success': False,
                    'api_error': 'Domain not found (404)',
                    'fetched_at': datetime.now().isoformat()
                }
            
            elif response.status_code == 429:
                # Rate limited - check for Retry-After header
                retry_after = response.headers.get('Retry-After', '60')
                wait_time = int(retry_after)
                print(f"⏳ Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            else:
                return {
                    'domain': domain,
                    'api_success': False,
                    'api_error': f'HTTP {response.status_code}: {response.text[:200]}',
                    'fetched_at': datetime.now().isoformat()
                }
                
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                return {
                    'domain': domain,
                    'api_success': False,
                    'api_error': f'Request exception: {str(e)}',
                    'fetched_at': datetime.now().isoformat()
                }
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return {
        'domain': domain,
        'api_success': False,
        'api_error': 'Max retries exceeded',
        'fetched_at': datetime.now().isoformat()
    }

def create_test_sample(sample_df, test_size_per_network=10):
    """
    Create a small test sample from the full sample
    
    Args:
        sample_df: Full sample dataframe
        test_size_per_network: Number of domains per network for testing
    
    Returns:
        pd.DataFrame: Test sample dataframe
    """
    test_samples = []
    
    print(f"Creating test sample: {test_size_per_network} domains per network")
    
    for network in sample_df['network'].unique():
        network_df = sample_df[sample_df['network'] == network]
        
        if len(network_df) <= test_size_per_network:
            test_sample = network_df
            print(f"  {network}: Using all {len(test_sample)} domains")
        else:
            test_sample = network_df.sample(n=test_size_per_network, random_state=42)
            print(f"  {network}: Sampled {len(test_sample)} from {len(network_df)} domains")
        
        test_samples.append(test_sample)
    
    test_df = pd.concat(test_samples, ignore_index=True)
    print(f"Total test sample: {len(test_df)} domains")
    
    return test_df

def save_incremental_progress(results, batch_num):
    """Save progress incrementally to avoid losing data"""
    os.makedirs('pickles', exist_ok=True)
    filename = f'pickles/sincera_data_batch_{batch_num:04d}.pkl'
    
    # Convert to DataFrame and save
    df = pd.DataFrame(results)
    df.to_pickle(filename)
    return filename

def get_dynamic_table_name():
    """Generate dynamic table name based on current month/year"""
    current_date = datetime.now()
    year_month = current_date.strftime("%Y%m")  # YYYYMM format
    return f"da_sincera_data_{year_month}"

def upload_to_snowflake(results_df, session, table_name):
    """
    Upload results to Snowflake using overwrite mode (auto-creates table schema)
    
    Args:
        results_df: DataFrame with results
        session: Snowpark session  
        table_name: Target table name
    """
    
    print(f"📤 Uploading {len(results_df):,} records to Snowflake...")
    print(f"📊 Table: ANALYTICS.DI_AGGREGATIONS.{table_name}")
    
    # Convert column names to uppercase for Snowflake convention
    upload_df = results_df.copy()
    upload_df.columns = upload_df.columns.str.upper()
    
    print(f"📋 Column names converted to uppercase for Snowflake")
    
    # Drop table if exists to ensure clean schema creation
    drop_sql = f"DROP TABLE IF EXISTS ANALYTICS.DI_AGGREGATIONS.{table_name}"
    session.sql(drop_sql).collect()
    print(f"📋 Dropped existing table if it existed")
    
    # Create Snowpark DataFrame and overwrite table (auto-creates schema)
    snowpark_df = session.create_dataframe(upload_df)
    snowpark_df.write.mode("overwrite").save_as_table(f"ANALYTICS.DI_AGGREGATIONS.{table_name}")
    
    print(f"✅ Upload complete: {len(results_df):,} records")
    
    # Verify the upload
    count_sql = f"SELECT COUNT(*) as record_count FROM ANALYTICS.DI_AGGREGATIONS.{table_name}"
    result = session.sql(count_sql).collect()
    total_records = result[0]['RECORD_COUNT']
    print(f"📊 Total records in table: {total_records:,}")

def collect_all_data(sample_df, batch_size=100, is_testing=False):
    """
    Collect Sincera data for all domains with progress tracking
    
    Args:
        sample_df: Sample dataframe with domains
        batch_size: Save progress every N domains  
        is_testing: If True, use smaller batch size and different messaging
    """
    
    mode = "TESTING" if is_testing else "FULL"
    print(f"🚀 Starting Sincera data collection ({mode} MODE)...")
    print(f"📊 Total domains: {len(sample_df):,}")
    print(f"⏱️  Estimated time: {len(sample_df) / REQUESTS_PER_MINUTE:.1f} minutes")
    
    # Use smaller batch size for testing
    if is_testing:
        batch_size = min(10, batch_size)
    print(f"💾 Saving progress every {batch_size} domains")
    
    all_results = []
    successful_requests = 0
    failed_requests = 0
    start_time = datetime.now()
    
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        domain = row['domain']
        network = row['network']
        
        print(f"\n[{i:,}/{len(sample_df):,}] Fetching: {domain} ({network})")
        
        result = fetch_domain_metrics(domain)
        result['network'] = network  # Add network info
        all_results.append(result)
        
        if result['api_success']:
            successful_requests += 1
            a2cr = result.get('avg_ads_to_content_ratio', 'N/A')
            print(f"  ✅ Success - A2CR: {a2cr}")
        else:
            failed_requests += 1
            print(f"  ❌ Failed - {result['api_error']}")
        
        # Save progress every batch_size domains
        if i % batch_size == 0:
            batch_num = i // batch_size
            filename = save_incremental_progress(all_results[-batch_size:], batch_num)
            print(f"  💾 Progress saved: {filename}")
        
        # Progress update
        if i % 50 == 0:
            elapsed = datetime.now() - start_time
            rate = i / elapsed.total_seconds() * 60  # requests per minute
            remaining = len(sample_df) - i
            eta_minutes = remaining / rate if rate > 0 else 0
            
            print(f"\n📈 PROGRESS UPDATE:")
            print(f"   Completed: {i:,}/{len(sample_df):,} ({i/len(sample_df)*100:.1f}%)")
            print(f"   Success rate: {successful_requests}/{i} ({successful_requests/i*100:.1f}%)")
            print(f"   Current rate: {rate:.1f} req/min")
            print(f"   ETA: {eta_minutes:.1f} minutes")
    
    # Save any remaining results
    if len(all_results) % batch_size != 0:
        remaining_start = -(len(all_results) % batch_size)
        batch_num = len(all_results) // batch_size + 1
        save_incremental_progress(all_results[remaining_start:], batch_num)
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n" + "=" * 60)
    print("DATA COLLECTION COMPLETE ✅")
    print("=" * 60)
    print(f"Total domains processed: {len(all_results):,}")
    print(f"Successful: {successful_requests:,} ({successful_requests/len(all_results)*100:.1f}%)")
    print(f"Failed: {failed_requests:,} ({failed_requests/len(all_results)*100:.1f}%)")
    print(f"Duration: {duration}")
    print(f"Average rate: {len(all_results) / duration.total_seconds() * 60:.1f} req/min")
    
    return all_results

def main(is_testing=False, upload_to_snowflake_flag=True):
    """
    Main function to collect Sincera data
    
    Args:
        is_testing: If True, run on small test sample only
        upload_to_snowflake_flag: If True, upload results to Snowflake
    """
    
    mode = "TESTING" if is_testing else "FULL"
    print("=" * 60)
    print(f"STEP 2: COLLECTING SINCERA DATA ({mode} MODE)")
    print("=" * 60)
    
    try:
        # Load sample domains
        print("📋 Loading domain sample...")
        if not os.path.exists('pickles/sample_domains.pkl'):
            raise FileNotFoundError("Sample domains not found. Run '1_create_domain_sample.py' first.")
        
        full_sample_df = pd.read_pickle('pickles/sample_domains.pkl')
        print(f"✅ Loaded {len(full_sample_df):,} domains from full sample")
        
        # Create test sample if in testing mode
        if is_testing:
            sample_df = create_test_sample(full_sample_df, test_size_per_network=10)
            filename_suffix = "_test"
        else:
            sample_df = full_sample_df
            filename_suffix = "_complete"
        
        # Show network distribution
        print(f"\n{mode} Network distribution:")
        for network, count in sample_df['network'].value_counts().items():
            print(f"  {network}: {count:,} domains")
        
        # Collect all data
        results = collect_all_data(sample_df, batch_size=100, is_testing=is_testing)
        
        # Convert to DataFrame and save final dataset
        print("\n💾 Saving final dataset...")
        results_df = pd.DataFrame(results)
        
        # Save as pickle
        final_pickle_path = f'pickles/sincera_metrics{filename_suffix}.pkl'
        results_df.to_pickle(final_pickle_path)
        print(f"✅ Final dataset saved: {final_pickle_path}")
        
        # Also save as CSV for easier viewing
        csv_path = f'pickles/sincera_metrics{filename_suffix}.csv'
        results_df.to_csv(csv_path, index=False)
        print(f"✅ CSV version saved: {csv_path}")
        
        # Upload to Snowflake if requested
        if upload_to_snowflake_flag:
            print(f"\n🗄️  Uploading to Snowflake...")
            
            # Create Snowpark session
            session = func_create_snowpark_session(USER_NAME="smaity")
            session.sql("use schema DI_AGGREGATIONS").collect()
            
            # Get dynamic table name
            table_name = get_dynamic_table_name()
            
            # Upload data (auto-creates table with overwrite mode)
            upload_to_snowflake(results_df, session, table_name)
            
            print(f"✅ Data uploaded to: ANALYTICS.DI_AGGREGATIONS.{table_name}")
        
        # Provide next steps
        if is_testing:
            print(f"\n🧪 TESTING COMPLETE!")
            print(f"   Review results in: {final_pickle_path}")
            print(f"   If satisfied, run: python 2_collect_sincera_data.py --full")
        else:
            print(f"\n🎉 FULL DATA COLLECTION COMPLETE!")
            print(f"   Next step: Run QA notebook to verify results")
        
        return results_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect Sincera API data')
    parser.add_argument('--test', action='store_true', 
                       help='Run in testing mode (10 domains per network)')
    parser.add_argument('--full', action='store_true', 
                       help='Run full data collection (all domains)')
    parser.add_argument('--no-snowflake', action='store_true',
                       help='Skip Snowflake upload')
    
    args = parser.parse_args()
    
    # Determine mode
    if args.test and args.full:
        print("❌ Error: Cannot use both --test and --full flags")
        exit(1)
    elif args.test:
        is_testing = True
    elif args.full:
        is_testing = False
    else:
        # Default: ask user
        print("Select mode:")
        print("1. Testing mode (10 domains per network)")
        print("2. Full mode (all domains)")
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "1":
            is_testing = True
        elif choice == "2":
            is_testing = False
        else:
            print("Invalid choice. Defaulting to testing mode.")
            is_testing = True
    
    upload_to_snowflake_flag = not args.no_snowflake
    
    # Run main function
    main(is_testing=is_testing, upload_to_snowflake_flag=upload_to_snowflake_flag)