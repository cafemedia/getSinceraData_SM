#!/usr/bin/env python3
"""
Step 1: Create Domain Sample
============================
This script:
1. Connects to Snowflake
2. Pulls clean domain data from SQL queries
3. Creates a sample dataset (500 per network, all for Aditude)
4. Saves the sample as a pickle file for the next step

Usage:
    python 1_create_domain_sample.py
"""

import sys
import os
import warnings
warnings.filterwarnings('ignore')

# Add current directory to Python path
sys.path.append(os.getcwd())

import yaml
import pandas as pd
from snowpark_connect import func_create_snowpark_session

def load_sql_queries(yaml_path='sql_queries.yaml'):
    """
    Load SQL queries from YAML file
    
    Returns:
        dict: Dictionary containing SQL queries
    """
    with open(yaml_path, 'r') as file:
        queries = yaml.safe_load(file)
    return queries

def create_clean_domain_dataset(session, sql_queries):
    """
    Create a clean dataset of domains with networks, ensuring no Raptive domains
    appear in competitor data (Raptive internal source is most reliable)
    
    Args:
        session: Snowpark session object
        sql_queries: Dictionary containing SQL queries from YAML
    
    Returns:
        pd.DataFrame: Clean dataframe with 'domain' and 'network' columns
    """
    
    # Execute Raptive query
    print("Fetching Raptive domains...")
    raptive_query = sql_queries['df_raptive_query']['select']
    raptive_df = session.sql(raptive_query).to_pandas()
    
    # Standardize column names - Snowflake returns uppercase
    raptive_df = raptive_df.rename(columns={'URL': 'domain'})
    raptive_df['network'] = 'Raptive'
    raptive_df = raptive_df[['domain', 'network']]
    
    # Clean and normalize Raptive domains
    raptive_df['domain'] = raptive_df['domain'].str.lower().str.strip()
    raptive_df = raptive_df.dropna(subset=['domain'])
    raptive_df = raptive_df[raptive_df['domain'] != '']
    
    # Remove any protocol prefixes and paths from Raptive domains
    raptive_df['domain'] = raptive_df['domain'].str.replace(r'^https?://', '', regex=True)
    raptive_df['domain'] = raptive_df['domain'].str.replace(r'^www\.', '', regex=True)
    raptive_df['domain'] = raptive_df['domain'].str.split('/').str[0]
    
    print(f"Found {len(raptive_df)} Raptive domains")
    
    # Execute competitor query
    print("Fetching competitor domains...")
    competitor_query = sql_queries['df_competitor_sites_query']['select']
    competitor_df = session.sql(competitor_query).to_pandas()
    
    # Clean and normalize competitor domains - Snowflake columns are uppercase
    competitor_df['DOMAIN'] = competitor_df['DOMAIN'].str.lower().str.strip()
    competitor_df = competitor_df.dropna(subset=['DOMAIN'])
    competitor_df = competitor_df[competitor_df['DOMAIN'] != '']
    
    # Remove any protocol prefixes and paths from competitor domains
    competitor_df['DOMAIN'] = competitor_df['DOMAIN'].str.replace(r'^https?://', '', regex=True)
    competitor_df['DOMAIN'] = competitor_df['DOMAIN'].str.replace(r'^www\.', '', regex=True) 
    competitor_df['DOMAIN'] = competitor_df['DOMAIN'].str.split('/').str[0]
    
    # Rename to lowercase for consistency
    competitor_df = competitor_df.rename(columns={'DOMAIN': 'domain', 'NETWORK': 'network'})
    
    print(f"Found {len(competitor_df)} total competitor domains")
    
    # Remove Raptive domains from competitor data (deduplication)
    raptive_domains_set = set(raptive_df['domain'].unique())
    competitor_df_clean = competitor_df[~competitor_df['domain'].isin(raptive_domains_set)]
    
    removed_count = len(competitor_df) - len(competitor_df_clean)
    if removed_count > 0:
        print(f"Removed {removed_count} Raptive domains found in competitor data")
    
    print(f"Clean competitor domains: {len(competitor_df_clean)}")
    
    # Combine datasets
    final_df = pd.concat([raptive_df, competitor_df_clean], ignore_index=True)
    
    # Final cleanup - remove duplicates and sort
    final_df = final_df.drop_duplicates(subset=['domain']).reset_index(drop=True)
    final_df = final_df.sort_values(['network', 'domain']).reset_index(drop=True)
    
    # Print summary
    print("\n=== FINAL DATASET SUMMARY ===")
    network_counts = final_df['network'].value_counts()
    for network, count in network_counts.items():
        print(f"{network}: {count} domains")
    print(f"Total: {len(final_df)} domains")
    
    return final_df[['domain', 'network']]

def create_sample_dataset(clean_df, sample_size=500):
    """
    Create a sample dataset with specified sample size per network
    Special handling for Aditude (take all domains)
    
    Args:
        clean_df: Clean dataframe with 'domain' and 'network' columns
        sample_size: Number of domains to sample per network (default 500)
    
    Returns:
        pd.DataFrame: Sampled dataframe
    """
    import os
    
    sampled_dfs = []
    
    print("=== CREATING SAMPLE DATASET ===")
    
    for network in clean_df['network'].unique():
        network_df = clean_df[clean_df['network'] == network].copy()
        
        if network == 'Aditude':
            # Take all Aditude domains
            sample_df = network_df
            print(f"{network}: Taking all {len(sample_df)} domains")
        else:
            # Sample specified number for other networks
            if len(network_df) <= sample_size:
                sample_df = network_df
                print(f"{network}: Taking all {len(sample_df)} domains (less than {sample_size})")
            else:
                sample_df = network_df.sample(n=sample_size, random_state=42)
                print(f"{network}: Sampled {len(sample_df)} domains from {len(network_df)}")
        
        sampled_dfs.append(sample_df)
    
    # Combine all samples
    final_sample = pd.concat(sampled_dfs, ignore_index=True)
    
    print(f"\n=== SAMPLE SUMMARY ===")
    sample_counts = final_sample['network'].value_counts()
    for network, count in sample_counts.items():
        print(f"{network}: {count} domains")
    print(f"Total sample size: {len(final_sample)}")
    
    # Create pickles folder if it doesn't exist
    os.makedirs('pickles', exist_ok=True)
    
    # Save as pickle
    pickle_path = 'pickles/sample_domains.pkl'
    final_sample.to_pickle(pickle_path)
    print(f"\nSample saved to: {pickle_path}")
    
    return final_sample

def main():
    """Main function to create domain sample"""
    
    print("=" * 60)
    print("STEP 1: CREATING DOMAIN SAMPLE")
    print("=" * 60)
    
    try:
        # Create Snowpark session
        print("🔗 Connecting to Snowflake...")
        session = func_create_snowpark_session(USER_NAME="smaity")
        session.sql("use schema DI_AGGREGATIONS").collect()
        print("✅ Connected to Snowflake")
        
        # Load SQL queries
        print("\n📋 Loading SQL queries...")
        sql_queries = load_sql_queries()
        print("✅ SQL queries loaded")
        
        # Create clean domain dataset
        print("\n🧹 Creating clean domain dataset...")
        clean_df = create_clean_domain_dataset(session, sql_queries)
        print(f"✅ Clean dataset created: {len(clean_df):,} domains")
        
        # Create sample dataset
        print("\n🎯 Creating sample dataset...")
        sample_df = create_sample_dataset(clean_df, sample_size=500)
        print(f"✅ Sample created and saved to pickles/sample_domains.pkl")
        
        # Final summary
        print("\n" + "=" * 60)
        print("STEP 1 COMPLETE ✅")
        print("=" * 60)
        print(f"Total sample size: {len(sample_df):,} domains")
        print("\nNetwork distribution:")
        for network, count in sample_df['network'].value_counts().items():
            print(f"  {network}: {count:,} domains")
        
        print(f"\nNext step: Run 'python 2_collect_sincera_data.py'")
        
        return sample_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()