# Sincera Competitive Intelligence Pipeline

## 📋 Overview

This repository contains a comprehensive data pipeline for collecting and analyzing advertising metrics from the Sincera API to perform competitive intelligence analysis for Raptive (formerly CafeMedia). The pipeline compares Raptive's performance against key competitors across various advertising quality metrics.

## 🏗️ Architecture

### Data Flow
```
Snowflake (Internal Data) → Domain Sampling → Sincera API → Analysis → Snowflake (Results) → Streamlit Dashboard
```

1. **Domain Collection**: Pull Raptive domains from internal systems and competitor domains from Jounce intelligence data
2. **Sampling**: Create balanced samples (500 domains per network) for API efficiency  
3. **API Collection**: Fetch comprehensive metrics from Sincera API with proper rate limiting
4. **Storage**: Save results locally (pickle) and upload to Snowflake for dashboard access
5. **Analysis**: QA verification and competitive benchmarking

## 🎯 Business Objective

**Primary Goal**: Competitive intelligence to understand how Raptive performs vs competitors on key advertising quality metrics.

**Key Competitors Analyzed**:
- Mediavine
- Freestar  
- Ezoic
- Playwire
- Aditude
- Penske Media Corporation

**Key Metrics Compared**:
- **A2CR (Ads-to-Content Ratio)**: Core user experience metric
- **Supply Path Complexity**: Number of intermediaries in ad delivery
- **Page Performance**: Weight and CPU usage impact
- **Ad Density**: Average ads in view and refresh rates
- **Supply Chain**: Reseller counts and identifier rates

## 🔧 Technical Components

### Core Scripts

#### `1_create_domain_sample.py`
**Purpose**: Creates balanced domain sample from Snowflake data sources
- Connects to Snowflake using `snowpark_connect.py`
- Pulls Raptive domains from internal `adthrive.site_extended` table
- Pulls competitor domains from `analytics.sigma_aggregations.jounce_pub_network_by_month`
- Deduplicates to ensure Raptive internal data takes precedence
- Creates balanced sample (500 per network, all 118 for Aditude)
- Saves to `pickles/sample_domains.pkl`

#### `2_collect_sincera_data.py`  
**Purpose**: Collects comprehensive metrics from Sincera API
- **Testing Mode** (`--test`): 10 domains per network (~70 total, ~3 minutes)
- **Full Mode** (`--full`): 500 domains per network (~3,118 total, ~70 minutes)  
- **Rate Limiting**: Respects 45 requests/minute, 5000 requests/day limits
- **Error Handling**: Graceful handling of 404s, rate limits, network errors
- **Progress Tracking**: Incremental saves every 100 domains, real-time ETA
- **Snowflake Upload**: Creates dynamic table `da_sincera_data_YYYYMM`

#### `snowpark_connect.py`
**Purpose**: Snowflake connection management
- Creates authenticated Snowpark sessions
- Retrieves credentials from AWS SSM Parameter Store
- Session auditing and logging

#### `collect_raptive_test.py`
**Purpose**: Collects Sincera data for specific Raptive test domains
- **Test Mode** (`--test`): 3 sample domains for validation
- **Full Mode** (`--full`): All domains from `raptive_test_domains.txt`
- **Rate Limiting**: Same 45 requests/minute, 5000 requests/day limits
- **Schema Matching**: Ensures exact column order/types match main table
- **Snowflake Upload**: Creates `da_sincera_data_YYYYMM_raptive_test` table
- **Use Case**: A/B testing and performance enhancement analysis

#### `sql_queries.yaml`
**Purpose**: Centralized SQL query definitions
- Raptive domains query: `adthrive.site_extended` 
- Competitor domains query: `jounce_pub_network_by_month`

### Supporting Files

#### `requirements.txt`
Python dependencies for the pipeline

#### `OpenSincera_API_Guide.txt`
Comprehensive API documentation and field descriptions

#### `raptive_test_domains.txt`
Text file containing specific Raptive domains for A/B testing (one domain per line)

#### `.env`
Environment variables (not committed to git):
```
SINCERA_API_KEY=your_api_key_here
```

## 📊 Data Schema

### Snowflake Output Table: `da_sincera_data_YYYYMM`

| Field | Type | Description |
|-------|------|-------------|
| `domain` | VARCHAR(500) | Publisher domain |
| `network` | VARCHAR(100) | Ad network (Raptive, Mediavine, etc.) |
| `publisher_id` | NUMBER | Sincera internal ID |
| `name` | VARCHAR(1000) | Publisher name |
| `visit_enabled` | BOOLEAN | Whether Sincera actively monitors |
| `status` | VARCHAR(100) | Publisher status (available, etc.) |
| `primary_supply_type` | VARCHAR(100) | Platform type (web, ctv) |
| `pub_description` | TEXT | Publisher description |
| `categories` | VARIANT | IAB category classifications (JSON) |
| `slug` | VARCHAR(500) | URL identifier |
| `avg_ads_to_content_ratio` | FLOAT | A2CR - key UX metric (0-1 scale) |
| `avg_ads_in_view` | FLOAT | Average simultaneous ads visible |
| `avg_ad_refresh` | FLOAT | Time between ad refreshes (seconds) |
| `avg_page_weight` | FLOAT | Page size in MB |
| `avg_cpu` | FLOAT | CPU usage in seconds |
| `total_supply_paths` | NUMBER | Supply chain complexity count |
| `reseller_count` | NUMBER | Number of ad resellers |
| `total_unique_gpids` | NUMBER | Unique placement identifiers |
| `id_absorption_rate` | FLOAT | Identifier success rate (0-1 scale) |
| `owner_domain` | VARCHAR(500) | Ads.txt owner domain |
| `updated_at` | TIMESTAMP | Sincera data freshness |
| `api_success` | BOOLEAN | Whether API call succeeded |
| `api_error` | TEXT | Error details if failed |
| `fetched_at` | TIMESTAMP | When we collected the data |
| `created_at` | TIMESTAMP | Row creation time |

## 🚀 Usage Guide

### Prerequisites
1. **Environment Setup**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**:
   Create `.env` file with:
   ```
   SINCERA_API_KEY=your_sincera_api_token
   ```

3. **AWS Credentials**: 
   Configured for Snowflake parameter access

### Running the Pipeline

#### Option 1: Test First (Recommended)
```bash
# Step 1: Create domain sample
python 1_create_domain_sample.py

# Step 2: Test with small sample (70 domains, ~3 minutes)  
python 2_collect_sincera_data.py --test

# Step 3: Review test results, then run full collection
python 2_collect_sincera_data.py --full
```

#### Option 2: Direct Full Run
```bash
# Steps 1-2 combined for full collection
python 1_create_domain_sample.py
python 2_collect_sincera_data.py --full
```

#### Option 3: Skip Snowflake Upload
```bash
# For local analysis only
python 2_collect_sincera_data.py --test --no-snowflake
python 2_collect_sincera_data.py --full --no-snowflake
```

#### Option 4: Raptive Test Domains Collection
```bash
# Test specific Raptive domains for A/B testing
python collect_raptive_test.py --test    # Test with 3 domains
python collect_raptive_test.py --full    # Collect all test domains
```

### Command Reference

| Command | Purpose | Duration | Output |
|---------|---------|----------|---------|
| `python 1_create_domain_sample.py` | Create balanced sample | ~2 min | `pickles/sample_domains.pkl` |
| `python 2_collect_sincera_data.py --test` | Test API collection | ~3 min | `pickles/sincera_metrics_test.pkl` |
| `python 2_collect_sincera_data.py --full` | Full API collection | ~70 min | `pickles/sincera_metrics_complete.pkl` |
| `python collect_raptive_test.py --test` | Test Raptive domains | ~1 min | `pickles/sincera_metrics_raptive_test.pkl` |
| `python collect_raptive_test.py --full` | Collect all test domains | ~9 min | `pickles/sincera_metrics_raptive_complete.pkl` |
| `python 2_collect_sincera_data.py` | Interactive mode | Variable | User selects test/full |

## 📈 Expected Results

### Sample Sizes
- **Raptive**: 500 domains (from internal data)
- **Mediavine**: 500 domains (sampled from 18,324)
- **Ezoic**: 500 domains (sampled from 24,809)  
- **Freestar**: 500 domains (sampled from 1,261)
- **Playwire**: 500 domains (sampled from 1,036)
- **Penske Media**: 500 domains (sampled from 1,735)
- **Aditude**: 118 domains (all available)
- **Raptive-Test**: ~400 domains (from `raptive_test_domains.txt`)
- **Total**: ~3,118 domains (main) + ~400 (test)

### Success Rates
- Expect 70-90% success rate (2,200-2,900 successful API responses)
- Common failures: Domains not in Sincera database (404 errors)
- All failures are logged and reported for transparency

### Performance Benchmarks
Based on historical data, expect results like:
- **A2CR Range**: 0.10-0.30 (10-30% of page pixels for ads)
- **Supply Paths**: 50-500 intermediaries per domain  
- **Page Weight**: 5-50 MB average page sizes
- **Refresh Rates**: 30-120 seconds between ad refreshes

## 🔍 Quality Assurance

### Data Validation
- Domain deduplication (Raptive internal data takes precedence)
- Rate limiting compliance (never exceeds API limits)
- Comprehensive error logging and categorization
- Progress tracking with estimated completion times

### Verification Steps
1. **Sample Verification**: Review `pickles/sincera_metrics_test.csv` for test run
2. **Network Balance**: Ensure balanced representation across competitors  
3. **Success Rates**: Validate >70% API success rate per network
4. **Data Ranges**: Verify metrics fall within expected ranges
5. **Snowflake Validation**: Confirm data uploaded correctly

### Error Handling
- **404 Errors**: Domain not found in Sincera (expected for some domains)
- **429 Rate Limits**: Automatic retry with exponential backoff
- **Network Failures**: Retry with timeout handling
- **API Changes**: Graceful handling of unexpected response formats

## 🗄️ Data Storage Strategy

### Local Storage
- **Pickle Files**: Fast Python serialization for development
- **CSV Files**: Human-readable format for manual inspection  
- **Incremental Saves**: Progress preservation every 100 API calls

### Snowflake Storage  
- **Main Tables**: `da_sincera_data_YYYYMM` for monthly competitive data
- **Test Tables**: `da_sincera_data_YYYYMM_raptive_test` for A/B testing
- **Schema**: `ANALYTICS.DI_AGGREGATIONS` for business analytics
- **Approach**: CREATE OR REPLACE for clean monthly updates
- **Data Combination**: Use `UNION ALL BY NAME` to combine main and test tables
- **Access**: Streamlit dashboard points to current month's table

## 🎨 Future Enhancements

### Dashboard Integration
- Streamlit app consuming `da_sincera_data_YYYYMM` table
- Interactive competitive analysis charts
- Network performance comparisons and trends
- Filtering by categories, metrics, time periods

### Analysis Expansion  
- Statistical significance testing between networks
- Correlation analysis between different metrics
- Outlier detection and investigation
- Historical trending when data accumulates

### Operational Improvements
- Automated monthly refresh scheduling  
- Alert system for significant competitive changes
- Additional competitor network integration
- Enhanced error monitoring and notification

## 🔒 Security & Compliance

### API Key Management
- Sincera API key stored in `.env` (not committed)
- Environment-based configuration
- Secure credential handling through AWS SSM

### Data Privacy
- No PII collection (only domain-level aggregated metrics)
- Public advertising metrics only
- Compliance with Sincera API terms of service

### Rate Limiting
- Strict adherence to API limits (45/min, 5000/day)
- Built-in backoff and retry mechanisms
- Daily usage tracking and cutoffs

## 📞 Support & Troubleshooting

### Common Issues

**"Sample domains not found"**
- Solution: Run `python 1_create_domain_sample.py` first

**"SINCERA_API_KEY environment variable not set"**  
- Solution: Create `.env` file with your API key

**"Rate limit exceeded"**
- Solution: Script automatically handles this, wait for retry

**"Snowflake connection failed"**
- Solution: Verify AWS credentials and SSM parameter access

### Getting Help
- Review error messages in console output
- Check `pickles/` folder for partial results  
- Examine API response details in CSV outputs
- Contact Sincera support at hello@sincera.io for API issues

## 📝 Development Notes

### Project Evolution
This pipeline evolved from a simple sellers.json analysis to a comprehensive competitive intelligence platform:

1. **V1**: Basic sellers.json scraping from public files
2. **V2**: Integration with internal Snowflake data sources  
3. **V3**: Sincera API integration with comprehensive metrics
4. **V4**: Production pipeline with testing modes and Snowflake integration

### Architecture Decisions
- **Python Scripts vs Notebooks**: Scripts for production reliability, notebooks for QA only
- **Pickle + Snowflake**: Local development flexibility + production dashboard access
- **Dynamic Table Names**: Monthly data versioning for easy dashboard updates  
- **Rate Limiting**: Conservative approach to ensure API compliance
- **Error Tolerance**: Comprehensive logging without pipeline failures

### Code Organization
```
├── 1_create_domain_sample.py    # Domain sampling from Snowflake
├── 2_collect_sincera_data.py    # API data collection  
├── collect_raptive_test.py      # Raptive test domains collection
├── snowpark_connect.py          # Snowflake connection utility
├── sql_queries.yaml             # Centralized query definitions
├── raptive_test_domains.txt     # Test domains list
├── requirements.txt             # Python dependencies
├── OpenSincera_API_Guide.txt    # API documentation
├── .env                         # Environment variables (not committed)
├── pickles/                     # Local data storage (not committed)  
└── reference/                   # Legacy implementation (not committed)
```

## 🎯 Success Metrics

### Technical Success
- ✅ 70%+ API success rate across all networks
- ✅ Complete data collection within 75 minutes  
- ✅ Successful Snowflake upload with data validation
- ✅ Zero API rate limit violations

### Business Success
- ✅ Actionable competitive intelligence insights
- ✅ Clear performance benchmarking vs competitors
- ✅ Reliable monthly data refresh capability
- ✅ Foundation for strategic advertising decisions

---

*This pipeline provides Raptive with comprehensive, reliable competitive intelligence to make data-driven decisions about advertising strategy and performance optimization.*