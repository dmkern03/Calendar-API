"""
Calendar Dimension Table Generator for Databricks
Generates a comprehensive calendar dimension and writes directly to Delta table.
Target: common.reference.dim_calendar
All datetime fields are aligned to Eastern Time (America/New_York).
"""
import time

try:
    import holidays
except ImportError:
    %pip install holidays --quiet
    time.sleep(10)
    dbutils.library.restartPython()

    print("Waiting 60 seconds...")
    time.sleep(60)
    print("Done waiting.")
    import holidays

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import requests
import os


# Configuration
TIMEZONE = ZoneInfo("America/New_York")  # Eastern Time (US and Canada)
START_DATE = date(1854, 12, 1)
END_DATE = datetime.now(TIMEZONE).date()  # Current date in Eastern Time
TARGET_TABLE = "common.reference.dim_calendar"

# US Federal Holidays (includes observed dates)
US_HOLIDAYS = holidays.US(years=range(START_DATE.year, END_DATE.year + 1))

# FRED API Key - retrieved from Databricks secrets
# Get a free API key at: https://fred.stlouisfed.org/docs/api/api_key.html
SECRET_SCOPE = "fred-api"
SECRET_KEY = "api-key"

try:
    # This works in Databricks notebooks
    FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    print(f"✓ API key retrieved from secret scope: {SECRET_SCOPE}")
except NameError:
    # Fallback for local testing (dbutils not available)
    FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_API_KEY_HERE")
    print("⚠ Running outside Databricks - using environment variable")


def fetch_fred_recession_data():
    """
    Fetch USRECD (US Recession Indicator) from FRED API.
    Returns a dictionary mapping date to recession indicator (0 or 1).
    """
    print("Fetching FRED USRECD data...")
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "USRECD",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE.strftime("%Y-%m-%d"),
        "observation_end": END_DATE.strftime("%Y-%m-%d"),
    }
    
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        recession_dict = {}
        for obs in data.get("observations", []):
            obs_date = obs["date"]
            value = obs["value"]
            if value != "." and value is not None:
                recession_dict[obs_date] = int(float(value))
        
        print(f"Retrieved {len(recession_dict)} USRECD observations from FRED")
        return recession_dict
    
    except Exception as e:
        print(f"Warning: Could not fetch FRED data: {e}")
        print("Proceeding without recession indicator data...")
        return {}


def get_quarter_dates(d):
    """Return start and end dates for the quarter containing date d."""
    quarter = (d.month - 1) // 3 + 1
    quarter_start = date(d.year, (quarter - 1) * 3 + 1, 1)
    if quarter < 4:
        quarter_end = date(d.year, quarter * 3 + 1, 1) - timedelta(days=1)
    else:
        quarter_end = date(d.year, 12, 31)
    return quarter_start, quarter_end


def localize_to_eastern(dt):
    """Convert a date or datetime to Eastern timezone datetime."""
    if isinstance(dt, date) and not isinstance(dt, datetime):
        # Convert date to datetime at midnight Eastern
        return datetime(dt.year, dt.month, dt.day, tzinfo=TIMEZONE)
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=TIMEZONE)
        else:
            return dt.astimezone(TIMEZONE)
    return dt


def generate_calendar_dimension():
    """Generate the complete calendar dimension dataframe."""
    
    print(f"Generating calendar from {START_DATE} to {END_DATE}...")
    print(f"Timezone: {TIMEZONE}")
    
    # Create date range with Eastern timezone
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D', tz=TIMEZONE)
    df = pd.DataFrame({'full_date': dates})
    
    # Fetch FRED recession data
    recession_data = fetch_fred_recession_data()
    
    # ===========================================
    # DATE KEYS & IDENTIFIERS
    # ===========================================
    df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['calendar_date'] = df['full_date'].dt.date  # DATE type field
    df['date_string'] = df['full_date'].dt.strftime('%m/%d/%Y')
    
    # ===========================================
    # STANDARD CALENDAR COMPONENTS
    # ===========================================
    df['day_of_week'] = df['full_date'].dt.dayofweek + 1
    df['day_name'] = df['full_date'].dt.day_name()
    df['day_abbrev'] = df['full_date'].dt.strftime('%a')
    df['day_of_month'] = df['full_date'].dt.day
    df['day_of_year'] = df['full_date'].dt.dayofyear
    df['week_of_year'] = df['full_date'].dt.isocalendar().week.astype(int)
    df['week_of_month'] = df['full_date'].apply(lambda x: (x.day - 1) // 7 + 1)
    df['month_number'] = df['full_date'].dt.month
    df['month_name'] = df['full_date'].dt.month_name()
    df['month_abbrev'] = df['full_date'].dt.strftime('%b')
    df['quarter'] = df['full_date'].dt.quarter
    df['quarter_name'] = 'Q' + df['quarter'].astype(str)
    df['year'] = df['full_date'].dt.year
    df['year_month'] = df['full_date'].dt.strftime('%Y%m').astype(int)
    
    # ===========================================
    # PERIOD FLAGS & INDICATORS
    # ===========================================
    df['is_weekend'] = (df['day_of_week'] >= 6).astype(int)
    df['is_weekday'] = (df['day_of_week'] < 6).astype(int)
    
    # Use holidays library for holiday detection (includes observed dates)
    df['is_holiday'] = df['full_date'].apply(lambda x: 1 if x.date() in US_HOLIDAYS else 0)
    df['holiday_name'] = df['full_date'].apply(lambda x: US_HOLIDAYS.get(x.date()))
    
    df['is_business_day'] = ((df['is_weekday'] == 1) & (df['is_holiday'] == 0)).astype(int)
    df['is_month_end'] = df['full_date'].dt.is_month_end.astype(int)
    df['is_quarter_end'] = df['full_date'].dt.is_quarter_end.astype(int)
    df['is_year_end'] = df['full_date'].dt.is_year_end.astype(int)
    
    # Last business day of month
    df['is_last_business_day_of_month'] = 0
    for idx in df.index:
        current_date = df.loc[idx, 'full_date']
        if df.loc[idx, 'is_business_day'] == 1:
            month_end = current_date + pd.offsets.MonthEnd(0)
            remaining_dates = pd.date_range(start=current_date + timedelta(days=1), end=month_end, freq='D', tz=TIMEZONE)
            has_future_business_day = False
            for future_date in remaining_dates:
                future_d = future_date.date()
                if future_date.weekday() < 5 and future_d not in US_HOLIDAYS:
                    has_future_business_day = True
                    break
            if not has_future_business_day:
                df.loc[idx, 'is_last_business_day_of_month'] = 1
    
    # ===========================================
    # RELATIVE PERIOD CALCULATIONS
    # ===========================================
    df['days_in_month'] = df['full_date'].dt.daysinmonth
    df['days_remaining_in_month'] = df['days_in_month'] - df['day_of_month']
    df['days_in_quarter'] = df['full_date'].apply(lambda x: (get_quarter_dates(x.date())[1] - get_quarter_dates(x.date())[0]).days + 1)
    df['days_remaining_in_quarter'] = df['full_date'].apply(lambda x: (get_quarter_dates(x.date())[1] - x.date()).days)
    
    # Prior year and prior month dates - localized to Eastern timezone
    df['prior_year_date'] = df['full_date'].apply(lambda x: localize_to_eastern((x - pd.DateOffset(years=1)).to_pydatetime()))
    df['prior_month_date'] = df['full_date'].apply(lambda x: localize_to_eastern((x - pd.DateOffset(months=1)).to_pydatetime()))
    df['same_day_prior_year_key'] = df['prior_year_date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    
    # ===========================================
    # REPORTING HELPERS
    # ===========================================
    today = pd.Timestamp(datetime.now(TIMEZONE).date(), tz=TIMEZONE)
    current_year = today.year
    current_quarter = (today.month - 1) // 3 + 1
    current_month = today.month
    
    df['ytd_flag'] = ((df['full_date'].dt.year == current_year) & (df['full_date'] <= today)).astype(int)
    df['qtd_flag'] = ((df['full_date'].dt.year == current_year) & (df['quarter'] == current_quarter) & (df['full_date'] <= today)).astype(int)
    df['mtd_flag'] = ((df['full_date'].dt.year == current_year) & (df['full_date'].dt.month == current_month) & (df['full_date'] <= today)).astype(int)
    df['trading_day_of_month'] = df.groupby([df['full_date'].dt.year, df['full_date'].dt.month])['is_business_day'].cumsum()
    df.loc[df['is_business_day'] == 0, 'trading_day_of_month'] = 0
    df['trading_day_of_year'] = df.groupby(df['full_date'].dt.year)['is_business_day'].cumsum()
    df.loc[df['is_business_day'] == 0, 'trading_day_of_year'] = 0
    
    # ===========================================
    # FRED RECESSION INDICATOR
    # ===========================================
    df['usrecd_recession_indicator'] = df['full_date'].dt.strftime('%Y-%m-%d').map(recession_data)
    
    # ===========================================
    # CONVERT DATETIME FIELDS TO EASTERN TIMEZONE
    # ===========================================
    # full_date is already in Eastern timezone from date_range
    # Convert prior_year_date and prior_month_date to timezone-aware timestamps
    df['prior_year_date'] = pd.to_datetime(df['prior_year_date']).dt.tz_localize(None).dt.tz_localize(TIMEZONE)
    df['prior_month_date'] = pd.to_datetime(df['prior_month_date']).dt.tz_localize(None).dt.tz_localize(TIMEZONE)
    
    # ===========================================
    # FINAL COLUMN ORDER
    # ===========================================
    column_order = [
        'date_key', 'full_date', 'calendar_date', 'date_string',
        'day_of_week', 'day_name', 'day_abbrev', 'day_of_month', 'day_of_year',
        'week_of_year', 'week_of_month', 'month_number', 'month_name', 'month_abbrev',
        'quarter', 'quarter_name', 'year', 'year_month',
        'is_weekend', 'is_weekday', 'is_holiday', 'holiday_name', 'is_business_day',
        'is_month_end', 'is_quarter_end', 'is_year_end', 'is_last_business_day_of_month',
        'days_in_month', 'days_remaining_in_month', 'days_in_quarter', 'days_remaining_in_quarter',
        'prior_year_date', 'prior_month_date', 'same_day_prior_year_key',
        'ytd_flag', 'qtd_flag', 'mtd_flag', 'trading_day_of_month', 'trading_day_of_year',
        'usrecd_recession_indicator'
    ]
    
    df = df[column_order]
    print(f"Generated {len(df):,} calendar records")
    print(f"All datetime fields aligned to: {TIMEZONE}")
    return df


def main():
    """Main execution function for Databricks."""
    from pyspark.sql import SparkSession
    
    print("=" * 60)
    print("Calendar Dimension Table Generator")
    print(f"Timezone: America/New_York (Eastern Time)")
    print("=" * 60)
    
    # Generate calendar as pandas DataFrame
    pdf = generate_calendar_dimension()
    
    # Get Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Set Spark session timezone to Eastern
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    
    # Convert to Spark DataFrame
    print(f"\nConverting to Spark DataFrame...")
    df = spark.createDataFrame(pdf)
    
    # Create temporary staging view
    df.createOrReplaceTempView("calendar_staging")
    
    # Write to Delta table using MERGE (preserves table metadata/comments)
    print(f"Merging into {TARGET_TABLE}...")
    spark.sql("""
        MERGE INTO common.reference.dim_calendar AS target
        USING calendar_staging AS source
        ON target.date_key = source.date_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Optimize table
    print("Optimizing table...")
    spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (year, month_number, date_key)")
    
    # Validate
    row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0]['cnt']
    date_range = spark.sql(f"SELECT MIN(full_date) as min_dt, MAX(full_date) as max_dt FROM {TARGET_TABLE}").collect()[0]
    
    print("\n" + "=" * 60)
    print("LOAD COMPLETE")
    print("=" * 60)
    print(f"Target Table:  {TARGET_TABLE}")
    print(f"Total Rows:    {row_count:,}")
    print(f"Date Range:    {date_range['min_dt']} to {date_range['max_dt']}")
    print(f"Timezone:      America/New_York (Eastern Time)")
    print("=" * 60)


if __name__ == "__main__":
    main()
