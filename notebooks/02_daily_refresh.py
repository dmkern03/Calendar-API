# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Refresh: Calendar Dimension Table
# MAGIC 
# MAGIC This notebook refreshes the calendar dimension table daily.
# MAGIC 
# MAGIC **Schedule this notebook to run daily (e.g., 6:00 AM Eastern).**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install holidays --quiet

# COMMAND ----------

# Restart Python after pip install
import time
time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import requests
import os
import holidays

# Configuration
TIMEZONE = ZoneInfo("America/New_York")
START_DATE = date(1854, 12, 1)
END_DATE = datetime.now(TIMEZONE).date()
TARGET_TABLE = "common.reference.dim_calendar"

# US Federal Holidays
US_HOLIDAYS = holidays.US(years=range(START_DATE.year, END_DATE.year + 1))

# FRED API Key
SECRET_SCOPE = "fred-api"
SECRET_KEY = "api-key"

try:
    FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    print(f"✓ API key retrieved from secret scope: {SECRET_SCOPE}")
except Exception as e:
    FRED_API_KEY = None
    print(f"⚠ Could not retrieve API key: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def fetch_fred_recession_data():
    """Fetch USRECD from FRED API."""
    if not FRED_API_KEY:
        print("No FRED API key available, skipping recession data")
        return {}
    
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
        
        print(f"Retrieved {len(recession_dict)} USRECD observations")
        return recession_dict
    except Exception as e:
        print(f"Warning: Could not fetch FRED data: {e}")
        return {}


def get_quarter_dates(d):
    """Return start and end dates for the quarter."""
    quarter = (d.month - 1) // 3 + 1
    quarter_start = date(d.year, (quarter - 1) * 3 + 1, 1)
    if quarter < 4:
        quarter_end = date(d.year, quarter * 3 + 1, 1) - timedelta(days=1)
    else:
        quarter_end = date(d.year, 12, 31)
    return quarter_start, quarter_end


def localize_to_eastern(dt):
    """Convert to Eastern timezone."""
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return datetime(dt.year, dt.month, dt.day, tzinfo=TIMEZONE)
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=TIMEZONE)
        else:
            return dt.astimezone(TIMEZONE)
    return dt

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Calendar Data

# COMMAND ----------

def generate_calendar_dimension():
    """Generate the calendar dimension dataframe."""
    
    print(f"Generating calendar from {START_DATE} to {END_DATE}...")
    print(f"Timezone: {TIMEZONE}")
    
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D', tz=TIMEZONE)
    df = pd.DataFrame({'full_date': dates})
    
    recession_data = fetch_fred_recession_data()
    
    # Date Keys & Identifiers
    df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['calendar_date'] = df['full_date'].dt.date
    df['date_string'] = df['full_date'].dt.strftime('%m/%d/%Y')
    
    # Standard Calendar Components
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
    
    # Period Flags & Indicators
    df['is_weekend'] = (df['day_of_week'] >= 6).astype(int)
    df['is_weekday'] = (df['day_of_week'] < 6).astype(int)
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
    
    # Relative Period Calculations
    df['days_in_month'] = df['full_date'].dt.daysinmonth
    df['days_remaining_in_month'] = df['days_in_month'] - df['day_of_month']
    df['days_in_quarter'] = df['full_date'].apply(lambda x: (get_quarter_dates(x.date())[1] - get_quarter_dates(x.date())[0]).days + 1)
    df['days_remaining_in_quarter'] = df['full_date'].apply(lambda x: (get_quarter_dates(x.date())[1] - x.date()).days)
    df['prior_year_date'] = df['full_date'].apply(lambda x: localize_to_eastern((x - pd.DateOffset(years=1)).to_pydatetime()))
    df['prior_month_date'] = df['full_date'].apply(lambda x: localize_to_eastern((x - pd.DateOffset(months=1)).to_pydatetime()))
    df['same_day_prior_year_key'] = df['prior_year_date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    
    # Reporting Helpers
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
    
    # FRED Recession Indicator
    df['usrecd_recession_indicator'] = df['full_date'].dt.strftime('%Y-%m-%d').map(recession_data)
    
    # Convert datetime fields
    df['prior_year_date'] = pd.to_datetime(df['prior_year_date']).dt.tz_localize(None).dt.tz_localize(TIMEZONE)
    df['prior_month_date'] = pd.to_datetime(df['prior_month_date']).dt.tz_localize(None).dt.tz_localize(TIMEZONE)
    
    # Final column order
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
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate and Load Data

# COMMAND ----------

print("=" * 60)
print("Calendar Dimension Table Generator")
print(f"Timezone: America/New_York (Eastern Time)")
print("=" * 60)

# Generate calendar
pdf = generate_calendar_dimension()

# COMMAND ----------

# Set Spark timezone
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# Convert to Spark DataFrame
print("\nConverting to Spark DataFrame...")
df = spark.createDataFrame(pdf)

# Create staging view
df.createOrReplaceTempView("calendar_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Merge into Target Table

# COMMAND ----------

print(f"Merging into {TARGET_TABLE}...")

spark.sql("""
    MERGE INTO common.reference.dim_calendar AS target
    USING calendar_staging AS source
    ON target.date_key = source.date_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("Merge complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optimize Table

# COMMAND ----------

print("Optimizing table...")
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (year, month_number, date_key)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Load

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample recent data
# MAGIC SELECT *
# MAGIC FROM common.reference.dim_calendar
# MAGIC WHERE full_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY full_date DESC
