%sql
-- ============================================================
-- Setup: Calendar Dimension Table
-- ============================================================
-- Run this script once to create the infrastructure for the
-- calendar dimension table.
-- ============================================================

-- Create catalog
CREATE CATALOG IF NOT EXISTS common
COMMENT 'Shared catalog for cross-domain reference data and common assets';

-- Create schema
CREATE SCHEMA IF NOT EXISTS common.reference
COMMENT 'Reference and dimension tables shared across the organization';

-- Create calendar dimension table
CREATE TABLE IF NOT EXISTS common.reference.dim_calendar
(
    -- Date Keys & Identifiers
    date_key                        INT             NOT NULL    COMMENT 'Date key in YYYYMMDD format for efficient joins',
    full_date                       TIMESTAMP       NOT NULL    COMMENT 'Full date value as timestamp (Eastern timezone)',
    calendar_date                   DATE            NOT NULL    COMMENT 'Calendar date as DATE type',
    date_string                     STRING          NOT NULL    COMMENT 'Formatted date string (MM/DD/YYYY)',
    
    -- Standard Calendar Components
    day_of_week                     INT             NOT NULL    COMMENT 'Day of week (1=Monday, 7=Sunday)',
    day_name                        STRING          NOT NULL    COMMENT 'Full day name (Monday, Tuesday, etc.)',
    day_abbrev                      STRING          NOT NULL    COMMENT 'Abbreviated day name (Mon, Tue, etc.)',
    day_of_month                    INT             NOT NULL    COMMENT 'Day of month (1-31)',
    day_of_year                     INT             NOT NULL    COMMENT 'Day of year (1-366)',
    week_of_year                    INT             NOT NULL    COMMENT 'ISO week of year (1-53)',
    week_of_month                   INT             NOT NULL    COMMENT 'Week of month (1-5)',
    month_number                    INT             NOT NULL    COMMENT 'Month number (1-12)',
    month_name                      STRING          NOT NULL    COMMENT 'Full month name (January, February, etc.)',
    month_abbrev                    STRING          NOT NULL    COMMENT 'Abbreviated month name (Jan, Feb, etc.)',
    quarter                         INT             NOT NULL    COMMENT 'Quarter number (1-4)',
    quarter_name                    STRING          NOT NULL    COMMENT 'Quarter name (Q1, Q2, Q3, Q4)',
    year                            INT             NOT NULL    COMMENT 'Calendar year',
    year_month                      INT             NOT NULL    COMMENT 'Year and month in YYYYMM format',
    
    -- Period Flags & Indicators
    is_weekend                      INT             NOT NULL    COMMENT 'Weekend flag (1=Yes, 0=No)',
    is_weekday                      INT             NOT NULL    COMMENT 'Weekday flag (1=Yes, 0=No)',
    is_holiday                      INT             NOT NULL    COMMENT 'US Federal holiday flag (1=Yes, 0=No)',
    holiday_name                    STRING                      COMMENT 'Name of holiday if applicable',
    is_business_day                 INT             NOT NULL    COMMENT 'Business day flag - weekday and not holiday (1=Yes, 0=No)',
    is_month_end                    INT             NOT NULL    COMMENT 'Last day of month flag (1=Yes, 0=No)',
    is_quarter_end                  INT             NOT NULL    COMMENT 'Last day of quarter flag (1=Yes, 0=No)',
    is_year_end                     INT             NOT NULL    COMMENT 'Last day of year flag (1=Yes, 0=No)',
    is_last_business_day_of_month   INT             NOT NULL    COMMENT 'Last business day of month flag (1=Yes, 0=No)',
    
    -- Relative Period Calculations
    days_in_month                   INT             NOT NULL    COMMENT 'Total days in the month',
    days_remaining_in_month         INT             NOT NULL    COMMENT 'Days remaining in month after this date',
    days_in_quarter                 INT             NOT NULL    COMMENT 'Total days in the quarter',
    days_remaining_in_quarter       INT             NOT NULL    COMMENT 'Days remaining in quarter after this date',
    prior_year_date                 TIMESTAMP       NOT NULL    COMMENT 'Same date in prior year (Eastern timezone)',
    prior_month_date                TIMESTAMP       NOT NULL    COMMENT 'Same date in prior month (Eastern timezone)',
    same_day_prior_year_key         INT             NOT NULL    COMMENT 'Date key for same date in prior year (YYYYMMDD)',
    
    -- Reporting Helpers
    ytd_flag                        INT             NOT NULL    COMMENT 'Year-to-date flag relative to current date (1=Yes, 0=No)',
    qtd_flag                        INT             NOT NULL    COMMENT 'Quarter-to-date flag relative to current date (1=Yes, 0=No)',
    mtd_flag                        INT             NOT NULL    COMMENT 'Month-to-date flag relative to current date (1=Yes, 0=No)',
    trading_day_of_month            INT             NOT NULL    COMMENT 'Business day count within month (0 if not business day)',
    trading_day_of_year             INT             NOT NULL    COMMENT 'Business day count within year (0 if not business day)',
    
    -- FRED Economic Indicator
    usrecd_recession_indicator      INT                         COMMENT 'NBER US Recession Indicator from FRED (1=Recession, 0=Expansion, NULL=No data)'
)
USING DELTA
COMMENT 'Calendar dimension table with US holidays, business day flags, and FRED recession indicator. All timestamps in Eastern timezone (America/New_York).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'domain' = 'reference',
    'timezone' = 'America/New_York'
);

-- ============================================================
-- Verify Setup
-- ============================================================

-- Check table exists
DESCRIBE TABLE EXTENDED common.reference.dim_calendar;
