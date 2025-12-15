# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Calendar Dimension Table
# MAGIC 
# MAGIC This notebook creates the infrastructure required for the calendar dimension table:
# MAGIC - Catalog: `common`
# MAGIC - Schema: `reference`
# MAGIC - Table: `dim_calendar`
# MAGIC 
# MAGIC **Run this notebook once during initial setup.**

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
# MAGIC ## 2. Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS common
# MAGIC COMMENT 'Shared catalog for cross-domain reference data and common assets';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS common.reference
# MAGIC COMMENT 'Reference and dimension tables shared across the organization';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Calendar Dimension Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS common.reference.dim_calendar
# MAGIC (
# MAGIC     -- Date Keys & Identifiers
# MAGIC     date_key                        INT             NOT NULL    COMMENT 'Date key in YYYYMMDD format for efficient joins',
# MAGIC     full_date                       TIMESTAMP       NOT NULL    COMMENT 'Full date value as timestamp (Eastern timezone)',
# MAGIC     calendar_date                   DATE            NOT NULL    COMMENT 'Calendar date as DATE type',
# MAGIC     date_string                     STRING          NOT NULL    COMMENT 'Formatted date string (MM/DD/YYYY)',
# MAGIC     
# MAGIC     -- Standard Calendar Components
# MAGIC     day_of_week                     INT             NOT NULL    COMMENT 'Day of week (1=Monday, 7=Sunday)',
# MAGIC     day_name                        STRING          NOT NULL    COMMENT 'Full day name (Monday, Tuesday, etc.)',
# MAGIC     day_abbrev                      STRING          NOT NULL    COMMENT 'Abbreviated day name (Mon, Tue, etc.)',
# MAGIC     day_of_month                    INT             NOT NULL    COMMENT 'Day of month (1-31)',
# MAGIC     day_of_year                     INT             NOT NULL    COMMENT 'Day of year (1-366)',
# MAGIC     week_of_year                    INT             NOT NULL    COMMENT 'ISO week of year (1-53)',
# MAGIC     week_of_month                   INT             NOT NULL    COMMENT 'Week of month (1-5)',
# MAGIC     month_number                    INT             NOT NULL    COMMENT 'Month number (1-12)',
# MAGIC     month_name                      STRING          NOT NULL    COMMENT 'Full month name (January, February, etc.)',
# MAGIC     month_abbrev                    STRING          NOT NULL    COMMENT 'Abbreviated month name (Jan, Feb, etc.)',
# MAGIC     quarter                         INT             NOT NULL    COMMENT 'Quarter number (1-4)',
# MAGIC     quarter_name                    STRING          NOT NULL    COMMENT 'Quarter name (Q1, Q2, Q3, Q4)',
# MAGIC     year                            INT             NOT NULL    COMMENT 'Calendar year',
# MAGIC     year_month                      INT             NOT NULL    COMMENT 'Year and month in YYYYMM format',
# MAGIC     
# MAGIC     -- Period Flags & Indicators
# MAGIC     is_weekend                      INT             NOT NULL    COMMENT 'Weekend flag (1=Yes, 0=No)',
# MAGIC     is_weekday                      INT             NOT NULL    COMMENT 'Weekday flag (1=Yes, 0=No)',
# MAGIC     is_holiday                      INT             NOT NULL    COMMENT 'US Federal holiday flag (1=Yes, 0=No)',
# MAGIC     holiday_name                    STRING                      COMMENT 'Name of holiday if applicable',
# MAGIC     is_business_day                 INT             NOT NULL    COMMENT 'Business day flag - weekday and not holiday (1=Yes, 0=No)',
# MAGIC     is_month_end                    INT             NOT NULL    COMMENT 'Last day of month flag (1=Yes, 0=No)',
# MAGIC     is_quarter_end                  INT             NOT NULL    COMMENT 'Last day of quarter flag (1=Yes, 0=No)',
# MAGIC     is_year_end                     INT             NOT NULL    COMMENT 'Last day of year flag (1=Yes, 0=No)',
# MAGIC     is_last_business_day_of_month   INT             NOT NULL    COMMENT 'Last business day of month flag (1=Yes, 0=No)',
# MAGIC     
# MAGIC     -- Relative Period Calculations
# MAGIC     days_in_month                   INT             NOT NULL    COMMENT 'Total days in the month',
# MAGIC     days_remaining_in_month         INT             NOT NULL    COMMENT 'Days remaining in month after this date',
# MAGIC     days_in_quarter                 INT             NOT NULL    COMMENT 'Total days in the quarter',
# MAGIC     days_remaining_in_quarter       INT             NOT NULL    COMMENT 'Days remaining in quarter after this date',
# MAGIC     prior_year_date                 TIMESTAMP       NOT NULL    COMMENT 'Same date in prior year (Eastern timezone)',
# MAGIC     prior_month_date                TIMESTAMP       NOT NULL    COMMENT 'Same date in prior month (Eastern timezone)',
# MAGIC     same_day_prior_year_key         INT             NOT NULL    COMMENT 'Date key for same date in prior year (YYYYMMDD)',
# MAGIC     
# MAGIC     -- Reporting Helpers
# MAGIC     ytd_flag                        INT             NOT NULL    COMMENT 'Year-to-date flag relative to current date (1=Yes, 0=No)',
# MAGIC     qtd_flag                        INT             NOT NULL    COMMENT 'Quarter-to-date flag relative to current date (1=Yes, 0=No)',
# MAGIC     mtd_flag                        INT             NOT NULL    COMMENT 'Month-to-date flag relative to current date (1=Yes, 0=No)',
# MAGIC     trading_day_of_month            INT             NOT NULL    COMMENT 'Business day count within month (0 if not business day)',
# MAGIC     trading_day_of_year             INT             NOT NULL    COMMENT 'Business day count within year (0 if not business day)',
# MAGIC     
# MAGIC     -- FRED Economic Indicator
# MAGIC     usrecd_recession_indicator      INT                         COMMENT 'NBER US Recession Indicator from FRED (1=Recession, 0=Expansion, NULL=No data)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Calendar dimension table with US holidays, business day flags, and FRED recession indicator. All timestamps in Eastern timezone (America/New_York).'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'domain' = 'reference',
# MAGIC     'timezone' = 'America/New_York'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'common';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN common LIKE 'reference';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED common.reference.dim_calendar;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Configure your FRED API key in Databricks Secrets:
# MAGIC    ```
# MAGIC    databricks secrets create-scope fred-api
# MAGIC    databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY"
# MAGIC    ```
# MAGIC 2. Run the `02_daily_refresh` notebook to load initial data
# MAGIC 3. Schedule `02_daily_refresh` as a daily job
