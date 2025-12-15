# Data Dictionary: dim_calendar

## Overview

The `common.reference.dim_calendar` table is a comprehensive calendar dimension table designed for finance and business intelligence use cases. All timestamp fields are aligned to Eastern Time (America/New_York).

## Table Details

| Property | Value |
|----------|-------|
| Catalog | common |
| Schema | reference |
| Table | dim_calendar |
| Format | Delta |
| Timezone | America/New_York (Eastern Time) |

---

## Column Definitions

### Date Keys & Identifiers

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date_key` | INT | No | Date key in YYYYMMDD format for efficient joins (e.g., 20241225) |
| `full_date` | TIMESTAMP | No | Full date value as timestamp in Eastern timezone |
| `calendar_date` | DATE | No | Calendar date as DATE type |
| `date_string` | STRING | No | Formatted date string in MM/DD/YYYY format |

### Standard Calendar Components

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `day_of_week` | INT | No | Day of week where 1=Monday, 7=Sunday |
| `day_name` | STRING | No | Full day name (Monday, Tuesday, etc.) |
| `day_abbrev` | STRING | No | Abbreviated day name (Mon, Tue, etc.) |
| `day_of_month` | INT | No | Day of month (1-31) |
| `day_of_year` | INT | No | Day of year (1-366) |
| `week_of_year` | INT | No | ISO week of year (1-53) |
| `week_of_month` | INT | No | Week of month (1-5) |
| `month_number` | INT | No | Month number (1-12) |
| `month_name` | STRING | No | Full month name (January, February, etc.) |
| `month_abbrev` | STRING | No | Abbreviated month name (Jan, Feb, etc.) |
| `quarter` | INT | No | Quarter number (1-4) |
| `quarter_name` | STRING | No | Quarter name (Q1, Q2, Q3, Q4) |
| `year` | INT | No | Calendar year |
| `year_month` | INT | No | Year and month in YYYYMM format (e.g., 202412) |

### Period Flags & Indicators

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `is_weekend` | INT | No | Weekend flag: 1=Saturday or Sunday, 0=Weekday |
| `is_weekday` | INT | No | Weekday flag: 1=Monday-Friday, 0=Weekend |
| `is_holiday` | INT | No | US Federal holiday flag: 1=Holiday, 0=Not holiday |
| `holiday_name` | STRING | Yes | Name of holiday if applicable (NULL if not a holiday) |
| `is_business_day` | INT | No | Business day flag: 1=Weekday AND not holiday, 0=Otherwise |
| `is_month_end` | INT | No | Last day of month flag: 1=Yes, 0=No |
| `is_quarter_end` | INT | No | Last day of quarter flag: 1=Yes, 0=No |
| `is_year_end` | INT | No | Last day of year flag: 1=Yes, 0=No |
| `is_last_business_day_of_month` | INT | No | Last business day of month flag: 1=Yes, 0=No |

### Relative Period Calculations

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `days_in_month` | INT | No | Total days in the month (28-31) |
| `days_remaining_in_month` | INT | No | Days remaining in month after this date |
| `days_in_quarter` | INT | No | Total days in the quarter (90-92) |
| `days_remaining_in_quarter` | INT | No | Days remaining in quarter after this date |
| `prior_year_date` | TIMESTAMP | No | Same date in prior year (Eastern timezone) |
| `prior_month_date` | TIMESTAMP | No | Same date in prior month (Eastern timezone) |
| `same_day_prior_year_key` | INT | No | Date key for same date in prior year (YYYYMMDD) |

### Reporting Helpers

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ytd_flag` | INT | No | Year-to-date flag: 1=On or before current date in current year, 0=Otherwise |
| `qtd_flag` | INT | No | Quarter-to-date flag: 1=On or before current date in current quarter, 0=Otherwise |
| `mtd_flag` | INT | No | Month-to-date flag: 1=On or before current date in current month, 0=Otherwise |
| `trading_day_of_month` | INT | No | Business day count within month (0 if not a business day) |
| `trading_day_of_year` | INT | No | Business day count within year (0 if not a business day) |

### FRED Economic Indicator

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `usrecd_recession_indicator` | INT | Yes | NBER US Recession Indicator from FRED: 1=Recession, 0=Expansion, NULL=No data available |

---

## US Federal Holidays Included

The following US federal holidays are recognized (including observed dates when holidays fall on weekends):

- New Year's Day (January 1)
- Martin Luther King Jr. Day (3rd Monday of January, since 1986)
- Presidents Day (3rd Monday of February)
- Memorial Day (Last Monday of May)
- Juneteenth (June 19, since 2021)
- Independence Day (July 4)
- Labor Day (1st Monday of September)
- Columbus Day (2nd Monday of October)
- Veterans Day (November 11)
- Thanksgiving (4th Thursday of November)
- Christmas Day (December 25)

---

## Usage Examples

### Get all business days in current month
```sql
SELECT *
FROM common.reference.dim_calendar
WHERE mtd_flag = 1
  AND is_business_day = 1
ORDER BY calendar_date;
```

### Get last business day of each month for 2024
```sql
SELECT calendar_date, month_name
FROM common.reference.dim_calendar
WHERE year = 2024
  AND is_last_business_day_of_month = 1
ORDER BY calendar_date;
```

### Join with fact table for YTD analysis
```sql
SELECT 
    c.month_name,
    SUM(f.revenue) as monthly_revenue
FROM fact_sales f
JOIN common.reference.dim_calendar c
    ON f.date_key = c.date_key
WHERE c.ytd_flag = 1
GROUP BY c.month_name, c.month_number
ORDER BY c.month_number;
```

### Find recession periods
```sql
SELECT 
    MIN(calendar_date) as recession_start,
    MAX(calendar_date) as recession_end,
    COUNT(*) as days_in_recession
FROM common.reference.dim_calendar
WHERE usrecd_recession_indicator = 1
GROUP BY year
ORDER BY year;
```
