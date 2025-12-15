-- ============================================================
-- Validation Queries for Calendar Dimension Table
-- ============================================================

-- 1. Basic row count and date range
SELECT 
    COUNT(*) as total_rows,
    MIN(full_date) as min_date,
    MAX(full_date) as max_date,
    COUNT(DISTINCT year) as years_covered
FROM common.reference.dim_calendar;

-- 2. Validate no null values in required fields
SELECT 
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_date_keys,
    SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) as null_full_dates,
    SUM(CASE WHEN calendar_date IS NULL THEN 1 ELSE 0 END) as null_calendar_dates,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) as null_years,
    SUM(CASE WHEN month_number IS NULL THEN 1 ELSE 0 END) as null_months,
    SUM(CASE WHEN day_of_month IS NULL THEN 1 ELSE 0 END) as null_days
FROM common.reference.dim_calendar;

-- 3. Validate flag columns (should only be 0 or 1)
SELECT 
    'is_weekend' as column_name,
    MIN(is_weekend) as min_val,
    MAX(is_weekend) as max_val,
    COUNT(DISTINCT is_weekend) as distinct_values
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_weekday', MIN(is_weekday), MAX(is_weekday), COUNT(DISTINCT is_weekday) 
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_holiday', MIN(is_holiday), MAX(is_holiday), COUNT(DISTINCT is_holiday) 
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_business_day', MIN(is_business_day), MAX(is_business_day), COUNT(DISTINCT is_business_day) 
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_month_end', MIN(is_month_end), MAX(is_month_end), COUNT(DISTINCT is_month_end) 
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_quarter_end', MIN(is_quarter_end), MAX(is_quarter_end), COUNT(DISTINCT is_quarter_end) 
FROM common.reference.dim_calendar
UNION ALL
SELECT 'is_year_end', MIN(is_year_end), MAX(is_year_end), COUNT(DISTINCT is_year_end) 
FROM common.reference.dim_calendar;

-- 4. Validate business day logic (weekday AND not holiday)
SELECT COUNT(*) as invalid_business_days
FROM common.reference.dim_calendar
WHERE is_business_day = 1 AND (is_weekday = 0 OR is_holiday = 1);

-- 5. Validate weekend logic
SELECT COUNT(*) as invalid_weekends
FROM common.reference.dim_calendar
WHERE is_weekend = 1 AND day_of_week NOT IN (6, 7);

-- 6. Validate holidays exist
SELECT 
    year,
    COUNT(*) as holiday_count,
    SUM(CASE WHEN holiday_name LIKE '%Christmas%' THEN 1 ELSE 0 END) as christmas_count,
    SUM(CASE WHEN holiday_name LIKE '%Thanksgiving%' THEN 1 ELSE 0 END) as thanksgiving_count
FROM common.reference.dim_calendar
WHERE is_holiday = 1
GROUP BY year
ORDER BY year DESC
LIMIT 10;

-- 7. Check recession indicator data coverage
SELECT 
    MIN(full_date) as first_recession_data,
    MAX(full_date) as last_recession_data,
    COUNT(*) as rows_with_recession_data,
    SUM(usrecd_recession_indicator) as recession_months
FROM common.reference.dim_calendar
WHERE usrecd_recession_indicator IS NOT NULL;

-- 8. Validate YTD/QTD/MTD flags for current year
SELECT 
    SUM(ytd_flag) as ytd_days,
    SUM(qtd_flag) as qtd_days,
    SUM(mtd_flag) as mtd_days
FROM common.reference.dim_calendar
WHERE year = YEAR(CURRENT_DATE);

-- 9. Sample recent data
SELECT *
FROM common.reference.dim_calendar
WHERE full_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY full_date DESC;

-- 10. Validate last business day of month
SELECT 
    year,
    month_number,
    MAX(CASE WHEN is_last_business_day_of_month = 1 THEN calendar_date END) as last_business_day,
    MAX(calendar_date) as last_calendar_day
FROM common.reference.dim_calendar
WHERE year >= YEAR(CURRENT_DATE) - 1
GROUP BY year, month_number
ORDER BY year DESC, month_number DESC
LIMIT 12;
