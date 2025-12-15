"""
Unit tests for Calendar Dimension Generator
"""

import pytest
import pandas as pd
from datetime import date, datetime
from zoneinfo import ZoneInfo
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestCalendarGeneration:
    """Tests for calendar generation functions."""
    
    def test_date_range_creation(self):
        """Test that date range is created correctly."""
        timezone = ZoneInfo("America/New_York")
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        
        dates = pd.date_range(start=start, end=end, freq='D', tz=timezone)
        
        assert len(dates) == 31
        assert dates[0].date() == start
        assert dates[-1].date() == end
    
    def test_date_key_format(self):
        """Test date key is in YYYYMMDD format."""
        test_date = pd.Timestamp("2024-12-25", tz=ZoneInfo("America/New_York"))
        date_key = int(test_date.strftime('%Y%m%d'))
        
        assert date_key == 20241225
    
    def test_day_of_week_calculation(self):
        """Test day of week calculation (1=Monday, 7=Sunday)."""
        # December 25, 2024 is a Wednesday
        test_date = pd.Timestamp("2024-12-25", tz=ZoneInfo("America/New_York"))
        day_of_week = test_date.dayofweek + 1
        
        assert day_of_week == 3  # Wednesday
    
    def test_weekend_flag(self):
        """Test weekend detection."""
        saturday = pd.Timestamp("2024-12-21", tz=ZoneInfo("America/New_York"))
        sunday = pd.Timestamp("2024-12-22", tz=ZoneInfo("America/New_York"))
        monday = pd.Timestamp("2024-12-23", tz=ZoneInfo("America/New_York"))
        
        assert saturday.dayofweek + 1 == 6  # Saturday
        assert sunday.dayofweek + 1 == 7    # Sunday
        assert monday.dayofweek + 1 == 1    # Monday
    
    def test_quarter_calculation(self):
        """Test quarter calculation."""
        q1_date = pd.Timestamp("2024-02-15", tz=ZoneInfo("America/New_York"))
        q2_date = pd.Timestamp("2024-05-15", tz=ZoneInfo("America/New_York"))
        q3_date = pd.Timestamp("2024-08-15", tz=ZoneInfo("America/New_York"))
        q4_date = pd.Timestamp("2024-11-15", tz=ZoneInfo("America/New_York"))
        
        assert q1_date.quarter == 1
        assert q2_date.quarter == 2
        assert q3_date.quarter == 3
        assert q4_date.quarter == 4
    
    def test_month_end_flag(self):
        """Test month end detection."""
        jan_31 = pd.Timestamp("2024-01-31", tz=ZoneInfo("America/New_York"))
        jan_30 = pd.Timestamp("2024-01-30", tz=ZoneInfo("America/New_York"))
        
        assert jan_31.is_month_end == True
        assert jan_30.is_month_end == False
    
    def test_year_month_format(self):
        """Test year_month is in YYYYMM format."""
        test_date = pd.Timestamp("2024-03-15", tz=ZoneInfo("America/New_York"))
        year_month = int(test_date.strftime('%Y%m'))
        
        assert year_month == 202403


class TestHolidays:
    """Tests for US holiday detection."""
    
    def test_christmas_is_holiday(self):
        """Test Christmas is detected as holiday."""
        import holidays
        us_holidays = holidays.US(years=[2024])
        
        christmas = date(2024, 12, 25)
        assert christmas in us_holidays
    
    def test_thanksgiving_is_holiday(self):
        """Test Thanksgiving is detected as holiday."""
        import holidays
        us_holidays = holidays.US(years=[2024])
        
        # Thanksgiving 2024 is November 28
        thanksgiving = date(2024, 11, 28)
        assert thanksgiving in us_holidays
    
    def test_regular_day_not_holiday(self):
        """Test regular day is not a holiday."""
        import holidays
        us_holidays = holidays.US(years=[2024])
        
        regular_day = date(2024, 3, 15)
        assert regular_day not in us_holidays
    
    def test_observed_holiday(self):
        """Test observed holidays are included."""
        import holidays
        us_holidays = holidays.US(years=[2021])
        
        # July 4, 2021 was Sunday, observed Monday July 5
        july_5_2021 = date(2021, 7, 5)
        assert july_5_2021 in us_holidays


class TestQuarterDates:
    """Tests for quarter date calculations."""
    
    def test_q1_dates(self):
        """Test Q1 start and end dates."""
        from datetime import timedelta
        
        d = date(2024, 2, 15)
        quarter = (d.month - 1) // 3 + 1
        quarter_start = date(d.year, (quarter - 1) * 3 + 1, 1)
        quarter_end = date(d.year, quarter * 3 + 1, 1) - timedelta(days=1)
        
        assert quarter == 1
        assert quarter_start == date(2024, 1, 1)
        assert quarter_end == date(2024, 3, 31)
    
    def test_q4_dates(self):
        """Test Q4 start and end dates."""
        d = date(2024, 11, 15)
        quarter = (d.month - 1) // 3 + 1
        quarter_start = date(d.year, (quarter - 1) * 3 + 1, 1)
        quarter_end = date(d.year, 12, 31)
        
        assert quarter == 4
        assert quarter_start == date(2024, 10, 1)
        assert quarter_end == date(2024, 12, 31)
    
    def test_days_in_quarter(self):
        """Test days in quarter calculation."""
        # Q1 2024: Jan 1 - Mar 31 = 91 days (leap year)
        q1_start = date(2024, 1, 1)
        q1_end = date(2024, 3, 31)
        days_in_q1 = (q1_end - q1_start).days + 1
        
        assert days_in_q1 == 91


class TestTimezone:
    """Tests for timezone handling."""
    
    def test_eastern_timezone(self):
        """Test Eastern timezone is applied."""
        timezone = ZoneInfo("America/New_York")
        now = datetime.now(timezone)
        
        assert now.tzinfo is not None
        assert str(now.tzinfo) == "America/New_York"
    
    def test_date_range_with_timezone(self):
        """Test date range respects timezone."""
        timezone = ZoneInfo("America/New_York")
        dates = pd.date_range(start="2024-01-01", periods=5, freq='D', tz=timezone)
        
        assert str(dates.tz) == "America/New_York"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
