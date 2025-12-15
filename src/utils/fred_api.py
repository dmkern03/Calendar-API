"""
FRED API Helper Functions

Provides functions to fetch economic data from the Federal Reserve Economic Data (FRED) API.
"""

import requests
from datetime import date
from typing import Dict, Optional


def fetch_fred_recession_data(
    api_key: str,
    start_date: date,
    end_date: date,
    series_id: str = "USRECD",
    timeout: int = 60
) -> Dict[str, int]:
    """
    Fetch recession indicator data from FRED API.
    
    Args:
        api_key: FRED API key
        start_date: Start date for observations
        end_date: End date for observations
        series_id: FRED series ID (default: USRECD for US Recession Indicator)
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary mapping date strings (YYYY-MM-DD) to indicator values (0 or 1)
        
    Raises:
        requests.RequestException: If API request fails
    """
    print(f"Fetching FRED {series_id} data...")
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.strftime("%Y-%m-%d"),
        "observation_end": end_date.strftime("%Y-%m-%d"),
    }
    
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        
        result = {}
        for obs in data.get("observations", []):
            obs_date = obs["date"]
            value = obs["value"]
            if value != "." and value is not None:
                result[obs_date] = int(float(value))
        
        print(f"Retrieved {len(result)} {series_id} observations from FRED")
        return result
    
    except requests.RequestException as e:
        print(f"Warning: Could not fetch FRED data: {e}")
        print("Proceeding without recession indicator data...")
        return {}


def get_fred_api_key(secret_scope: str = "fred-api", secret_key: str = "api-key") -> Optional[str]:
    """
    Retrieve FRED API key from Databricks secrets or environment variable.
    
    Args:
        secret_scope: Databricks secret scope name
        secret_key: Secret key name within the scope
        
    Returns:
        API key string or None if not found
    """
    import os
    
    try:
        # Try Databricks secrets first
        api_key = dbutils.secrets.get(scope=secret_scope, key=secret_key)
        print(f"✓ API key retrieved from secret scope: {secret_scope}")
        return api_key
    except NameError:
        # Fallback to environment variable
        api_key = os.environ.get("FRED_API_KEY")
        if api_key:
            print("✓ API key retrieved from environment variable")
        else:
            print("⚠ No FRED API key found")
        return api_key
