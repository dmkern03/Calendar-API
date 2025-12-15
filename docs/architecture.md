# Architecture: Calendar Dimension Table

## Overview

This document describes the architecture and design decisions for the calendar dimension table solution.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Databricks Workspace                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────┐ │
│  │   Workflow   │────▶│   Notebook   │────▶│     Delta Table              │ │
│  │  (Scheduled) │     │  02_daily_   │     │  common.reference.           │ │
│  │   6 AM ET    │     │   refresh    │     │     dim_calendar             │ │
│  └──────────────┘     └──────┬───────┘     └──────────────────────────────┘ │
│                              │                           ▲                   │
│                              │                           │                   │
│                              ▼                           │                   │
│  ┌──────────────────────────────────────┐               │                   │
│  │         Python Generation            │               │                   │
│  │  ┌─────────────┐  ┌───────────────┐ │               │                   │
│  │  │   pandas    │  │   holidays    │ │               │                   │
│  │  │  DataFrame  │  │   library     │ │               │                   │
│  │  └─────────────┘  └───────────────┘ │               │                   │
│  └──────────────────────────────────────┘               │                   │
│                              │                           │                   │
│                              ▼                           │                   │
│  ┌──────────────────────────────────────┐               │                   │
│  │         Spark DataFrame              │───── MERGE ───┘                   │
│  │      (staging temp view)             │                                    │
│  └──────────────────────────────────────┘                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ API Call
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FRED API                                        │
│                    (Federal Reserve Economic Data)                           │
│                         USRECD Series                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Databricks Workflow

**Purpose**: Orchestrates daily execution of the calendar refresh.

**Schedule**: Daily at 6:00 AM Eastern Time

**Configuration**:
- Cluster: Shared job cluster or serverless
- Timeout: 30 minutes
- Retries: 2 with 5-minute intervals
- Alerts: Email on failure

### 2. Notebooks

#### 01_setup.py
- One-time execution
- Creates catalog, schema, and table
- Installs dependencies

#### 02_daily_refresh.py
- Daily execution
- Generates calendar data
- Merges into target table
- Optimizes table

### 3. Python Generation Layer

**Libraries**:
- `pandas`: DataFrame operations and date calculations
- `holidays`: US federal holiday detection with observed dates
- `requests`: FRED API integration
- `zoneinfo`: Timezone handling

**Key Functions**:
- `generate_calendar_dimension()`: Main generation logic
- `fetch_fred_recession_data()`: FRED API integration
- `localize_to_eastern()`: Timezone conversion

### 4. Delta Table

**Location**: `common.reference.dim_calendar`

**Format**: Delta Lake

**Optimizations**:
- Auto-optimize writes enabled
- Auto-compaction enabled
- Z-ORDER by `year`, `month_number`, `date_key`

### 5. External Data Sources

#### FRED API
- **Series**: USRECD (US Recession Indicator)
- **Authentication**: API key stored in Databricks Secrets
- **Frequency**: Monthly data
- **Fallback**: Graceful degradation if API unavailable

## Data Flow

```
1. Workflow triggers at 6 AM Eastern
         │
         ▼
2. Notebook installs dependencies
         │
         ▼
3. Python generates full calendar DataFrame
   ├── Date range: 1854-12-01 to current date
   ├── Holiday detection via `holidays` library
   └── FRED API call for recession data
         │
         ▼
4. Convert pandas DataFrame to Spark DataFrame
         │
         ▼
5. Create temporary staging view
         │
         ▼
6. MERGE into target Delta table
   ├── Match on date_key
   ├── Update existing rows (YTD/MTD/QTD flags change daily)
   └── Insert new rows (new dates)
         │
         ▼
7. OPTIMIZE table with Z-ORDER
         │
         ▼
8. Validate and log results
```

## Design Decisions

### Why MERGE instead of OVERWRITE?

1. **Preserves table metadata**: Column comments and table properties are retained
2. **Efficient updates**: Only changed rows are updated (YTD/MTD/QTD flags)
3. **Audit trail**: Delta time travel remains intact
4. **Reduced I/O**: Less data written compared to full overwrite

### Why pandas for generation?

1. **Simplicity**: Date calculations are more intuitive in pandas
2. **holidays library**: Integrates seamlessly with pandas
3. **Performance**: 62K rows is well within pandas capacity
4. **Flexibility**: Easy to add new calculated fields

### Why Eastern Timezone?

1. **Finance focus**: US markets operate on Eastern Time
2. **Consistency**: All date/time fields use same timezone
3. **Business alignment**: Matches typical US business operations

### Why Delta Lake?

1. **ACID transactions**: Ensures data consistency
2. **Time travel**: Ability to query historical versions
3. **Optimization**: Z-ORDER improves query performance
4. **Unity Catalog**: Full governance integration

## Security

### Secrets Management

```
┌─────────────────────────────────────┐
│     Databricks Secrets             │
│  ┌─────────────────────────────┐   │
│  │  Scope: fred-api            │   │
│  │  Key: api-key               │   │
│  │  Value: [FRED API Key]      │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

### Access Control

- Table owned by service principal
- Read access granted to analysts/reports
- Write access limited to ETL service principal

## Monitoring

### Success Metrics
- Row count matches expected date range
- No null values in required fields
- FRED data coverage percentage

### Alerts
- Job failure notification
- FRED API unavailability warning
- Data validation failures

## Scalability

### Current State
- ~62,000 rows (1854-2025)
- ~170 years of data
- Sub-minute execution time

### Growth Projection
- +365 rows per year
- Minimal performance impact
- No architectural changes needed for 100+ years

## Disaster Recovery

### Backup Strategy
- Delta time travel (30 days default)
- Can recreate from scratch (deterministic generation)

### Recovery Steps
1. Check Delta history for valid version
2. If corrupted, truncate and regenerate
3. Run 02_daily_refresh notebook

## Future Enhancements

1. **Additional holiday calendars**: UK, EU, APAC markets
2. **Fiscal calendar support**: Configurable fiscal year start
3. **Additional FRED series**: Interest rates, unemployment
4. **Streaming updates**: Real-time flag updates
