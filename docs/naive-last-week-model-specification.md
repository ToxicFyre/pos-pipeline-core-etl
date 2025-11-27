# Naive Last Week Forecast Model Specification

## Overview

This document specifies the implementation of the "Naive Last Week" forecasting model for the POS Core ETL forecasting module. This model provides a simple baseline forecasting approach that uses historical values from the same weekday in previous weeks, avoiding holidays and days adjacent to holidays.

## Version

- **Implementation Date**: November 2024
- **Model Version**: 1.0.0
- **Compatible with**: pos-core-etl >= 0.1.0

## Purpose

The Naive Last Week model serves as:
1. A baseline forecasting model for comparison with more sophisticated models (e.g., ARIMA)
2. A simple, interpretable forecasting method that leverages weekly seasonality
3. A fallback option when complex models fail or when historical patterns are stable

## Model Description

### Core Concept

For each forecast date, the model:
1. Identifies the weekday (Monday=0, Tuesday=1, ..., Sunday=6)
2. Looks back through historical data (up to N weeks, default: 10)
3. Finds the most recent historical date with the same weekday that:
   - Is not a national holiday
   - Is not adjacent to a national holiday (day before or after)
4. Uses the value from that historical date as the forecast

### Algorithm

```
For each forecast date F:
  1. Get weekday W of F
  2. For week_offset in [1, 2, ..., max_weeks_back]:
      candidate_date = F - (week_offset * 7 days)
      If candidate_date.weekday() == W:
         If candidate_date is not holiday and not adjacent to holiday:
            If candidate_date exists in training series:
               Return value from candidate_date
  3. If no suitable date found, return 0.0
```

## Implementation Details

### File Structure

```
src/pos_core/forecasting/
├── models/
│   ├── __init__.py          # Exports NaiveLastWeekModel
│   ├── base.py              # ForecastModel base class
│   ├── arima.py             # Existing ARIMA model
│   └── naive.py             # NEW: Naive Last Week model
├── api.py                   # MODIFIED: Added model_type support
└── pipeline.py             # MODIFIED: Added --model CLI argument
```

### 1. Naive Model Implementation (`models/naive.py`)

#### Class: `NaiveLastWeekModel`

**Inheritance**: Inherits from `ForecastModel` (abstract base class)

**Initialization**:
```python
NaiveLastWeekModel(max_weeks_back: int = 10)
```

**Parameters**:
- `max_weeks_back` (int, default=10): Maximum number of weeks to look back when finding equivalent historical weekdays

**Attributes**:
- `max_weeks_back` (int): Maximum weeks to look back
- `training_series` (pd.Series | None): Stored training time series
- `holidays` (set[date]): Set of holiday dates to avoid

#### Method: `train(series, **kwargs)`

**Signature**:
```python
def train(self, series: pd.Series, **kwargs: Any) -> Any
```

**Parameters**:
- `series` (pd.Series): Time series with DateTimeIndex (raw values)
- `**kwargs`: Additional parameters
  - `holidays` (set[date], optional): Set of holiday dates to avoid

**Behavior**:
1. Stores a copy of the training series
2. Extracts and stores holidays from kwargs (if provided)
3. Returns self (for consistency with ForecastModel interface)

**Returns**: The model instance itself

#### Method: `forecast(model, steps, **kwargs)`

**Signature**:
```python
def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series
```

**Parameters**:
- `model` (Any): Trained model (should be self)
- `steps` (int): Number of periods to forecast ahead
- `**kwargs`: Additional parameters
  - `last_date` (pd.Timestamp, optional): Last date of training series for index creation

**Behavior**:
1. Validates that model has been trained
2. Gets last date from kwargs or training series
3. For each forecast step:
   - Generates next forecast date
   - Finds equivalent historical weekday using `find_equivalent_historical_weekday()`
   - Retrieves value from historical date (or 0.0 if not found)
4. Creates forecast series with DateTimeIndex

**Returns**: `pd.Series` with DateTimeIndex containing forecast values

**Raises**: `ValueError` if model has not been trained

#### Helper Functions

##### `is_weekday(d: date) -> bool`

Checks if a date is a weekday (Monday-Friday).

**Parameters**:
- `d` (date): Date to check

**Returns**: `True` if date is Monday-Friday (weekday < 5)

##### `find_equivalent_historical_weekday(target_date, series, holidays, max_weeks_back) -> date | None`

Finds the most recent historical date with the same weekday, avoiding holidays.

**Parameters**:
- `target_date` (date): Target date to find equivalent for
- `series` (pd.Series): Historical time series with DateTimeIndex
- `holidays` (set[date]): Set of holiday dates to avoid
- `max_weeks_back` (int, default=10): Maximum weeks to look back

**Returns**: Equivalent historical date, or `None` if not found

**Algorithm**:
1. Get target weekday
2. Convert series index to dates (handles both Timestamp and date indices)
3. For each week offset from 1 to max_weeks_back:
   - Calculate candidate_date = target_date - (week_offset * 7 days)
   - Check if candidate has same weekday
   - Check if candidate is not holiday or adjacent to holiday
   - Check if candidate exists in series
   - If all checks pass, return candidate_date
4. Return None if no suitable date found

### 2. API Updates (`api.py`)

#### ForecastConfig Dataclass

**New Field**:
```python
model_type: str = "arima"  # Options: "arima", "naive"
```

**Description**: Specifies which forecasting model to use. Default is "arima" for backward compatibility.

#### run_payments_forecast Function

**New Behavior**:

1. **Holiday Extraction**:
   ```python
   holidays: set[date] = set()
   if "is_national_holiday" in df.columns:
       holiday_dates = df[df["is_national_holiday"] == True]["fecha"].dt.date.unique()
       holidays = set(holiday_dates)
   ```

2. **Model Selection**:
   ```python
   if config.model_type == "naive":
       model = NaiveLastWeekModel()
   elif config.model_type == "arima":
       model = LogARIMAModel()
   else:
       raise ValueError(f"Unknown model_type: {config.model_type}")
   ```

3. **Training with Holidays**:
   ```python
   if config.model_type == "naive":
       trained_model = model.train(series, holidays=holidays)
   else:
       trained_model = model.train(series)
   ```

### 3. CLI Updates (`pipeline.py`)

#### New Command-Line Argument

```python
parser.add_argument(
    "--model",
    type=str,
    default="arima",
    choices=["arima", "naive"],
    help="Forecast model to use (default: arima). Options: arima, naive",
)
```

#### Usage

```bash
# Use ARIMA model (default)
python -m pos_core.forecasting.pipeline --file data.csv --horizon 7

# Use Naive Last Week model
python -m pos_core.forecasting.pipeline --file data.csv --horizon 7 --model naive
```

### 4. Module Exports (`models/__init__.py`)

**New Export**:
```python
from pos_core.forecasting.models.naive import NaiveLastWeekModel

__all__ = ["ForecastModel", "LogARIMAModel", "NaiveLastWeekModel"]
```

## Dependencies

### External Dependencies
- `pandas`: For time series handling
- `datetime`: For date operations

### Internal Dependencies
- `pos_core.forecasting.models.base.ForecastModel`: Base class
- `pos_core.forecasting.deposit_schedule.is_holiday_or_adjacent`: Holiday checking function

## Data Requirements

### Input Data

The model requires:
1. **Training Series**: `pd.Series` with `DateTimeIndex` containing historical values
2. **Holidays** (optional): `set[date]` containing national holiday dates

### Holiday Data Source

Holidays are automatically extracted from the `payments_df` DataFrame if it contains an `is_national_holiday` column:

```python
if "is_national_holiday" in df.columns:
    holiday_dates = df[df["is_national_holiday"] == True]["fecha"].dt.date.unique()
    holidays = set(holiday_dates)
```

This column is typically generated by the ETL pipeline in `aggregate_payments_by_day.py`.

## Usage Examples

### Programmatic Usage

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast
import pandas as pd

# Load payments data
payments_df = pd.read_csv("aggregated_payments_daily.csv")
payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])

# Configure for naive model
config = ForecastConfig(
    horizon_days=7,
    model_type="naive"  # Use naive model
)

# Run forecast
result = run_payments_forecast(payments_df, config=config)

# Access results
print(result.forecast)
print(result.deposit_schedule)
```

### Direct Model Usage

```python
from pos_core.forecasting.models.naive import NaiveLastWeekModel
import pandas as pd
from datetime import date

# Create model
model = NaiveLastWeekModel(max_weeks_back=10)

# Prepare training data
series = pd.Series(
    [100, 120, 110, 130, 140, 50, 60],  # Example values
    index=pd.date_range("2024-01-01", periods=7, freq="D")
)

# Define holidays
holidays = {date(2024, 1, 1)}  # New Year's Day

# Train model
trained_model = model.train(series, holidays=holidays)

# Generate forecast
forecast = model.forecast(trained_model, steps=7, last_date=series.index[-1])
print(forecast)
```

## Behavior and Edge Cases

### Holiday Handling

1. **Holiday Avoidance**: The model skips dates that are holidays
2. **Adjacent Day Avoidance**: The model also skips dates adjacent to holidays (day before or after)
3. **Holiday Function**: Uses `is_holiday_or_adjacent()` from `deposit_schedule` module

### Missing Data Handling

1. **No Equivalent Date Found**: Returns 0.0 as fallback
2. **Date Not in Series**: Returns 0.0 as fallback
3. **Empty Training Series**: Raises `ValueError` during forecast

### Index Type Handling

The model handles both:
- `DatetimeIndex` with `pd.Timestamp` objects
- Date index with `date` objects

### Weekday Matching

- Monday matches Monday (weekday 0)
- Tuesday matches Tuesday (weekday 1)
- ... and so on
- Weekend days (Saturday=5, Sunday=6) are also matched

## Performance Characteristics

### Time Complexity

- **Training**: O(1) - Just stores series and holidays
- **Forecasting**: O(steps × max_weeks_back) - For each forecast step, checks up to max_weeks_back weeks

### Space Complexity

- **Training**: O(n) where n is the length of the training series
- **Forecasting**: O(steps) for the output series

### Typical Performance

- Very fast training (microseconds)
- Fast forecasting (milliseconds for typical horizons)
- No external dependencies or heavy computations

## Limitations

1. **No Trend Handling**: Does not account for trends or long-term changes
2. **No Seasonality Beyond Weekly**: Only captures weekly patterns, not monthly/yearly
3. **Simple Fallback**: Returns 0.0 when no equivalent date is found
4. **Fixed Lookback Window**: Limited to max_weeks_back weeks of history
5. **No Uncertainty Quantification**: Does not provide confidence intervals

## Testing

### Unit Tests

Recommended test cases:
1. Test `is_weekday()` with various dates
2. Test `find_equivalent_historical_weekday()` with:
   - Normal dates
   - Holidays
   - Dates adjacent to holidays
   - Missing data scenarios
3. Test `NaiveLastWeekModel.train()` with and without holidays
4. Test `NaiveLastWeekModel.forecast()` with various scenarios
5. Test integration with `run_payments_forecast()`

### Integration Tests

1. Test CLI with `--model naive` argument
2. Test end-to-end forecast pipeline with naive model
3. Test holiday extraction from payments_df

## Migration and Compatibility

### Backward Compatibility

- **Default Behavior**: `model_type="arima"` maintains existing behavior
- **API Compatibility**: All existing code continues to work without changes
- **CLI Compatibility**: Existing scripts work without modification

### Migration Path

To migrate to naive model:
1. Update `ForecastConfig` to include `model_type="naive"`
2. Or use CLI argument `--model naive`
3. Ensure `is_national_holiday` column exists in payments_df for holiday handling

## Future Enhancements

Potential improvements:
1. **Weighted Averaging**: Average values from multiple equivalent dates
2. **Trend Adjustment**: Apply simple trend correction
3. **Confidence Intervals**: Provide uncertainty estimates
4. **Adaptive Lookback**: Adjust max_weeks_back based on data availability
5. **Multiple Seasonality**: Handle monthly/yearly patterns

## References

- **Base Model Interface**: `src/pos_core/forecasting/models/base.py`
- **Holiday Function**: `src/pos_core/forecasting/deposit_schedule.py`
- **API Integration**: `src/pos_core/forecasting/api.py`
- **CLI Integration**: `src/pos_core/forecasting/pipeline.py`

## Changelog

### Version 1.0.0 (November 2024)
- Initial implementation
- Basic naive last week forecasting
- Holiday avoidance
- Integration with forecasting API and CLI
