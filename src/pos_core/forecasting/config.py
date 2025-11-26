"""Configuration constants for forecasting pipeline."""

# Branch names
BRANCHES = ["Zambrano", "Punto Valle", "QIN", "Kavia", "CrediClub", "Carreta", "Nativa"]

# Payment metrics to forecast
METRICS = ["ingreso_efectivo", "ingreso_credito", "ingreso_debito", "ingreso_total"]

# Forecast horizon (number of days ahead)
FORECAST_DAYS = 7

# Seasonal period for time series models (7 = weekly seasonality)
SEASONAL_PERIOD = 7

