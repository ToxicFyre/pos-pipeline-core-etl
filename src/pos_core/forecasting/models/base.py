"""Base model interface for forecasting models.

This module defines the abstract base class that all forecasting models must implement,
enabling a consistent interface for different model types (ARIMA, Prophet, LSTM, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class ForecastModel(ABC):
    """Abstract base class for forecasting models.
    
    All forecasting models must implement the train() and forecast() methods
    to provide a consistent interface for the forecasting pipeline.
    """
    
    @abstractmethod
    def train(self, series: pd.Series, **kwargs) -> object:
        """Train the forecasting model on a time series.
        
        Args:
            series: Time series with DateTimeIndex (raw values, not transformed)
            **kwargs: Model-specific hyperparameters
            
        Returns:
            Trained model object (type depends on implementation)
            
        Raises:
            ValueError: If training fails (e.g., insufficient data)
        """
        pass
    
    @abstractmethod
    def forecast(self, model: object, steps: int, **kwargs) -> pd.Series:
        """Generate forecast from a trained model.
        
        Args:
            model: Trained model object (from train() method)
            steps: Number of periods to forecast ahead
            **kwargs: Model-specific forecast parameters
            
        Returns:
            Forecast series with DateTimeIndex, back-transformed to original scale
        """
        pass

