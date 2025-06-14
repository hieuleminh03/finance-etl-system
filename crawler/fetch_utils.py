#!/usr/bin/env python3
"""Stock data fetching utilities using yfinance."""

import os
import logging
import pandas as pd
import yfinance as yf
import datetime
import requests
import tempfile
import csv
from typing import Optional, Dict, Any, List, Union

# Setup logging
logger = logging.getLogger("fetch_utils")


def fetch_stock_history(ticker_symbol: str, period: str = "30d") -> Optional[pd.DataFrame]:
    try:
        logger.info(f"Fetching history data for {ticker_symbol} for period {period}")
        
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period=period, auto_adjust=False)
        
        if df.empty:
            logger.warning(f"No history data available for {ticker_symbol}")
            return None
            
        df.reset_index(inplace=True)
        df['ticker'] = ticker_symbol
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching history data for {ticker_symbol}: {e}")
        return None

def fetch_stock_actions(ticker_symbol: str) -> Optional[pd.DataFrame]:
    try:
        logger.info(f"Fetching actions data for {ticker_symbol}")
        
        ticker = yf.Ticker(ticker_symbol)
        df_actions = ticker.actions
        
        if df_actions is None or df_actions.empty:
            logger.warning(f"No actions data available for {ticker_symbol}")
            return None
            
        df_actions.reset_index(inplace=True)
        df_actions.rename(columns={"Date": "date"}, inplace=True)
        df_actions['ticker'] = ticker_symbol
        
        return df_actions
        
    except Exception as e:
        logger.error(f"Error fetching actions data for {ticker_symbol}: {e}")
        return None

def fetch_stock_info(ticker_symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetches company information for a specific stock ticker.
    """
    try:
        logger.info(f"Fetching company info for {ticker_symbol}")
        
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        
        if not info or 'symbol' not in info:
            logger.warning(f"No company info available for {ticker_symbol}")
            return None
            
        info['ticker'] = info.get('symbol', ticker_symbol)
        
        return info
        
    except Exception as e:
        logger.error(f"Error fetching company info for {ticker_symbol}: {e}")
        return None

def fetch_stock_financials(ticker_symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Fetches financial statements for a specific stock ticker.
    """
    try:
        logger.info(f"Fetching financials for {ticker_symbol}")
        
        ticker = yf.Ticker(ticker_symbol)
        
        financials = {
            'income_statement': ticker.income_stmt,
            'balance_sheet': ticker.balance_sheet,
            'cash_flow': ticker.cashflow
        }
        
        if all(df.empty for df in financials.values()):
            logger.warning(f"No financial data available for {ticker_symbol}")
            return None
            
        for name, df in financials.items():
            if not df.empty:
                df['ticker'] = ticker_symbol
        
        return financials
        
    except Exception as e:
        logger.error(f"Error fetching financials for {ticker_symbol}: {e}")
        return None

def download_file_from_google_drive(file_id: str, destination: str) -> bool:
    """
    Downloads a file from Google Drive, handling potential virus scan warnings.
    """
    URL = "https://docs.google.com/uc?export=download"
    
    try:
        with requests.Session() as session:
            response = session.get(URL, params={'id': file_id}, stream=True)
            response.raise_for_status()
            
            token = None
            for key, value in response.cookies.items():
                if key.startswith('download_warning'):
                    token = value
                    break
            
            if token:
                params = {'id': file_id, 'confirm': token}
                response = session.get(URL, params=params, stream=True)
                response.raise_for_status()

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded file to {destination}")
            return True
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading file from Google Drive: {e}")
        return False
    except Exception as e:
        logger.error(f"Error downloading file from Google Drive: {e}")
        return False

def load_stock_symbols_from_google_drive() -> List[str]:
    """
    Load stock symbols from a CSV file on Google Drive.
    """
    file_id = os.getenv('GOOGLE_DRIVE_FILE_ID')
    if not file_id:
        logger.error("GOOGLE_DRIVE_FILE_ID not found. Using default symbols.")
        return get_default_symbols()

    try:
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=True) as temp_file:
            if not download_file_from_google_drive(file_id, temp_file.name):
                logger.error("Failed to download symbols file. Using default symbols.")
                return get_default_symbols()

            df = pd.read_csv(temp_file.name)
            if 'Symbol' not in df.columns:
                logger.error("'Symbol' column not found. Using default symbols.")
                return get_default_symbols()

            symbols = df['Symbol'].dropna().astype(str).tolist()
            symbols = [s.strip() for s in symbols if s.strip()]
            
            logger.info(f"Loaded {len(symbols)} symbols from Google Drive.")
            return symbols

    except Exception as e:
        logger.error(f"Error loading symbols from Google Drive: {e}")
        return get_default_symbols()

def get_default_symbols() -> List[str]:
    """
    Get default stock symbols as fallback
    
    Returns:
        List[str]: List of default stock symbols
    """
    default_symbols = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM']
    logger.info(f"Using default symbols: {default_symbols}")
    return default_symbols

def load_stock_symbols() -> List[str]:
    """
    Load stock symbols from Google Drive.
    """
    return load_stock_symbols_from_google_drive()
