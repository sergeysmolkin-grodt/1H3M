import pandas as pd
from datetime import time, datetime, timedelta
import pytz # For timezone handling
import time as sleep_timer # Import the standard time module and alias it to avoid conflict
import argparse # For command-line arguments
import matplotlib.pyplot as plt
import mplfinance as mpf
import os # For creating directories

# --- Bot Configuration & Parameters ---
SYMBOL_TO_TRADE = "EUR/USD" # Default, can be overridden in main
STOP_LOSS_BUFFER_PIPS = 1.0 # Adjusted from C# 0.3 for M5, needs tuning. Was 5 for H1 SL rule. Let's use a small practical value.
MIN_SL_PIPS = 5.0           # Minimum stop loss in pips
MIN_RR = 1.3                # Minimum Risk/Reward ratio
MAX_RR = 8.0                # Maximum Risk/Reward ratio (Increased from 5.0)
MAX_BOS_DISTANCE_PIPS = 15.0 # Maximum distance for BOS confirmation in pips
H1_FRACTAL_PERIOD = 3 # Period for H1 fractal identification (N bars on each side, e.g., 3 means center of 7 bars for TP)
ASIA_H1_FRACTAL_PERIOD = 1 # For 3-bar Asian session H1 fractals (1 bar on each side)

PIP_SIZE_DEFAULT = 0.0001     # For EURUSD like pairs
PIP_SIZE_JPY = 0.01         # For JPY pairs

H1_DATA_PRELOAD_DAYS = 4    # Number of extra days of H1 data to fetch before the backtest start_date for trend calculation

# --- Account and Risk Parameters (NEW) ---
INITIAL_ACCOUNT_BALANCE = 10000.0 # Example initial account balance
RISK_PERCENT = 1.0                # Risk 1% of account balance per trade

# --- Symbol Specific Parameters (Placeholders - to be refined or made dynamic) ---
# These would typically come from broker API or detailed symbol specification
PIP_VALUE_PER_LOT_STD_PAIR = 10.0 # For a standard lot (100,000 units) of EURUSD, USD is quote, so pip value is $10
MIN_LOT_SIZE_STD = 0.01           # Minimum volume in lots for standard pairs
LOT_STEP_STD = 0.01               # Lot step for standard pairs
MAX_LOT_SIZE_STD = 100.0          # Maximum volume in lots
# For JPY pairs, pip value might be different, e.g. if account currency is USD and trading USDJPY
# For simplicity, we might assume account currency is the quote currency of the pair, or USD.
# This part needs careful consideration for a multi-currency backtester.

# --- Enums / Constants ---
class TrendContext:
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"

# Global state variables (аналогично вашим переменным в C#)
# asia_high = None # Removed as unused
# asia_low = None # Removed as unused
asia_high_time = None # Will store time of the identified Asia High Fractal
asia_low_time = None # Will store time of the identified Asia Low Fractal

fractal_level_asia_high = None
fractal_level_asia_low = None

sweep_terjadi_high = False
sweep_terjadi_low = False
sweep_bar_actual_high = None # Store the actual bar object (Pandas Series)
sweep_bar_actual_low = None  # Store the actual bar object (Pandas Series)

bos_level_to_break_high = None
bos_level_to_break_low = None

last_processed_h1_bar_time = None
last_processed_m5_bar_time = None

# --- Global State Tracking ---
last_trade_execution_date = None # Tracks the date of the last executed trade to allow one trade per day
executed_trades_list = [] # List to store details of all simulated trades

# --- Session Times (UTC) ---
# Asia Session (примерно 00:00 - 06:00 UTC, но фракталы ищем до 05:00 UTC H1 свечи)
ASIA_START_HOUR_UTC = 0
ASIA_END_HOUR_UTC = 6 # Сессия длится до этого часа, но фракталы по свечам ДО этого часа. This might be just a comment for general Asia session.
ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE = 9 # H1 свечи *до* этого часа (т.е. 00,01,..08) используются для поиска фрактала

# Frankfurt Session (06:00 - 07:00 UTC для "первого часа")
FRANKFURT_SESSION_START_HOUR_UTC = 6
FRANKFURT_SESSION_END_HOUR_UTC = 7

# London Session (07:00 - 12:00 UTC для основной логики)
LONDON_SESSION_START_HOUR_UTC = 7
LONDON_SESSION_END_HOUR_UTC = 12

# --- Helper Functions for Time ---
def get_pip_size(symbol_str):
    if "JPY" in symbol_str.upper():
        return PIP_SIZE_JPY
    return PIP_SIZE_DEFAULT

def is_in_session(current_time_utc, start_hour, end_hour):
    """Checks if the current UTC time is within the session (exclusive of end_hour)."""
    if not isinstance(current_time_utc, pd.Timestamp):
        # Assuming current_time_utc might be a datetime.datetime if not from pandas
        current_time_utc_time = current_time_utc.time()
    else:
        current_time_utc_time = current_time_utc.time()
    
    session_start = time(start_hour, 0, 0)
    session_end = time(end_hour, 0, 0) # End hour is exclusive
    
    return session_start <= current_time_utc_time < session_end

def is_in_asia_session_for_fractal_search(bar_time_utc):
    """H1 bars from 00:00 UTC up to (but not including) ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE."""
    return ASIA_START_HOUR_UTC <= bar_time_utc.hour < ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE

def is_in_frankfurt_session_for_sweep(bar_time_utc):
    """M5 bars for sweep check during Frankfurt."""
    return FRANKFURT_SESSION_START_HOUR_UTC <= bar_time_utc.hour < FRANKFURT_SESSION_END_HOUR_UTC

def is_in_active_trading_session_for_bos_or_entry(bar_time_utc):
    """M5 bars for BOS check and entry during Frankfurt or London sessions (06:00 - 11:59 UTC)."""
    # Frankfurt starts at 6, London ends at 12 (exclusive)
    return FRANKFURT_SESSION_START_HOUR_UTC <= bar_time_utc.hour < LONDON_SESSION_END_HOUR_UTC

def reset_daily_states():
    """Resets states at the beginning of a new trading day or cycle."""
    global asia_high, asia_low, asia_high_time, asia_low_time
    global fractal_level_asia_high, fractal_level_asia_low
    global sweep_terjadi_high, sweep_terjadi_low, sweep_bar_actual_high, sweep_bar_actual_low
    global bos_level_to_break_high, bos_level_to_break_low
    
    print("[STATE_RESET] Resetting daily states.")
    # asia_high = None # Removed
    # asia_low = None # Removed
    asia_high_time = None
    asia_low_time = None
    fractal_level_asia_high = None
    fractal_level_asia_low = None
    sweep_terjadi_high = False
    sweep_terjadi_low = False
    sweep_bar_actual_high = None
    sweep_bar_actual_low = None
    bos_level_to_break_high = None
    bos_level_to_break_low = None

# --- Fractal Helper Functions ---
def _find_h1_fractals(h1_data: pd.DataFrame, fractal_lookback_period: int = H1_FRACTAL_PERIOD):
    """
    Identifies H1 fractals from the provided H1 data.
    A fractal is a high/low point with 'fractal_lookback_period' lower/higher bars on each side.
    Example: fractal_lookback_period = 2 means it's the highest/lowest of 5 bars.

    Args:
        h1_data (pd.DataFrame): DataFrame with H1 OHLC data, indexed by datetime.
                                Must contain 'high' and 'low' columns.
        fractal_lookback_period (int): Number of bars to check on each side of a potential fractal.

    Returns:
        tuple: (list_of_up_fractals, list_of_down_fractals)
               Each list contains tuples of (price, datetime).
    """
    if h1_data is None or h1_data.empty or len(h1_data) < (2 * fractal_lookback_period + 1):
        return [], []

    up_fractals = []
    down_fractals = []
    
    # Iterate from fractal_lookback_period to len(h1_data) - fractal_lookback_period -1
    # to ensure there are enough bars on both sides for comparison.
    for i in range(fractal_lookback_period, len(h1_data) - fractal_lookback_period):
        is_up_fractal = True
        is_down_fractal = True
        
        current_high = h1_data['high'].iloc[i]
        current_low = h1_data['low'].iloc[i]
        current_time = h1_data.index[i]

        for j in range(1, fractal_lookback_period + 1):
            # Check left side
            if h1_data['high'].iloc[i-j] >= current_high:
                is_up_fractal = False
            if h1_data['low'].iloc[i-j] <= current_low:
                is_down_fractal = False
            
            # Check right side
            if h1_data['high'].iloc[i+j] >= current_high:
                is_up_fractal = False
            if h1_data['low'].iloc[i+j] <= current_low:
                is_down_fractal = False
        
        if is_up_fractal:
            up_fractals.append((current_high, current_time))
        if is_down_fractal:
            down_fractals.append((current_low, current_time))
            
    return up_fractals, down_fractals

def find_nearest_h1_fractal_for_tp(trade_type: str, entry_price: float, h1_data: pd.DataFrame, pip_size: float):
    """
    Finds the nearest H1 fractal for Take Profit.
    """
    up_fractals, down_fractals = _find_h1_fractals(h1_data)
    
    nearest_level = None
    min_distance_pips = float('inf')

    if trade_type == "bullish": # TP for buy is an UP fractal
        for level, time_val in up_fractals:
            if level > entry_price:
                distance_pips = (level - entry_price) / pip_size
                if distance_pips < min_distance_pips:
                    min_distance_pips = distance_pips
                    nearest_level = level
    elif trade_type == "bearish": # TP for sell is a DOWN fractal
        for level, time_val in down_fractals:
            if level < entry_price:
                distance_pips = (entry_price - level) / pip_size
                if distance_pips < min_distance_pips:
                    min_distance_pips = distance_pips
                    nearest_level = level
    
    # print(f"[TP_DEBUG_INTERNAL] Nearest H1 Fractal for {trade_type} from entry {entry_price}: {nearest_level}")
    return nearest_level

def try_find_next_h1_fractal(trade_type: str, entry_price: float, first_fractal_level: float, h1_data: pd.DataFrame, pip_size: float):
    """
    Finds the next H1 fractal after the first_fractal_level.
    """
    up_fractals, down_fractals = _find_h1_fractals(h1_data)
    
    next_level = None
    min_distance_pips = float('inf')

    if trade_type == "bullish": # Next UP fractal
        for level, time_val in up_fractals:
            if level > entry_price and level > first_fractal_level: # Must be further than the first TP
                distance_pips = (level - entry_price) / pip_size
                if distance_pips < min_distance_pips: # Still want the closest *of the next ones*
                    min_distance_pips = distance_pips
                    next_level = level
    elif trade_type == "bearish": # Next DOWN fractal
        for level, time_val in down_fractals:
            if level < entry_price and level < first_fractal_level: # Must be further than the first TP
                distance_pips = (entry_price - level) / pip_size
                if distance_pips < min_distance_pips:
                    min_distance_pips = distance_pips
                    next_level = level
    
    # print(f"[TP_DEBUG_INTERNAL] Next H1 Fractal for {trade_type} (after {first_fractal_level}): {next_level}")
    return next_level

def calculate_take_profit(trade_type: str, entry_price: float, sl_price: float, 
                          h1_data: pd.DataFrame, pip_size: float, 
                          min_rr_val: float, max_rr_val: float):
    """
    Calculates Take Profit based on H1 fractals and Risk/Reward ratio.
    Returns: (take_profit_price, rr_achieved) or (None, 0)
    """
    sl_pips = abs(entry_price - sl_price) / pip_size
    if sl_pips == 0:
        print("[TP_CALC_ERROR] Stop loss distance is 0 pips. Cannot calculate TP.")
        return None, 0

    print(f"[TP_CALC] SL pips: {sl_pips:.1f}. Required RR range: {min_rr_val}-{max_rr_val}")

    first_tp_candidate = find_nearest_h1_fractal_for_tp(trade_type, entry_price, h1_data, pip_size)
    print(f"[TP_CALC] Nearest H1 Fractal for TP: {first_tp_candidate}")

    if first_tp_candidate is not None:
        tp1_pips = abs(first_tp_candidate - entry_price) / pip_size
        rr1 = tp1_pips / sl_pips if sl_pips > 0 else float('inf')
        print(f"[TP_CALC] First TP candidate {first_tp_candidate} ({tp1_pips:.1f} pips), RR: {rr1:.2f}")

        if min_rr_val <= rr1 <= max_rr_val:
            print(f"[TP_CALC] First TP candidate is SUITABLE. Using: {first_tp_candidate}")
            return round(first_tp_candidate, 5 if pip_size == 0.0001 else 3), rr1
        elif rr1 < min_rr_val:
            print(f"[TP_CALC] RR for First TP is TOO LOW. Searching for next H1 fractal.")
            second_tp_candidate = try_find_next_h1_fractal(trade_type, entry_price, first_tp_candidate, h1_data, pip_size)
            print(f"[TP_CALC] Next H1 Fractal for TP: {second_tp_candidate}")
            if second_tp_candidate is not None:
                tp2_pips = abs(second_tp_candidate - entry_price) / pip_size
                rr2 = tp2_pips / sl_pips if sl_pips > 0 else float('inf')
                print(f"[TP_CALC] Second TP candidate {second_tp_candidate} ({tp2_pips:.1f} pips), RR: {rr2:.2f}")
                if min_rr_val <= rr2 <= max_rr_val:
                    print(f"[TP_CALC] Second TP candidate is SUITABLE. Using: {second_tp_candidate}")
                    return round(second_tp_candidate, 5 if pip_size == 0.0001 else 3), rr2
                else:
                    print(f"[TP_CALC] RR for Second TP is NOT SUITABLE ({rr2:.2f}). No valid TP found meeting RR criteria after checking next fractal.")
                    return None, rr2 # Return actual RR for logging, even if not suitable
            else:
                print(f"[TP_CALC] No next H1 fractal found. First TP RR was {rr1:.2f}. No valid TP.")
                return None, rr1 # Return actual RR for logging
        else: # rr1 > max_rr_val
            print(f"[TP_CALC] RR for First TP is TOO HIGH ({rr1:.2f}). No valid TP found meeting RR criteria (too far).")
            return None, rr1 # Return actual RR for logging
    else:
        print(f"[TP_CALC] No H1 fractals found for TP. No valid TP.")
        return None, 0
    
# --- Position Sizing Function (NEW) ---
def calculate_position_size(account_balance: float, risk_percent: float, 
                            stop_loss_pips: float, symbol: str, 
                            pip_value_per_lot: float, 
                            min_lot: float, lot_step: float, max_lot: float) -> float:
    """
    Calculates the position size in lots based on account risk and stop loss in pips.
    Analogous to C# CalculatePositionSize.

    Args:
        account_balance (float): Current account balance.
        risk_percent (float): Percentage of account balance to risk (e.g., 1.0 for 1%).
        stop_loss_pips (float): Stop loss in pips for the trade.
        symbol (str): The trading symbol (currently used for logging/future extension).
        pip_value_per_lot (float): The value of one pip for one standard lot of the symbol 
                                   (in account currency).
        min_lot (float): Minimum tradable lot size for the symbol.
        lot_step (float): Smallest increment for lot size for the symbol.
        max_lot (float): Maximum tradable lot size for the symbol.

    Returns:
        float: Position size in lots. Returns 0 if SL is 0 or other issues occur.
    """
    if stop_loss_pips <= 0:
        print(f"[POS_SIZE_WARN] Stop loss pips is {stop_loss_pips}. Cannot calculate position size.")
        return 0.0

    risk_amount_per_trade = account_balance * (risk_percent / 100.0)
    stop_loss_amount_per_lot = stop_loss_pips * pip_value_per_lot

    if stop_loss_amount_per_lot <= 0:
        print(f"[POS_SIZE_WARN] Stop loss amount per lot is {stop_loss_amount_per_lot}. Cannot calculate position size.")
        return 0.0

    raw_position_size_lots = risk_amount_per_trade / stop_loss_amount_per_lot
    
    # Normalize to lot step: floor to the nearest lot step
    # e.g., if lot_step is 0.01, raw 0.123 becomes 0.12
    normalized_position_size = (raw_position_size_lots // lot_step) * lot_step
    
    # Ensure volume is within allowed range
    final_position_size = max(min_lot, normalized_position_size)
    final_position_size = min(max_lot, final_position_size)
    
    # Ensure final size is not less than min_lot after all calculations if it started above
    if raw_position_size_lots < min_lot: # If desired size was already less than min, but we forced it to min_lot
        print(f"[POS_SIZE_INFO] Desired raw size {raw_position_size_lots:.4f} lots for {symbol} is less than min_lot {min_lot}. Using min_lot.")
        # This might mean actual risk % is higher than target if SL is very small relative to min_lot value
    elif final_position_size < min_lot: # Should not happen if max(min_lot, ...) is used correctly
         final_position_size = min_lot # Safeguard

    if final_position_size == min_lot and normalized_position_size < min_lot:
        pass # Already handled by print above
    elif final_position_size > raw_position_size_lots and final_position_size == min_lot:
        # This can happen if raw size was e.g. 0.003, normalized to 0, then max(min_lot, 0) = min_lot
        print(f"[POS_SIZE_INFO] Raw size {raw_position_size_lots:.4f} for {symbol} normalized to {normalized_position_size:.4f}, then clamped to min_lot {min_lot}.")

    print(f"[POS_SIZE_CALC] Symbol: {symbol}, AcctBal: {account_balance}, Risk%: {risk_percent}, SLpips: {stop_loss_pips:.1f}")
    print(f"[POS_SIZE_CALC] RiskAmt: {risk_amount_per_trade:.2f}, SL_Amt/Lot: {stop_loss_amount_per_lot:.2f}")
    print(f"[POS_SIZE_CALC] RawLots: {raw_position_size_lots:.4f}, NormLots: {normalized_position_size:.4f}, FinalLots: {final_position_size:.2f}")

    if final_position_size <=0:
        print(f"[POS_SIZE_WARN] Calculated position size for {symbol} is {final_position_size:.2f}. Check parameters.")
        return 0.0
        
    return round(final_position_size, 2) # Lots are typically to 2 decimal places

# --- Trade Simulation Function ---
def simulate_trade_outcome(entry_price: float, sl_price: float, tp_price: float, 
                           trade_direction: str, entry_time: pd.Timestamp, 
                           subsequent_m5_bars_for_day: pd.DataFrame, pip_size: float,
                           position_size_lots: float, pip_value_per_lot: float):
    """
    Simulates the outcome of a trade by checking subsequent M5 bars for SL or TP hit.
    If neither is hit by the end of the provided bars (typically end of day),
    the trade is closed at the last bar's close price.

    Args:
        entry_price (float): The price at which the trade was entered.
        sl_price (float): The stop loss price.
        tp_price (float): The take profit price.
        trade_direction (str): 'bullish' or 'bearish'.
        entry_time (pd.Timestamp): The time of trade entry.
        subsequent_m5_bars_for_day (pd.DataFrame): M5 bars from entry bar onwards for the current day.
                                                    Must contain 'high', 'low', 'close' and be indexed by datetime.
        pip_size (float): The pip size for the instrument.
        position_size_lots (float): The calculated size of the position in lots.
        pip_value_per_lot (float): The monetary value of one pip for one lot.

    Returns:
        dict: Contains 'outcome' (str: 'SL_HIT', 'TP_HIT', 'CLOSED_EOD'),
              'exit_price' (float),
              'exit_time' (pd.Timestamp),
              'pnl_pips' (float),
              'pnl_currency' (float).
    """
    exit_price = None
    exit_time = None
    outcome = None
    pnl_pips = 0
    pnl_currency = 0 # NEW

    if subsequent_m5_bars_for_day.empty:
        print(f"[SIM_TRADE_ERROR] No subsequent M5 bars provided for trade entered at {entry_time}. Cannot simulate.")
        return {
            'outcome': 'ERROR_NO_BARS',
            'exit_price': entry_price,
            'exit_time': entry_time, # Or last known bar time
            'pnl_pips': 0,
            'pnl_currency': 0, # NEW
            'position_size_lots': position_size_lots # NEW
        }

    for bar_time, bar in subsequent_m5_bars_for_day.iterrows():
        # Ensure we don't process the entry bar itself if it was accidentally included
        if bar_time <= entry_time:
            continue

        if trade_direction == TrendContext.BULLISH:
            # Check SL first
            if bar['low'] <= sl_price:
                outcome = 'SL_HIT'
                exit_price = sl_price # Assume SL executed at sl_price
                exit_time = bar_time
                print(f"[SIM_TRADE] SL HIT for BUY trade at {exit_price} on bar {bar_time} (Bar Low: {bar['low']})")
                break
            # Check TP second (important: a bar could hit SL then TP, SL takes precedence)
            elif bar['high'] >= tp_price:
                outcome = 'TP_HIT'
                exit_price = tp_price # Assume TP executed at tp_price
                exit_time = bar_time
                print(f"[SIM_TRADE] TP HIT for BUY trade at {exit_price} on bar {bar_time} (Bar High: {bar['high']})")
                break
        elif trade_direction == TrendContext.BEARISH:
            # Check SL first
            if bar['high'] >= sl_price:
                outcome = 'SL_HIT'
                exit_price = sl_price
                exit_time = bar_time
                print(f"[SIM_TRADE] SL HIT for SELL trade at {exit_price} on bar {bar_time} (Bar High: {bar['high']})")
                break
            # Check TP second
            elif bar['low'] <= tp_price:
                outcome = 'TP_HIT'
                exit_price = tp_price
                exit_time = bar_time
                print(f"[SIM_TRADE] TP HIT for SELL trade at {exit_price} on bar {bar_time} (Bar Low: {bar['low']})")
                break

    # If loop finishes without SL/TP hit, close at EOD (end of provided data for the day)
    if outcome is None:
        if not subsequent_m5_bars_for_day.empty:
            last_bar_for_day = subsequent_m5_bars_for_day.iloc[-1]
            exit_price = last_bar_for_day['close']
            exit_time = subsequent_m5_bars_for_day.index[-1]
            outcome = 'CLOSED_EOD'
            print(f"[SIM_TRADE] Trade CLOSED_EOD at {exit_price} (Close of bar {exit_time})")
        else:
            # Should have been caught by the initial empty check, but as a safeguard:
            outcome = 'ERROR_NO_BARS_EOD'
            exit_price = entry_price
            exit_time = entry_time 
            print(f"[SIM_TRADE_ERROR] No bars to determine EOD close for trade from {entry_time}.")

    # Calculate P&L in pips
    if exit_price is not None:
        if trade_direction == TrendContext.BULLISH:
            pnl_pips = (exit_price - entry_price) / pip_size
        elif trade_direction == TrendContext.BEARISH:
            pnl_pips = (entry_price - exit_price) / pip_size
        pnl_currency = pnl_pips * position_size_lots * pip_value_per_lot # NEW
    
    return {
        'outcome': outcome,
        'entry_price': entry_price,
        'entry_time': entry_time,
        'sl_price': sl_price,
        'tp_price': tp_price,
        'trade_direction': trade_direction,
        'exit_price': round(exit_price, 5 if pip_size == 0.0001 else 3) if exit_price is not None else None,
        'exit_time': exit_time,
        'pnl_pips': round(pnl_pips, 2),
        'pnl_currency': round(pnl_currency, 2), # NEW
        'position_size_lots': position_size_lots # NEW
    }
    
# --- Trend Determination Functions ---
def determine_h1_trend_context(h1_data: pd.DataFrame, pip_size: float, symbol_name: str = "EURUSD") -> str:
    """
    Determines H1 trend context based on bar counts, momentum, and structure.
    Analogous to SimpleTrendContext in C#.
    """
    if h1_data is None or len(h1_data) < 25:
        print("[TREND_H1] Not enough H1 data to determine trend (< 25 bars).")
        return TrendContext.NEUTRAL

    h1_recent_25 = h1_data.iloc[-25:] # Last 25 bars
    h1_recent_10 = h1_data.iloc[-10:] # Last 10 bars for structure
    h1_recent_5 = h1_data.iloc[-5:]   # Last 5 bars for impulse

    # 1. Bar counting
    bullish_bars = 0
    bearish_bars = 0
    for _, bar in h1_recent_25.iterrows():
        if bar['close'] > bar['open']:
            bullish_bars += 1
        elif bar['close'] < bar['open']:
            bearish_bars += 1
    
    # 2. Impulse check (last 5 bars, > 40 pips)
    impulse_threshold_pips = 40
    recent_movement_pips = (h1_recent_5.iloc[-1]['close'] - h1_recent_5.iloc[0]['open']) / pip_size
    strong_bullish_impulse = recent_movement_pips > impulse_threshold_pips
    strong_bearish_impulse = recent_movement_pips < -impulse_threshold_pips

    # 3. Structure check (HH/HL or LL/LH over last 10 bars)
    has_higher_highs = False
    has_higher_lows = False
    has_lower_lows = False
    has_lower_highs = False

    if len(h1_recent_10) >= 2: # Need at least 2 bars to compare for structure
        # Check for Higher Highs and Higher Lows
        # Simplified: check if last high is > prev high, and last low is > prev low, etc.
        # More robust: look for series of HH/HL or LL/LH
        # For simplicity, let's check if the last few highs are generally increasing and lows are increasing
        # This is a simplification of the C# logic which used prevHigh/prevLow iteratively.
        # C# loop: for (int i = 9; i >= 0; i--) ... prevHigh = _h1Bars.HighPrices[last - i];
        # Python equivalent will iterate through h1_recent_10
        
        prev_high = h1_recent_10.iloc[0]['high']
        prev_low = h1_recent_10.iloc[0]['low']
        consecutive_hh = 0
        consecutive_hl = 0
        consecutive_ll = 0
        consecutive_lh = 0

        for i in range(1, len(h1_recent_10)):
            current_high = h1_recent_10.iloc[i]['high']
            current_low = h1_recent_10.iloc[i]['low']
            
            if current_high > prev_high: consecutive_hh +=1 
            else: consecutive_hh = 0 # Reset if not higher
                
            if current_low > prev_low: consecutive_hl +=1
            else: consecutive_hl = 0
            
            if current_low < prev_low: consecutive_ll +=1
            else: consecutive_ll = 0
                
            if current_high < prev_high: consecutive_lh +=1
            else: consecutive_lh = 0

            prev_high = current_high
            prev_low = current_low
            
        # Consider structure valid if we have at least 2-3 consecutive HH/HL or LL/LH
        # This is an interpretation of the C# logic which set flags like hasHigherHighs = true on any occurrence.
        # Let's use a threshold, e.g., if at least half of the recent 10 bars showed this pattern.
        # C# logic: `if (_h1Bars.HighPrices[last - i] > prevHigh) { hasHigherHighs = true; }`
        # This means any single HH in the last 10 bars would set the flag.
        # We will replicate this simpler logic first.

        # Reset for direct C# logic replication
        prev_high = h1_recent_10.iloc[0]['high']
        prev_low = h1_recent_10.iloc[0]['low']
        for i in range(1, len(h1_recent_10)):
            if h1_recent_10.iloc[i]['high'] > prev_high: has_higher_highs = True
            if h1_recent_10.iloc[i]['low'] > prev_low: has_higher_lows = True
            if h1_recent_10.iloc[i]['low'] < prev_low: has_lower_lows = True
            if h1_recent_10.iloc[i]['high'] < prev_high: has_lower_highs = True
            prev_high = h1_recent_10.iloc[i]['high']
            prev_low = h1_recent_10.iloc[i]['low']

    # 4. Decision making (mimicking C# SimpleTrendContext)
    trend_decision = TrendContext.NEUTRAL
    if (bullish_bars > bearish_bars + 5) or (has_higher_highs and has_higher_lows) or strong_bullish_impulse:
        trend_decision = TrendContext.BULLISH
    elif (bearish_bars > bullish_bars + 5) or (has_lower_lows and has_lower_highs) or strong_bearish_impulse:
        trend_decision = TrendContext.BEARISH
    
    print(f"[TREND_H1] Determined for {h1_data.index[-1].date()}: {trend_decision}. Bars B/M: {bullish_bars}/{bearish_bars}, Impulse:{recent_movement_pips:.1f} pips, HH:{has_higher_highs}, HL:{has_higher_lows}, LL:{has_lower_lows}, LH:{has_lower_highs}")
    return trend_decision

# --- Core Logic Functions (find_asia_fractals, check_sweep, check_bos) ---
def find_asia_fractals(h1_bars: pd.DataFrame, trend_h1: str):
    """
    Identifies the Asian session High/Low fractals based on H1 trend, using a 3-bar fractal definition.
    If trend_h1 is BULLISH, primarily looks for Asia Low Fractal (for potential buy setups).
    If trend_h1 is BEARISH, primarily looks for Asia High Fractal (for potential sell setups).
    If trend_h1 is NEUTRAL, finds both (though NEUTRAL days are typically skipped for trading).
    Fractals are identified on H1 bars whose open time is within the ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE window.
    The highest Up-Fractal and lowest Down-Fractal from this period are selected.
    """
    global fractal_level_asia_high, fractal_level_asia_low, asia_high_time, asia_low_time
    # Reset before finding new ones for the day
    fractal_level_asia_high = None
    fractal_level_asia_low = None
    asia_high_time = None
    asia_low_time = None

    # Filter H1 bars for the Asian session fractal evaluation window
    # is_in_asia_session_for_fractal_search uses ASIA_START_HOUR_UTC and ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE
    asia_h1_bars_for_fractal_search = h1_bars[h1_bars.index.to_series().apply(is_in_asia_session_for_fractal_search)]

    if asia_h1_bars_for_fractal_search.empty or len(asia_h1_bars_for_fractal_search) < (2 * ASIA_H1_FRACTAL_PERIOD + 1):
        required_bars = 2 * ASIA_H1_FRACTAL_PERIOD + 1
        print(f"[ASIA_FRACTAL] Not enough H1 bars ({len(asia_h1_bars_for_fractal_search)}) in Asia session for {required_bars}-bar fractal search.")
        return

    current_day_str = asia_h1_bars_for_fractal_search.index.min().date() # Date of the first bar in the asia session window

    # Find all 3-bar fractals (ASIA_H1_FRACTAL_PERIOD = 1) within this subset of H1 bars
    # _find_h1_fractals returns (price, datetime) tuples
    up_fractals, down_fractals = _find_h1_fractals(asia_h1_bars_for_fractal_search, fractal_lookback_period=ASIA_H1_FRACTAL_PERIOD)

    identified_asia_high = None
    identified_asia_high_time = None
    if up_fractals:
        identified_asia_high = max(f[0] for f in up_fractals) # Get the highest price among up-fractals
        # Get the time of the first occurrence of this max high, if multiple fractals hit the same high
        identified_asia_high_time = next((f[1] for f in up_fractals if f[0] == identified_asia_high), None)
        
    identified_asia_low = None
    identified_asia_low_time = None
    if down_fractals:
        identified_asia_low = min(f[0] for f in down_fractals) # Get the lowest price among down-fractals
        identified_asia_low_time = next((f[1] for f in down_fractals if f[0] == identified_asia_low), None)

    log_msg_parts = [f"[ASIA_FRACTAL] Evaluated Asia H1 fractals ({ASIA_H1_FRACTAL_PERIOD*2+1}-bar) for {current_day_str}:"]
    if identified_asia_high and identified_asia_high_time:
        log_msg_parts.append(f" Identified Asia High Fractal: {identified_asia_high:.5f} at {identified_asia_high_time}.")
    else:
        log_msg_parts.append(" No valid Up-Fractal found in Asia session.")
    
    if identified_asia_low and identified_asia_low_time:
        log_msg_parts.append(f" Identified Asia Low Fractal: {identified_asia_low:.5f} at {identified_asia_low_time}.")
    else:
        log_msg_parts.append(" No valid Down-Fractal found in Asia session.")

    if trend_h1 == TrendContext.BULLISH:
        if identified_asia_low and identified_asia_low_time:
            fractal_level_asia_low = identified_asia_low
            asia_low_time = identified_asia_low_time
            log_msg_parts.append(f" Trend is BULLISH, focusing on Asia Low Fractal: {fractal_level_asia_low:.5f}")
        else:
            log_msg_parts.append(" Trend is BULLISH, but no Asia Low Fractal found or its time is missing.")
    elif trend_h1 == TrendContext.BEARISH:
        if identified_asia_high and identified_asia_high_time:
            fractal_level_asia_high = identified_asia_high
            asia_high_time = identified_asia_high_time
            log_msg_parts.append(f" Trend is BEARISH, focusing on Asia High Fractal: {fractal_level_asia_high:.5f}")
        else:
            log_msg_parts.append(" Trend is BEARISH, but no Asia High Fractal found or its time is missing.")
    elif trend_h1 == TrendContext.NEUTRAL: # Neutral days are currently skipped in process_bar_data
        if identified_asia_high and identified_asia_high_time:
            fractal_level_asia_high = identified_asia_high
            asia_high_time = identified_asia_high_time
        if identified_asia_low and identified_asia_low_time:
            fractal_level_asia_low = identified_asia_low
            asia_low_time = identified_asia_low_time
        log_msg_parts.append(f" Trend is NEUTRAL. Asia High Fractal: {fractal_level_asia_high}, Low Fractal: {fractal_level_asia_low}")
    
    print("".join(log_msg_parts))

def check_sweep(current_m5_bar, m5_history_for_bos_level: pd.DataFrame, K_bars_lookback_for_bos_level: int = 3):
    """
    Checks if the current M5 bar sweeps an Asian fractal.
    If so, identifies the actual BOS level by looking at K bars *before* the sweep.
    K_bars_lookback_for_bos_level: Number of M5 bars *before* the sweep bar to check for the initiating high/low.
    """
    global sweep_terjadi_high, sweep_terjadi_low, sweep_bar_actual_high, sweep_bar_actual_low
    global fractal_level_asia_high, fractal_level_asia_low, bos_level_to_break_low, bos_level_to_break_high

    bar_time = current_m5_bar.name 
    bar_high = current_m5_bar['high']
    bar_low = current_m5_bar['low']

    m5_bars_before_current = m5_history_for_bos_level[m5_history_for_bos_level.index < bar_time]

    if bar_time.hour == 6 and bar_time.minute < 20:
        print(f"    [SWEEP_TRACE] Entered check_sweep for M5 bar {bar_time}")

    is_active_session_for_sweep = is_in_frankfurt_session_for_sweep(bar_time) or is_in_active_trading_session_for_bos_or_entry(bar_time)
    if not is_active_session_for_sweep:
        return

    if bar_time.hour == FRANKFURT_SESSION_START_HOUR_UTC and bar_time.minute < 5:
        if sweep_terjadi_high or sweep_terjadi_low:
            print(f"[SWEEP_RESET] Resetting sweep states at start of Frankfurt: {bar_time}")
            sweep_terjadi_high, sweep_terjadi_low, sweep_bar_actual_high, sweep_bar_actual_low = False, False, None, None
            bos_level_to_break_high, bos_level_to_break_low = None, None

    # Bullish Scenario: Sweep of Asian Low Fractal
    if fractal_level_asia_low is not None and not sweep_terjadi_low:
        if bar_low <= fractal_level_asia_low: 
            sweep_terjadi_low = True
            sweep_bar_actual_low = current_m5_bar 
            
            if not m5_bars_before_current.empty:
                # Determine the actual number of bars to look back, capped by K_bars_lookback_for_bos_level and available history
                actual_lookback = min(len(m5_bars_before_current), K_bars_lookback_for_bos_level)
                if actual_lookback > 0:
                    relevant_prior_bars = m5_bars_before_current.iloc[-actual_lookback:]
                    initiating_high = relevant_prior_bars['high'].max()
                    bos_level_to_break_low = round(initiating_high, 5 if get_pip_size(SYMBOL_TO_TRADE) == 0.0001 else 3)
                    print(f"[SWEEP_DEBUG] Asian Low {fractal_level_asia_low:.5f} SWEPT by M5 {bar_time} (L: {bar_low:.5f}).")
                    print(f"[SWEEP_DEBUG] BOS Level (High of last {actual_lookback} bar(s) prior to sweep): {bos_level_to_break_low:.5f} from bars ending {relevant_prior_bars.index[-1].strftime('%H:%M')}")
                else:
                    bos_level_to_break_low = None # Not enough prior bars
                    print(f"[SWEEP_WARN] Asian Low {fractal_level_asia_low:.5f} SWEPT by M5 {bar_time}, but less than 1 prior M5 bar found to determine BOS level.")
            else:
                bos_level_to_break_low = None 
                print(f"[SWEEP_WARN] Asian Low {fractal_level_asia_low:.5f} SWEPT by M5 {bar_time}, but NO prior M5 bars found to determine BOS level.")

            sweep_terjadi_high, sweep_bar_actual_high, bos_level_to_break_high = False, None, None

    # Bearish Scenario: Sweep of Asian High Fractal
    if fractal_level_asia_high is not None and not sweep_terjadi_high:
        if bar_high >= fractal_level_asia_high:
            sweep_terjadi_high = True
            sweep_bar_actual_high = current_m5_bar
            
            if not m5_bars_before_current.empty:
                actual_lookback = min(len(m5_bars_before_current), K_bars_lookback_for_bos_level)
                if actual_lookback > 0:
                    relevant_prior_bars = m5_bars_before_current.iloc[-actual_lookback:]
                    initiating_low = relevant_prior_bars['low'].min()
                    bos_level_to_break_high = round(initiating_low, 5 if get_pip_size(SYMBOL_TO_TRADE) == 0.0001 else 3)
                    print(f"[SWEEP_DEBUG] Asian High {fractal_level_asia_high:.5f} SWEPT by M5 {bar_time} (H: {bar_high:.5f}).")
                    print(f"[SWEEP_DEBUG] BOS Level (Low of last {actual_lookback} bar(s) prior to sweep): {bos_level_to_break_high:.5f} from bars ending {relevant_prior_bars.index[-1].strftime('%H:%M')}")
                else:
                    bos_level_to_break_high = None
                    print(f"[SWEEP_WARN] Asian High {fractal_level_asia_high:.5f} SWEPT by M5 {bar_time}, but less than 1 prior M5 bar found to determine BOS level.")
            else:
                bos_level_to_break_high = None
                print(f"[SWEEP_WARN] Asian High {fractal_level_asia_high:.5f} SWEPT by M5 {bar_time}, but NO prior M5 bars found to determine BOS level.")

            sweep_terjadi_low, sweep_bar_actual_low, bos_level_to_break_low = False, None, None

def check_bos(m5_bar, pip_size=0.0001):
    """Checks if the current M5 bar confirms a Break of Structure (BOS)."""
    global sweep_terjadi_low, sweep_terjadi_high, bos_level_to_break_low, bos_level_to_break_high
    global sweep_bar_actual_low, sweep_bar_actual_high
    # Access global fractal levels to potentially invalidate them if BOS is too far
    global fractal_level_asia_low, fractal_level_asia_high

    bar_time = m5_bar.name
    bar_close = m5_bar['close']

    if bar_time.hour == 6 and bar_time.minute < 20:
        print(f"      [BOS_TRACE] Entered check_bos for M5 bar {bar_time}")

    if not is_in_active_trading_session_for_bos_or_entry(bar_time):
        # print(f"    [BOS_TRACE] {bar_time}: Not in active session for BOS check.") # Verbose log if needed
        return False, None # Not in session for BOS

    # Bullish BOS: After Asian Low was swept, M5 bar closes above the identified pre-sweep high.
    if sweep_terjadi_low and bos_level_to_break_low is not None:
        if bar_time.hour == 6 and bar_time.minute < 20:
            print(f"      [BOS_TRACE] {bar_time}: Checking Bullish BOS. Target: > {bos_level_to_break_low:.5f} (PreSweepHigh), BarClose: {bar_close:.5f}")
        if bar_close > bos_level_to_break_low:
            distance_pips = (bar_close - bos_level_to_break_low) / pip_size
            print(f"[BOS_DEBUG] Bullish BOS Check: M5 {bar_time} C: {bar_close:.5f} vs PreSweepHigh: {bos_level_to_break_low:.5f}. Dist: {distance_pips:.1f} pips.")
            if distance_pips <= MAX_BOS_DISTANCE_PIPS:
                print(f"[BOS_DEBUG] Bullish BOS CONFIRMED. Distance {distance_pips:.1f} pips <= MAX_BOS_DISTANCE_PIPS ({MAX_BOS_DISTANCE_PIPS}).")
                sweep_terjadi_high = False 
                bos_level_to_break_high = None
                return True, "bullish"
            else:
                print(f"[BOS_REJECT] Bullish BOS attempt on bar {bar_time} REJECTED. Distance {distance_pips:.1f} pips > MAX_BOS_DISTANCE_PIPS ({MAX_BOS_DISTANCE_PIPS}). Asian Low Fractal {fractal_level_asia_low} invalidated for the day.")
                fractal_level_asia_low = None # Invalidate this fractal for the rest of the day
                sweep_terjadi_low = False # Reset sweep state as this path is now invalid
                bos_level_to_break_low = None
                return False, None # BOS too far

    # Bearish BOS: After Asian High was swept, M5 bar closes below the identified pre-sweep low.
    if sweep_terjadi_high and bos_level_to_break_high is not None:
        if bar_time.hour == 6 and bar_time.minute < 20:
            print(f"      [BOS_TRACE] {bar_time}: Checking Bearish BOS. Target: < {bos_level_to_break_high:.5f} (PreSweepLow), BarClose: {bar_close:.5f}")
        if bar_close < bos_level_to_break_high:
            distance_pips = (bos_level_to_break_high - bar_close) / pip_size
            print(f"[BOS_DEBUG] Bearish BOS Check: M5 {bar_time} C: {bar_close:.5f} vs PreSweepLow: {bos_level_to_break_high:.5f}. Dist: {distance_pips:.1f} pips.")
            if distance_pips <= MAX_BOS_DISTANCE_PIPS:
                print(f"[BOS_DEBUG] Bearish BOS CONFIRMED. Distance {distance_pips:.1f} pips <= MAX_BOS_DISTANCE_PIPS ({MAX_BOS_DISTANCE_PIPS}).")
                sweep_terjadi_low = False
                bos_level_to_break_low = None
                return True, "bearish"
            else:
                print(f"[BOS_REJECT] Bearish BOS attempt on bar {bar_time} REJECTED. Distance {distance_pips:.1f} pips > MAX_BOS_DISTANCE_PIPS ({MAX_BOS_DISTANCE_PIPS}). Asian High Fractal {fractal_level_asia_high} invalidated for the day.")
                fractal_level_asia_high = None # Invalidate this fractal for the rest of the day
                sweep_terjadi_high = False # Reset sweep state as this path is now invalid
                bos_level_to_break_high = None
                return False, None # BOS too far
            
    return False, None

# --- Plotting Function (NEW) ---
def plot_trade_with_context(trade_info: dict, 
                            h1_all_data: pd.DataFrame, 
                            m5_all_data: pd.DataFrame, 
                            symbol: str, 
                            plot_filename_prefix: str,
                            asia_level_data: dict = None, # {'level': float, 'time': pd.Timestamp, 'type': 'high'/'low'}
                            sweep_bar_m5_data: dict = None, # {'time': pd.Timestamp, 'high': float, 'low': float, 'close': float}
                            bos_level_data: dict = None,    # {'level': float, 'time': pd.Timestamp}
                            pip_size: float = 0.0001
                            ):
    """
    Plots H1 and M5 charts for a given trade, including context like fractals, sweep, and BOS.
    Saves the plot to a file.
    """
    if not os.path.exists("charts"):
        os.makedirs("charts")

    entry_time = trade_info['entry_time']
    exit_time = trade_info['exit_time']
    entry_price = trade_info['entry_price']
    sl_price = trade_info['sl_price']
    tp_price = trade_info['tp_price']
    exit_price = trade_info['exit_price']
    trade_direction = trade_info['trade_direction']

    # --- Determine plot ranges ---
    # M5 chart: from 1 hour before entry to 1 hour after exit (or end of data for that day)
    m5_plot_start = entry_time - timedelta(hours=1)
    m5_plot_end = exit_time + timedelta(hours=1) if exit_time else entry_time + timedelta(hours=4) # If no exit time (e.g. error), show a few hours
    
    print(f"[PLOT_DEBUG_M5] Requested M5 plot range: {m5_plot_start} to {m5_plot_end}")
    print(f"[PLOT_DEBUG_M5] m5_all_data shape: {m5_all_data.shape if m5_all_data is not None and not m5_all_data.empty else 'None or Empty'}")
    print(f"[PLOT_DEBUG_M5] Entry time: {entry_time}, Exit time: {exit_time}")

    # Ensure m5_plot_end does not go beyond available m5_all_data for that day or next day if trade spans
    if not m5_all_data.empty:
        m5_plot_end = min(m5_plot_end, m5_all_data.index.max())
        m5_plot_start = max(m5_plot_start, m5_all_data.index.min())
        print(f"[PLOT_DEBUG_M5] Adjusted M5 plot range: {m5_plot_start} to {m5_plot_end}")

    m5_df_trade = m5_all_data[(m5_all_data.index >= m5_plot_start) & (m5_all_data.index <= m5_plot_end)].copy()
    print(f"[PLOT_DEBUG_M5] m5_df_trade shape after filtering: {m5_df_trade.shape}")

    # H1 chart: Show a window of H1 bars around the trade day(s)
    h1_plot_start_date = entry_time.date() - timedelta(days=1)
    h1_plot_end_date = (exit_time.date() if exit_time else entry_time.date()) + timedelta(days=1)
    
    h1_df_trade = h1_all_data[(h1_all_data.index.date >= h1_plot_start_date) & 
                              (h1_all_data.index.date <= h1_plot_end_date)].copy()

    if m5_df_trade.empty and h1_df_trade.empty:
        print(f"[PLOT_WARN] No data available for plotting trade at {entry_time}. Skipping plot.")
        return

    # Prepare data for mplfinance (expects capitalized column names)
    for df in [m5_df_trade, h1_df_trade]:
        if not df.empty:
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)

    # --- Create Plot ---    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=False, gridspec_kw={'height_ratios': [1, 2]})
    ax_h1 = axes[0]
    ax_m5 = axes[1]

    fig.suptitle(f"{symbol} - Trade at {entry_time.strftime('%Y-%m-%d %H:%M')} ({trade_direction.capitalize()})\nOutcome: {trade_info['outcome']}, PnL: {trade_info['pnl_pips']:.1f} pips", fontsize=14)

    # --- Plot H1 Data ---
    if not h1_df_trade.empty:
        mpf.plot(h1_df_trade, type='candle', ax=ax_h1, style='yahoo', 
                 ylabel='Price (H1)', xrotation=0)
        ax_h1.set_title('H1 Context')
        ax_h1.grid(True, linestyle='--', alpha=0.7)

        # Add Entry, SL, TP lines to H1 chart
        if entry_price: ax_h1.axhline(entry_price, color='blue', linestyle='-', linewidth=1.5, label=f'Entry: {entry_price:.5f}') # Increased linewidth
        if sl_price: ax_h1.axhline(sl_price, color='red', linestyle='--', linewidth=1.5, label=f'SL: {sl_price:.5f}') # Increased linewidth, changed to dashed
        if tp_price: ax_h1.axhline(tp_price, color='green', linestyle=':', linewidth=1.5, label=f'TP: {tp_price:.5f}') # Increased linewidth, changed to dotted

        if asia_level_data and asia_level_data.get('level') is not None and asia_level_data.get('time') is not None:
            ax_h1.axhline(asia_level_data['level'], color='purple', linestyle='-.', linewidth=1.2,
                          label=f"Asia {asia_level_data['type'].capitalize()} Fr. ({asia_level_data['level']:.5f} @ {asia_level_data['time'].strftime('%H:%M')})")
        ax_h1.legend(fontsize='small')

    # --- Plot M5 Data ---
    if not m5_df_trade.empty:
        print(f"[PLOT_DEBUG_M5] m5_df_trade.head():\n{m5_df_trade.head()}")
        print(f"[PLOT_DEBUG_M5] m5_df_trade.tail():\n{m5_df_trade.tail()}")
        # print(f"[PLOT_DEBUG_M5] m5_df_trade columns before rename: {m5_df_trade.columns.tolist()}") # This log was confusing, columns are already renamed by this point by the earlier loop
        
        # Check for required uppercase columns for mplfinance
        ohlc_present = all(col in m5_df_trade.columns for col in ['Open', 'High', 'Low', 'Close'])
        print(f"[PLOT_DEBUG_M5] Required columns (Open, High, Low, Close) for mplfinance present? {ohlc_present}")

        # Ensure correct data types before plotting (already done in previous step, but as a safeguard for this block)
        m5_df_trade.index = pd.to_datetime(m5_df_trade.index)
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in m5_df_trade.columns:
                m5_df_trade[col] = pd.to_numeric(m5_df_trade[col], errors='coerce')
        m5_df_trade.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True) # Drop rows if coerce created NaNs

        if len(m5_df_trade) < 2 or not ohlc_present:
            warning_message = f"M5 data insufficient for candles ({len(m5_df_trade)} row(s))"
            if not ohlc_present:
                warning_message += " or OHLC columns missing/incorrectly named for mplfinance."
            print(f"[PLOT_WARN_M5] {warning_message}")
            ax_m5.text(0.5, 0.5, warning_message, horizontalalignment='center', verticalalignment='center', transform=ax_m5.transAxes, wrap=True)
            ax_m5.set_title('M5 Execution & Management (Candles Not Plotted)')
            ax_m5.grid(True, linestyle='--', alpha=0.7)
        else:
            try:
                print(f"[PLOT_DEBUG_M5] Plotting M5 candles with m5_df_trade (shape: {m5_df_trade.shape}).")

                # --- START: Temporary M5 Isolated Plot Test ---
                # try: 
                #     filename_safe_time = entry_time.strftime("%Y%m%d_%H%M%S") 
                #     isolated_fig_path = os.path.join("charts", f"{plot_filename_prefix}_{symbol.replace('/', '')}_{filename_safe_time}_M5_ISOLATED.png")
                #     m5_df_trade_iso = m5_df_trade.copy()
                #     for col_iso in ['Open', 'High', 'Low', 'Close']:
                #         if col_iso in m5_df_trade_iso.columns:
                #             m5_df_trade_iso[col_iso] = pd.to_numeric(m5_df_trade_iso[col_iso], errors='coerce')
                #     m5_df_trade_iso.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
                #     
                #     if not m5_df_trade_iso.empty and len(m5_df_trade_iso) >= 2:
                #         mpf.plot(m5_df_trade_iso, type='candle', style='yahoo', 
                #                  title=f"M5 Isolated Test - {symbol} - {filename_safe_time}", 
                #                  savefig=isolated_fig_path, volume=False) 
                #         print(f"[PLOT_DEBUG_M5_ISO] Isolated M5 plot saved to: {isolated_fig_path}")
                #     else:
                #         print(f"[PLOT_DEBUG_M5_ISO] Data for isolated M5 plot was empty or less than 2 rows after type conversion.")
                # except Exception as e_iso:
                #     print(f"[PLOT_ERROR_M5_ISO] Error saving isolated M5 plot: {e_iso}")
                # --- END: Temporary M5 Isolated Plot Test ---

                # Plot M5 candles FIRST (original logic for combined plot)
                mpf.plot(m5_df_trade, type='candle', ax=ax_m5, style='yahoo', 
                         ylabel='Price (M5)', xrotation=0)
                         # addplot=ap_m5 was here, ap_m5 is empty, so removed for simplicity
                
                ax_m5.set_title('M5 Execution & Management') # Set title after main plot
                ax_m5.grid(True, linestyle='--', alpha=0.7) # Set grid after main plot

                # THEN, plot horizontal lines and markers
                if sl_price: ax_m5.axhline(sl_price, color='red', linestyle='-', linewidth=1, label=f'SL: {sl_price:.5f}')
                if tp_price: ax_m5.axhline(tp_price, color='green', linestyle='-', linewidth=1, label=f'TP: {tp_price:.5f}')

                entry_marker_style = '^' if trade_direction == TrendContext.BULLISH else 'v'
                if entry_price and entry_time: 
                    ax_m5.plot(entry_time, entry_price, marker=entry_marker_style, 
                               color='blue', markersize=12, label=f'Entry @ {entry_price:.5f}')
                    va_offset = 'bottom' if trade_direction == TrendContext.BULLISH else 'top'
                    ha_offset = 'left' 
                    ax_m5.text(entry_time, entry_price, f" {entry_price:.5f}", 
                               color='blue', 
                               verticalalignment=va_offset, 
                               horizontalalignment=ha_offset, 
                               fontsize=9,
                               bbox=dict(boxstyle='round,pad=0.2', fc='yellow', alpha=0.6, ec='blue'))

                if exit_time and exit_price:
                    ax_m5.plot(exit_time, exit_price, marker='o', color='black', markersize=10, label=f'Exit: {exit_price:.5f}')

                if asia_level_data and asia_level_data.get('level') is not None:
                     ax_m5.axhline(asia_level_data['level'], color='purple', linestyle=':', linewidth=1.2, 
                                  label=f"Asia Lvl ({asia_level_data['level']:.5f})")

                if sweep_bar_m5_data and sweep_bar_m5_data.get('time') is not None:
                    sweep_time = sweep_bar_m5_data['time']
                    sweep_marker_price = sweep_bar_m5_data['low'] if trade_direction == TrendContext.BULLISH else sweep_bar_m5_data['high']
                    ax_m5.plot(sweep_time, sweep_marker_price, marker='s', color='orange', markersize=7, 
                               label=f"Sweep Bar @ {sweep_time.strftime('%H:%M')}")
                
                if bos_level_data and bos_level_data.get('level') is not None and bos_level_data.get('type'):
                    bos_line_label = f"BOS Lvl (Sweep H): {bos_level_data['level']:.5f}" if bos_level_data['type'] == 'sweep_high' else f"BOS Lvl (Sweep L): {bos_level_data['level']:.5f}"
                    ax_m5.axhline(bos_level_data['level'], color='cyan', linestyle=':', linewidth=1.2, label=bos_line_label)
                
                ax_m5.legend(fontsize='small') # Legend after all plottable items are added

            except Exception as e_mpf_m5:
                print(f"[PLOT_ERROR_M5] Error during M5 mplfinance.plot or subsequent M5 plotting: {e_mpf_m5}")
                error_text = f"Error plotting M5: {str(e_mpf_m5)[:100]}" # Limit error message length
                ax_m5.text(0.5, 0.5, error_text, horizontalalignment='center', verticalalignment='center', transform=ax_m5.transAxes, wrap=True, color='red')
                ax_m5.set_title('M5 Execution & Management (Error)')
                ax_m5.grid(True, linestyle='--', alpha=0.7)
        
        ax_m5.legend(fontsize='small') 
    else:
        print("[PLOT_INFO_M5] m5_df_trade is empty. Displaying 'No M5 data' message.")
        ax_m5.text(0.5, 0.5, "No M5 data for this plot period", horizontalalignment='center', verticalalignment='center', transform=ax_m5.transAxes)
        ax_m5.set_title('M5 Execution & Management (No Data)')
        ax_m5.grid(True, linestyle='--', alpha=0.7) # Add grid and title for consistency

    # Fine-tune layout
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save the plot
    filename_safe_time = entry_time.strftime("%Y%m%d_%H%M%S")
    plot_path = os.path.join("charts", f"{plot_filename_prefix}_{symbol.replace('/', '')}_{filename_safe_time}.png")
    try:
        plt.savefig(plot_path)
        print(f"[PLOT] Trade chart saved to: {plot_path}")
    except Exception as e:
        print(f"[PLOT_ERROR] Failed to save chart: {e}")
    plt.close(fig) # Close the figure to free memory

# --- Main Processing Loop ---
def process_bar_data(h1_dataframe, m5_dataframe, symbol):
    """ 
    Main loop to process historical data bar by bar.
    Simulates OnBar/OnTick behavior.
    Returns a list of executed trades and the final account balance.
    """
    global last_processed_h1_bar_time, last_processed_m5_bar_time
    global fractal_level_asia_high, fractal_level_asia_low, asia_high_time, asia_low_time # Added times here for plot data gathering
    global last_trade_execution_date 
    global sweep_terjadi_high, sweep_terjadi_low 
    global sweep_bar_actual_high, sweep_bar_actual_low
    global bos_level_to_break_high, bos_level_to_break_low
    global executed_trades_list 

    K_bars_lookback_for_bos_level = 3 # Define K here for process_bar_data scope

    executed_trades_list.clear() 
    current_account_balance = INITIAL_ACCOUNT_BALANCE 
    print(f"[ACCOUNT] Initial Balance: {current_account_balance:.2f}")

    if h1_dataframe.empty or m5_dataframe.empty:
        print("[PROCESS_BAR_DATA] H1 or M5 data is empty. Cannot proceed.")
        return executed_trades_list, current_account_balance

    all_m5_dates = sorted(list(set(m5_dataframe.index.date)))
    pip_size = get_pip_size(symbol)

    for current_processing_date in all_m5_dates:
        print(f"\n--- Processing data for date: {current_processing_date} ---")
        reset_daily_states() 

        if last_trade_execution_date == current_processing_date:
            print(f"[PROCESS_BAR_DATA] Trade already executed on {current_processing_date}. Skipping further processing for this date.")
            continue

        end_of_prev_day_for_trend = pd.Timestamp(current_processing_date).replace(hour=0, minute=0, second=0, microsecond=0)
        h1_data_for_trend_calc = h1_dataframe[h1_dataframe.index < end_of_prev_day_for_trend]
        current_h1_trend = determine_h1_trend_context(h1_data_for_trend_calc, pip_size, symbol)

        if current_h1_trend == TrendContext.NEUTRAL:
            print(f"[PROCESS_BAR_DATA] H1 Trend is NEUTRAL for {current_processing_date}. Skipping trading for this day.")
            continue

        h1_bars_for_asia_today = h1_dataframe[h1_dataframe.index.date == current_processing_date]
        if not h1_bars_for_asia_today.empty:
            find_asia_fractals(h1_bars_for_asia_today, current_h1_trend)
        else:
            print(f"[PROCESS_BAR_DATA] No H1 data for {current_processing_date} to find Asia fractals.")
            continue # Skip if no H1 data for the day

        if (current_h1_trend == TrendContext.BULLISH and fractal_level_asia_low is None) or \
           (current_h1_trend == TrendContext.BEARISH and fractal_level_asia_high is None):
            print(f"[PROCESS_BAR_DATA] No relevant Asian fractal identified for {current_processing_date} (Trend: {current_h1_trend}). Skipping M5 processing.")
            continue

        m5_bars_today = m5_dataframe[m5_dataframe.index.date == current_processing_date].sort_index()

        if m5_bars_today.empty:
            print(f"[PROCESS_BAR_DATA] No M5 data for {current_processing_date}. Skipping M5 processing.")
            continue
        else:
            print(f"[PROCESS_BAR_DATA] Starting M5 bar processing for {current_processing_date} ({len(m5_bars_today)} bars).")

        for m5_bar_time, m5_bar_data in m5_bars_today.iterrows():
            if (m5_bar_time.hour == FRANKFURT_SESSION_START_HOUR_UTC and m5_bar_time.minute < 30) or \
               (m5_bar_time.hour == LONDON_SESSION_START_HOUR_UTC and m5_bar_time.minute < 15):
                print(f"    [M5_DEBUG] {m5_bar_time} O:{m5_bar_data['open']:.5f} H:{m5_bar_data['high']:.5f} L:{m5_bar_data['low']:.5f} C:{m5_bar_data['close']:.5f}")
            
            if last_trade_execution_date == current_processing_date: # Double check one trade per day
                break # Already traded today, break M5 loop for this day

            can_check_sweep = False
            if current_h1_trend == TrendContext.BULLISH and fractal_level_asia_low is not None and not sweep_terjadi_low:
                can_check_sweep = True
            elif current_h1_trend == TrendContext.BEARISH and fractal_level_asia_high is not None and not sweep_terjadi_high:
                can_check_sweep = True
            
            if can_check_sweep and (is_in_frankfurt_session_for_sweep(m5_bar_time) or is_in_active_trading_session_for_bos_or_entry(m5_bar_time)):
                 check_sweep(m5_bar_data, m5_bars_today, K_bars_lookback_for_bos_level) # Pass K_bars_lookback
            
            can_check_bos = False
            if current_h1_trend == TrendContext.BULLISH and sweep_terjadi_low and bos_level_to_break_low is not None:
                can_check_bos = True
            elif current_h1_trend == TrendContext.BEARISH and sweep_terjadi_high and bos_level_to_break_high is not None:
                can_check_bos = True

            if can_check_bos and is_in_active_trading_session_for_bos_or_entry(m5_bar_time):
                bos_confirmed, trade_direction_from_bos = check_bos(m5_bar_data, pip_size)
                
                if bos_confirmed and trade_direction_from_bos == current_h1_trend:
                    if last_trade_execution_date == m5_bar_time.date(): # Redundant check but safe
                        print(f"[TRADE_LOGIC] BOS Confirmed at {m5_bar_time} but trade already made today ({last_trade_execution_date}). Internal check.")
                        continue 

                    print(f"[TRADE_LOGIC] BOS Confirmed: {trade_direction_from_bos} at {m5_bar_time}, Entry Price (Bar Close): {m5_bar_data['close']:.5f}")
                    entry_price = m5_bar_data['close']
                    sl_price = None
                    sl_pips = 0

                    if trade_direction_from_bos == TrendContext.BULLISH:
                        if sweep_bar_actual_low is None: print(f"[ERROR_SL_CALC] Bullish BOS but sweep_bar_actual_low is None. Bar: {m5_bar_time}"); continue 
                        sl_price = sweep_bar_actual_low['low'] - (STOP_LOSS_BUFFER_PIPS * pip_size)
                        sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3) 
                        calculated_sl_pips = (entry_price - sl_price) / pip_size
                        if calculated_sl_pips < MIN_SL_PIPS:
                            print(f"[SL_ADJUST] Bullish SL pips {calculated_sl_pips:.1f} < Min {MIN_SL_PIPS}. Adjusting.")
                            sl_price = entry_price - (MIN_SL_PIPS * pip_size)
                            sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                        sl_pips = (entry_price - sl_price) / pip_size # Final SL pips

                        if sl_price is not None and sl_pips > 0: 
                            take_profit_price, actual_rr = calculate_take_profit(trade_direction_from_bos, entry_price, sl_price, h1_dataframe, pip_size, MIN_RR, MAX_RR)
                            if take_profit_price is not None:
                                position_size = calculate_position_size(current_account_balance, RISK_PERCENT, sl_pips, symbol, PIP_VALUE_PER_LOT_STD_PAIR, MIN_LOT_SIZE_STD, LOT_STEP_STD, MAX_LOT_SIZE_STD)
                                if position_size <= 0: print(f"[TRADE_REJECT] Pos size {position_size:.2f}. Skipping. Bar: {m5_bar_time}"); continue
                                
                                last_trade_execution_date = m5_bar_time.date()
                                print(f"[TRADE_EXECUTION] {trade_direction_from_bos.upper()} at {m5_bar_time}. SL:{sl_price:.5f} ({sl_pips:.1f} pips), TP:{take_profit_price:.5f} (RR:{actual_rr:.2f}), Size:{position_size:.2f}")
                                
                                subsequent_m5_bars = m5_dataframe[(m5_dataframe.index.date == current_processing_date) & (m5_dataframe.index > m5_bar_time)]
                                trade_result = simulate_trade_outcome(entry_price, sl_price, take_profit_price, trade_direction_from_bos, m5_bar_time, subsequent_m5_bars, pip_size, position_size, PIP_VALUE_PER_LOT_STD_PAIR)
                                executed_trades_list.append(trade_result)
                                print(f"[TRADE_RESULT] Outcome: {trade_result['outcome']}, PnL Pips: {trade_result['pnl_pips']:.2f}, PnL Currency: {trade_result.get('pnl_currency', 'N/A'):.2f}")
                                
                                plot_asia_level_data, plot_sweep_bar_data, plot_bos_level_data = None, None, None
                                if fractal_level_asia_low is not None and asia_low_time is not None: plot_asia_level_data = {'level': fractal_level_asia_low, 'time': asia_low_time, 'type': 'low'}
                                if sweep_bar_actual_low is not None: plot_sweep_bar_data = {'time': sweep_bar_actual_low.name, 'high': sweep_bar_actual_low['high'], 'low': sweep_bar_actual_low['low'], 'close': sweep_bar_actual_low['close']}
                                if bos_level_to_break_low is not None: plot_bos_level_data = {'level': bos_level_to_break_low, 'time': m5_bar_time, 'type': 'sweep_high'}
                                plot_trade_with_context(trade_result, h1_dataframe, m5_dataframe, symbol, "trade_bullish", plot_asia_level_data, plot_sweep_bar_data, plot_bos_level_data, pip_size)
                                
                                current_account_balance += trade_result.get('pnl_currency', 0)
                                print(f"[ACCOUNT] New Balance: {current_account_balance:.2f}")
                                sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None
                                print(f"[STATE_RESET] Sweeps/BOS reset post-trade. Bar: {m5_bar_time}")
                                break # Exit M5 loop for the day after a trade
                            else: # No TP
                                print(f"[TRADE_REJECT] No TP for {trade_direction_from_bos} at {m5_bar_time}. SL pips:{sl_pips:.1f}, RR:{actual_rr:.2f}")
                                sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None; print(f"[STATE_RESET] No TP. Bar: {m5_bar_time}")
                                # Do not break here, allow other opportunities if any within same fractal sweep (unlikely with current logic but for safety)
                        else: # SL calc failed
                            print(f"[TRADE_REJECT] SL calc fail for {trade_direction_from_bos} at {m5_bar_time}")
                            sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None; print(f"[STATE_RESET] SL Fail. Bar: {m5_bar_time}")

                    elif trade_direction_from_bos == TrendContext.BEARISH:
                        if sweep_bar_actual_high is None: print(f"[ERROR_SL_CALC] Bearish BOS but sweep_bar_actual_high is None. Bar: {m5_bar_time}"); continue
                        sl_price = sweep_bar_actual_high['high'] + (STOP_LOSS_BUFFER_PIPS * pip_size)
                        sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                        calculated_sl_pips = (sl_price - entry_price) / pip_size
                        if calculated_sl_pips < MIN_SL_PIPS:
                            print(f"[SL_ADJUST] Bearish SL pips {calculated_sl_pips:.1f} < Min {MIN_SL_PIPS}. Adjusting.")
                            sl_price = entry_price + (MIN_SL_PIPS * pip_size)
                            sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                        sl_pips = (sl_price - entry_price) / pip_size # Final SL pips

                        if sl_price is not None and sl_pips > 0:
                            take_profit_price, actual_rr = calculate_take_profit(trade_direction_from_bos, entry_price, sl_price, h1_dataframe, pip_size, MIN_RR, MAX_RR)
                            if take_profit_price is not None:
                                position_size = calculate_position_size(current_account_balance, RISK_PERCENT, sl_pips, symbol, PIP_VALUE_PER_LOT_STD_PAIR, MIN_LOT_SIZE_STD, LOT_STEP_STD, MAX_LOT_SIZE_STD)
                                if position_size <= 0: print(f"[TRADE_REJECT] Pos size {position_size:.2f}. Skipping. Bar: {m5_bar_time}"); continue

                                last_trade_execution_date = m5_bar_time.date()
                                print(f"[TRADE_EXECUTION] {trade_direction_from_bos.upper()} at {m5_bar_time}. SL:{sl_price:.5f} ({sl_pips:.1f} pips), TP:{take_profit_price:.5f} (RR:{actual_rr:.2f}), Size:{position_size:.2f}")

                                subsequent_m5_bars = m5_dataframe[(m5_dataframe.index.date == current_processing_date) & (m5_dataframe.index > m5_bar_time)]
                                trade_result = simulate_trade_outcome(entry_price, sl_price, take_profit_price, trade_direction_from_bos, m5_bar_time, subsequent_m5_bars, pip_size, position_size, PIP_VALUE_PER_LOT_STD_PAIR)
                                executed_trades_list.append(trade_result)
                                print(f"[TRADE_RESULT] Outcome: {trade_result['outcome']}, PnL Pips: {trade_result['pnl_pips']:.2f}, PnL Currency: {trade_result.get('pnl_currency', 'N/A'):.2f}")
                                
                                plot_asia_level_data, plot_sweep_bar_data, plot_bos_level_data = None, None, None
                                if fractal_level_asia_high is not None and asia_high_time is not None: plot_asia_level_data = {'level': fractal_level_asia_high, 'time': asia_high_time, 'type': 'high'}
                                if sweep_bar_actual_high is not None: plot_sweep_bar_data = {'time': sweep_bar_actual_high.name, 'high': sweep_bar_actual_high['high'], 'low': sweep_bar_actual_high['low'], 'close': sweep_bar_actual_high['close']}
                                if bos_level_to_break_high is not None: plot_bos_level_data = {'level': bos_level_to_break_high, 'time': m5_bar_time, 'type': 'sweep_low'}
                                plot_trade_with_context(trade_result, h1_dataframe, m5_dataframe, symbol, "trade_bearish", plot_asia_level_data, plot_sweep_bar_data, plot_bos_level_data, pip_size)
                                
                                current_account_balance += trade_result.get('pnl_currency', 0)
                                print(f"[ACCOUNT] New Balance: {current_account_balance:.2f}")
                                sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None
                                print(f"[STATE_RESET] Sweeps/BOS reset post-trade. Bar: {m5_bar_time}")
                                break # Exit M5 loop for the day after a trade
                            else: # No TP
                                print(f"[TRADE_REJECT] No TP for {trade_direction_from_bos} at {m5_bar_time}. SL pips:{sl_pips:.1f}, RR:{actual_rr:.2f}")
                                sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None; print(f"[STATE_RESET] No TP. Bar: {m5_bar_time}")
                        else: # SL calc failed
                            print(f"[TRADE_REJECT] SL calc fail for {trade_direction_from_bos} at {m5_bar_time}")
                            sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None; print(f"[STATE_RESET] SL Fail. Bar: {m5_bar_time}")
                
                elif bos_confirmed and trade_direction_from_bos != current_h1_trend:
                    print(f"[BOS_REJECT_TREND_MISMATCH] BOS: {trade_direction_from_bos} at {m5_bar_time}, H1 Trend: {current_h1_trend}. Mismatch.")
                    sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low = False, False, None, None
                    print(f"[STATE_RESET] Sweeps/BOS reset: Trend Mismatch. Bar: {m5_bar_time}")
                    # Potentially invalidate the specific Asia fractal that led to this failed BOS, 
                    # so it's not re-evaluated on the same day with a different sweep.
                    if trade_direction_from_bos == TrendContext.BULLISH and fractal_level_asia_low is not None:
                        # This was a bullish BOS attempt that failed trend match, after sweeping Asia Low
                        # To prevent re-sweep of SAME Asia Low leading to another mismatched BOS:
                        # We might not need to do anything special if sweep_terjadi_low is reset, 
                        # as a new sweep would be needed anyway.
                        pass 
                    elif trade_direction_from_bos == TrendContext.BEARISH and fractal_level_asia_high is not None:
                        pass
                    # Continue to next M5 bar, the current setup (sweep + bos direction) is invalid.
            
            # End of M5 bar processing, loop to next M5 bar if no trade was made and day not ended by trade.

    print("\n--- Backtesting processing complete ---")
    return executed_trades_list, current_account_balance


if __name__ == '__main__':
    import data_fetcher
    import config

    parser = argparse.ArgumentParser(description="H3M Bot Backtester")
    parser.add_argument("--start_date", type=str, required=True, help="Backtest start date for M5 data and trade processing (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end_date", type=str, required=True, help="Backtest end date for M5 and H1 data (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--symbol", type=str, default=SYMBOL_TO_TRADE, help=f"Trading symbol (default: {SYMBOL_TO_TRADE})")

    args = parser.parse_args()

    try:
        # Validate date formats (basic check)
        backtest_start_datetime_obj = datetime.strptime(args.start_date, "%Y-%m-%d %H:%M:%S")
        datetime.strptime(args.end_date, "%Y-%m-%d %H:%M:%S") # end_date format check
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD HH:MM:SS for start_date and end_date.")
        exit(1)

    if config.TWELVE_DATA_API_KEY == "YOUR_API_KEY_HERE":
        print("Please set your TWELVE_DATA_API_KEY in config.py before running the main bot logic.")
    else:
        symbol_to_trade = args.symbol
        # User-defined period for M5 data and actual backtesting simulation
        user_backtest_start_date_str = args.start_date
        user_backtest_end_date_str = args.end_date

        # Calculate extended start date for H1 data fetching
        h1_fetch_start_datetime_obj = backtest_start_datetime_obj - timedelta(days=H1_DATA_PRELOAD_DAYS)
        h1_fetch_start_date_str = h1_fetch_start_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

        print(f"Starting H3M Bot backtest for {symbol_to_trade}")
        print(f"M5 Data & Trade Processing Period: {user_backtest_start_date_str} to {user_backtest_end_date_str}")
        print(f"H1 Data Fetch Period (for trend context): {h1_fetch_start_date_str} to {user_backtest_end_date_str}")

        # 1. Fetch Data
        print("\nFetching H1 data...")
        h1_data = data_fetcher.get_historical_data(
            symbol_to_trade, "1h", 
            h1_fetch_start_date_str, # Use extended start date for H1
            user_backtest_end_date_str,  # Use user-defined end date for H1
            config.TWELVE_DATA_API_KEY
        )
        sleep_timer.sleep(1) 

        print("\nFetching M5 data...")
        m5_data = data_fetcher.get_historical_data(
            symbol_to_trade, "5min", 
            user_backtest_start_date_str, # Use user-defined start for M5
            user_backtest_end_date_str,   # Use user-defined end for M5
            config.TWELVE_DATA_API_KEY
        )

        if h1_data is not None and not h1_data.empty and m5_data is not None and not m5_data.empty:
            print("\nData fetched successfully. Starting strategy processing...")
            
            executed_trades, final_account_balance = process_bar_data(h1_data, m5_data, symbol_to_trade)
            
            print("\n--- Executed Trades Summary ---")
            if not executed_trades:
                print("No trades were executed during the backtest period.")
            else:
                print(f"Total trades executed: {len(executed_trades)}")
                for i, trade in enumerate(executed_trades):
                    print(f"  Trade {i+1}: Entry {trade['entry_time']} ({trade['trade_direction']}) @ {trade['entry_price']:.5f}, "
                          f"SL {trade['sl_price']:.5f}, TP {trade['tp_price']:.5f}, Size: {trade.get('position_size_lots', 'N/A'):.2f} -> "
                          f"Exit {trade['exit_time']} @ {trade['exit_price']:.5f} ({trade['outcome']}), "
                          f"PnL Pips: {trade['pnl_pips']:.2f}, PnL Currency: {trade.get('pnl_currency', 'N/A'):.2f}")
                    if i >= 9 and len(executed_trades) > 10: # Limit output for very long lists
                        print(f"  ... and {len(executed_trades) - (i+1)} more trades.")
                        break

                # Basic statistics
                win_count = sum(1 for t in executed_trades if t['pnl_pips'] > 0)
                loss_count = sum(1 for t in executed_trades if t['pnl_pips'] < 0)
                # Breakeven trades are those with pnl_pips == 0, but not due to an error in simulation
                breakeven_count = sum(1 for t in executed_trades if t['pnl_pips'] == 0 and t['exit_price'] is not None and 'ERROR' not in t['outcome'])
                error_trades = sum(1 for t in executed_trades if 'ERROR' in t['outcome'] or t['exit_price'] is None)

                valid_trades_count = len(executed_trades) - error_trades
                total_pnl_pips = sum(t['pnl_pips'] for t in executed_trades if t['exit_price'] is not None and 'ERROR' not in t['outcome'])
                total_pnl_currency = sum(t.get('pnl_currency', 0) for t in executed_trades if t['exit_price'] is not None and 'ERROR' not in t['outcome'])

                print(f"\nWins: {win_count}, Losses: {loss_count}, Breakeven (valid): {breakeven_count}")
                if error_trades > 0:
                    print(f"Errored/Invalid trades (simulation issue or no exit): {error_trades}")
                
                if valid_trades_count > 0:
                    win_rate = win_count / valid_trades_count * 100 if valid_trades_count > 0 else 0
                    print(f"Win Rate (of valid trades): {win_rate:.2f}%")
                    avg_pnl_pips = total_pnl_pips / valid_trades_count
                    print(f"Average PnL per valid trade: {avg_pnl_pips:.2f} pips")
                    avg_pnl_currency = total_pnl_currency / valid_trades_count
                    print(f"Average PnL per valid trade: {avg_pnl_currency:.2f} (currency)")
                print(f"Total PnL (valid trades): {total_pnl_pips:.2f} pips")
                print(f"Total PnL (valid trades): {total_pnl_currency:.2f} (currency)")
                print(f"Final Account Balance: {final_account_balance:.2f} (Initial: {INITIAL_ACCOUNT_BALANCE:.2f})")

        else:
            print("\nFailed to fetch necessary data. Aborting backtest.") 