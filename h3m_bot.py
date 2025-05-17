import pandas as pd
from datetime import time, datetime, timedelta
import pytz # For timezone handling
import time as sleep_timer # Import the standard time module and alias it to avoid conflict

# --- Bot Configuration & Parameters ---
SYMBOL_TO_TRADE = "EUR/USD" # Default, can be overridden in main
STOP_LOSS_BUFFER_PIPS = 1.0 # Adjusted from C# 0.3 for M5, needs tuning. Was 5 for H1 SL rule. Let's use a small practical value.
MIN_SL_PIPS = 5.0           # Minimum stop loss in pips
MIN_RR = 1.3                # Minimum Risk/Reward ratio
MAX_RR = 5.0                # Maximum Risk/Reward ratio
MAX_BOS_DISTANCE_PIPS = 15.0 # Maximum distance for BOS confirmation in pips
H1_FRACTAL_PERIOD = 2 # Period for H1 fractal identification (N bars on each side, e.g., 2 means center of 5 bars)
ASIA_H1_FRACTAL_PERIOD = 1 # For 3-bar Asian session H1 fractals (1 bar on each side)

PIP_SIZE_DEFAULT = 0.0001     # For EURUSD like pairs
PIP_SIZE_JPY = 0.01         # For JPY pairs

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

def is_in_london_session_for_bos(bar_time_utc):
    """M5 bars for BOS check during London."""
    return LONDON_SESSION_START_HOUR_UTC <= bar_time_utc.hour < LONDON_SESSION_END_HOUR_UTC

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

def check_sweep(m5_bar):
    """Checks if the current M5 bar sweeps an Asian fractal."""
    global sweep_terjadi_high, sweep_terjadi_low, sweep_bar_actual_high, sweep_bar_actual_low
    global fractal_level_asia_high, fractal_level_asia_low, bos_level_to_break_low, bos_level_to_break_high

    bar_time = m5_bar.name # m5_bar is a Pandas Series, name is its datetime index
    bar_high = m5_bar['high']
    bar_low = m5_bar['low']
    bar_close = m5_bar['close']

    # Sweep Check (only during Frankfurt or London first hour as per original logic)
    # We'll refine this: sweep can happen anytime if fractal is set, but we only act on it in EUR sessions
    if not (is_in_frankfurt_session_for_sweep(bar_time) or is_in_london_session_for_bos(bar_time)):
        return

    # Reset sweep states if entering Frankfurt session, before any checks
    # This was originally done if Server.Time.Hour < 6 (UTC), meaning before Frankfurt
    # Here, we do it on the first relevant bar of Frankfurt/London if not already done for the day.
    # A more robust daily reset is handled by `reset_daily_states` called at start of day processing.
    if bar_time.hour == FRANKFURT_SESSION_START_HOUR_UTC and bar_time.minute < 5: # First M5 bar of Frankfurt
        if sweep_terjadi_high or sweep_terjadi_low:
            print(f"[SWEEP_RESET] Resetting sweep states at start of Frankfurt: {bar_time}")
            sweep_terjadi_high = False
            sweep_terjadi_low = False
            sweep_bar_actual_high = None
            sweep_bar_actual_low = None
            bos_level_to_break_high = None
            bos_level_to_break_low = None

    # Bullish Scenario: Sweep of Asian Low Fractal
    if fractal_level_asia_low is not None and not sweep_terjadi_low:
        if bar_low < fractal_level_asia_low:
            sweep_terjadi_low = True
            sweep_bar_actual_low = m5_bar # Store the whole bar
            # Level to break for Bullish BOS is the CLOSE of the sweep bar
            bos_level_to_break_low = sweep_bar_actual_low['close'] 
            print(f"[SWEEP_DEBUG] Asian Low Fractal {fractal_level_asia_low} SWEPT by M5 bar {bar_time} (L: {bar_low}, C: {bar_close}). BOS Level (Sweep Close): {bos_level_to_break_low}")
            # In a bullish sweep, we don't care about further high sweeps for now
            sweep_terjadi_high = False 
            sweep_bar_actual_high = None
            bos_level_to_break_high = None

    # Bearish Scenario: Sweep of Asian High Fractal
    if fractal_level_asia_high is not None and not sweep_terjadi_high:
        if bar_high > fractal_level_asia_high:
            sweep_terjadi_high = True
            sweep_bar_actual_high = m5_bar # Store the whole bar
            # Level to break for Bearish BOS is the CLOSE of the sweep bar
            bos_level_to_break_high = sweep_bar_actual_high['close'] 
            print(f"[SWEEP_DEBUG] Asian High Fractal {fractal_level_asia_high} SWEPT by M5 bar {bar_time} (H: {bar_high}, C: {bar_close}). BOS Level (Sweep Close): {bos_level_to_break_high}")
            # In a bearish sweep, we don't care about further low sweeps for now
            sweep_terjadi_low = False 
            sweep_bar_actual_low = None
            bos_level_to_break_low = None

def check_bos(m5_bar, pip_size=0.0001):
    """Checks if the current M5 bar confirms a Break of Structure (BOS)."""
    global sweep_terjadi_low, sweep_terjadi_high, bos_level_to_break_low, bos_level_to_break_high
    global sweep_bar_actual_low, sweep_bar_actual_high
    # Access global fractal levels to potentially invalidate them if BOS is too far
    global fractal_level_asia_low, fractal_level_asia_high

    bar_time = m5_bar.name
    bar_close = m5_bar['close']

    if not is_in_london_session_for_bos(bar_time):
        return False, None # Not in session for BOS

    # Bullish BOS: After Asian Low was swept, M5 bar closes above the CLOSE of the sweep bar.
    if sweep_terjadi_low and bos_level_to_break_low is not None:
        if bar_close > bos_level_to_break_low:
            distance_pips = (bar_close - bos_level_to_break_low) / pip_size
            print(f"[BOS_DEBUG] Bullish BOS Check: M5 {bar_time} C: {bar_close} vs SweepBarClose: {bos_level_to_break_low}. Dist: {distance_pips:.1f} pips.")
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

    # Bearish BOS: After Asian High was swept, M5 bar closes below the CLOSE of the sweep bar.
    if sweep_terjadi_high and bos_level_to_break_high is not None:
        if bar_close < bos_level_to_break_high:
            distance_pips = (bos_level_to_break_high - bar_close) / pip_size
            print(f"[BOS_DEBUG] Bearish BOS Check: M5 {bar_time} C: {bar_close} vs SweepBarClose: {bos_level_to_break_high}. Dist: {distance_pips:.1f} pips.")
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

# --- Main Processing Loop ---
def process_bar_data(h1_dataframe, m5_dataframe, symbol):
    """ 
    Main loop to process historical data bar by bar.
    Simulates OnBar/OnTick behavior.
    """
    global last_processed_h1_bar_time, last_processed_m5_bar_time
    global fractal_level_asia_high, fractal_level_asia_low
    global last_trade_execution_date # Ensure we can modify it
    # Declare all globals that are modified in this function at the top
    global sweep_terjadi_high, sweep_terjadi_low 
    global sweep_bar_actual_high, sweep_bar_actual_low
    global bos_level_to_break_high, bos_level_to_break_low

    # Combine and sort all bars by time to process chronologically
    # For simplicity, we'll iterate M5 bars and fetch relevant H1 state as needed.
    # However, Asia fractals are determined from H1 bars first for a given day.

    # We need to ensure H1 Asia fractals are identified *before* M5 bars of that day are processed for sweeps.
    # We can iterate by days, or process H1s first up to a point.

    # Let's process day by day for clarity in backtesting.
    if h1_dataframe.empty or m5_dataframe.empty:
        print("[PROCESS_BAR_DATA] H1 or M5 data is empty. Cannot proceed.")
        return

    all_m5_dates = sorted(list(set(m5_dataframe.index.date)))
    
    pip_size = get_pip_size(symbol)

    for current_processing_date in all_m5_dates:
        print(f"\n--- Processing data for date: {current_processing_date} ---")
        reset_daily_states() # Reset for the new day

        # Check if a trade has already been executed for this date (based on global state)
        if last_trade_execution_date == current_processing_date:
            print(f"[PROCESS_BAR_DATA] Trade already executed on {current_processing_date}. Skipping further processing for this date.")
            continue

        # 1. Determine H1 Trend Context for the current day
        # We need H1 data up to the start of current_processing_date to determine trend for it.
        h1_data_for_trend = h1_dataframe[h1_dataframe.index.date < current_processing_date]
        # Or, if we want to use data *of* the current day up to Asia, it's more complex.
        # For simplicity, let's use data *before* the current day to establish a daily bias.
        # C# logic for trend was often on _h1Bars.Last(x) which implies current data.
        # Let's use H1 data available *at the beginning* of current_processing_date.
        # This usually means all H1 bars whose open time is < current_processing_date 00:00:00.
        # Or, more practically, all H1 data up to the end of the *previous* trading day.
        # For a daily trend, it's often set at market open or after Asia.
        # Let's take all H1 data *up to the current processing date's Asia session start time*.
        end_of_prev_day_for_trend = pd.Timestamp(current_processing_date).replace(hour=0, minute=0, second=0, microsecond=0)
        h1_data_for_trend_calc = h1_dataframe[h1_dataframe.index < end_of_prev_day_for_trend]

        current_h1_trend = determine_h1_trend_context(h1_data_for_trend_calc, pip_size, symbol)

        if current_h1_trend == TrendContext.NEUTRAL:
            print(f"[PROCESS_BAR_DATA] H1 Trend is NEUTRAL for {current_processing_date}. Skipping trading for this day.")
            continue

        # 2. Identify Asia Fractals based on H1 trend
        h1_bars_for_asia_today = h1_dataframe[h1_dataframe.index.date == current_processing_date]
        if not h1_bars_for_asia_today.empty:
            find_asia_fractals(h1_bars_for_asia_today, current_h1_trend)
        else:
            print(f"[PROCESS_BAR_DATA] No H1 data for {current_processing_date} to find Asia fractals.")
            # Potentially load more H1 data if needed, or skip if it implies no trading day

        # If no fractals were found (e.g. weekend, holiday, missing data), skip M5 processing for this day
        if fractal_level_asia_high is None and fractal_level_asia_low is None:
            print(f"[PROCESS_BAR_DATA] No Asian fractals identified for {current_processing_date} (Trend: {current_h1_trend}). Skipping M5 processing.")
            continue

        # 3. Process M5 bars for the current_processing_date
        m5_bars_today = m5_dataframe[m5_dataframe.index.date == current_processing_date].sort_index()

        for m5_bar_time, m5_bar_data in m5_bars_today.iterrows():
            # m5_bar_data is a Pandas Series with O,H,L,C,V for that M5 bar
            # m5_bar_time is the datetime index of the bar
            
            # print(f"Processing M5 bar: {m5_bar_time} O:{m5_bar_data['open']} H:{m5_bar_data['high']} L:{m5_bar_data['low']} C:{m5_bar_data['close']}")
            
            # A. Check for Sweep (during Frankfurt & London)
            # Only proceed if the relevant fractal (based on H1 trend) is set
            can_check_sweep = False
            if current_h1_trend == TrendContext.BULLISH and fractal_level_asia_low is not None:
                can_check_sweep = True
            elif current_h1_trend == TrendContext.BEARISH and fractal_level_asia_high is not None:
                can_check_sweep = True
            # If NEUTRAL trend was allowed, it would check based on any fractal found.
            # However, we added a 'continue' if trend is NEUTRAL earlier.
            
            if can_check_sweep:
                 check_sweep(m5_bar_data) # check_sweep internally checks its own session times
            else:
                # This case should ideally not be hit frequently if NEUTRAL trend days are skipped
                # and if non-relevant fractals are None due to find_asia_fractals logic.
                pass # No relevant fractal to check for sweep based on trend

            # B. Check for BOS (during London)
            # BOS can only happen *after* a sweep has occurred AND the H1 trend aligns.
            can_check_bos = False
            direction_to_check = None
            if current_h1_trend == TrendContext.BULLISH and sweep_terjadi_low and bos_level_to_break_low is not None:
                can_check_bos = True
                direction_to_check = TrendContext.BULLISH
            elif current_h1_trend == TrendContext.BEARISH and sweep_terjadi_high and bos_level_to_break_high is not None:
                can_check_bos = True
                direction_to_check = TrendContext.BEARISH
            
            if can_check_bos:
                # Pass the expected direction to check_bos, or let check_bos determine itself
                # For now, check_bos determines direction internally but only one sweep flag (high or low) should be true.
                bos_confirmed, trade_direction = check_bos(m5_bar_data, pip_size)
                
                # Ensure the confirmed BOS direction matches the H1 trend direction
                if bos_confirmed and trade_direction == current_h1_trend:
                    # Check if a trade has already been made today (important after BOS confirmation)
                    if last_trade_execution_date == m5_bar_time.date():
                        print(f"[TRADE_LOGIC] BOS Confirmed at {m5_bar_time} but trade already made today ({last_trade_execution_date}). Skipping new trade.")
                        # Reset sweeps here to prevent re-triggering on the same day even if no new trade
                        sweep_terjadi_high = False
                        sweep_terjadi_low = False
                        bos_level_to_break_high = None
                        bos_level_to_break_low = None
                        continue # Move to next M5 bar

                    print(f"[TRADE_LOGIC] BOS Confirmed: {trade_direction} at {m5_bar_time}, Entry Price (Bar Close): {m5_bar_data['close']}")
                    
                    entry_price = m5_bar_data['close']
                    sl_price = None
                    sl_pips = 0

                    if trade_direction == TrendContext.BULLISH:
                        if sweep_bar_actual_low is None:
                            print(f"[ERROR_SL_CALC] Bullish BOS but sweep_bar_actual_low is None. Cannot set SL. Bar: {m5_bar_time}")
                            continue # Skip this trade signal
                        # SL below the low of the M5 sweep bar + buffer
                        sl_price = sweep_bar_actual_low['low'] - (STOP_LOSS_BUFFER_PIPS * pip_size)
                        sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3) # Round to symbol precision
                        sl_pips = (entry_price - sl_price) / pip_size
                        
                        if sl_pips < MIN_SL_PIPS:
                            print(f"[SL_ADJUST] Original Bullish SL pips {sl_pips:.1f} < Min SL pips {MIN_SL_PIPS}. Adjusting SL.")
                            sl_price = entry_price - (MIN_SL_PIPS * pip_size)
                            sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                            sl_pips = (entry_price - sl_price) / pip_size

                        # Now calculate Take Profit using the new H1 fractal logic
                        if sl_price is not None and sl_pips > 0: # Ensure SL is valid before TP calc
                            take_profit_price, actual_rr = calculate_take_profit(
                                trade_direction, 
                                entry_price, 
                                sl_price, 
                                h1_dataframe, # Pass the full H1 dataframe
                                pip_size,
                                MIN_RR,
                                MAX_RR
                            )

                            if take_profit_price is not None:
                                print(f"[TRADE_SIM] {trade_direction.capitalize()} Entry: {entry_price:.5f}, SL: {sl_price:.5f} ({sl_pips:.1f} pips), TP: {take_profit_price:.5f} (RR: {actual_rr:.2f})")
                                last_trade_execution_date = m5_bar_time.date()
                                print(f"[TRADE_EXECUTION] Trade logged for {trade_direction} at {m5_bar_time}. SL pips: {sl_pips:.1f}, TP RR: {actual_rr:.2f}. One trade per day rule active for {last_trade_execution_date}.")
                                
                                # Reset sweeps and BOS levels after successful trade signal
                                sweep_terjadi_high = False
                                sweep_terjadi_low = False
                                bos_level_to_break_high = None
                                bos_level_to_break_low = None
                                print(f"[STATE_RESET] Sweeps and BOS levels reset after trade signal at {m5_bar_time}.")
                            else:
                                print(f"[TRADE_REJECT] BOS Confirmed for {trade_direction} at {m5_bar_time}, but no suitable Take Profit found meeting RR criteria {MIN_RR}-{MAX_RR}. SL pips: {sl_pips:.1f}, Achieved RR: {actual_rr:.2f}. No trade.")
                                # Do NOT set last_trade_execution_date here
                                # Sweeps should still be reset to avoid repeated attempts on the same failed setup for the day
                                sweep_terjadi_high = False
                                sweep_terjadi_low = False
                                bos_level_to_break_high = None
                                bos_level_to_break_low = None
                                print(f"[STATE_RESET] Sweeps and BOS levels reset after trade attempt (or signal) at {m5_bar_time}.")
                            continue # Move to next M5 bar
                        else:
                            print(f"[TRADE_REJECT] SL calculation failed or SL pips is zero for {trade_direction} at {m5_bar_time}. Cannot calculate TP. No trade.")
                            # Sweeps should also be reset here
                            sweep_terjadi_high = False
                            sweep_terjadi_low = False
                            bos_level_to_break_high = None
                            bos_level_to_break_low = None
                            print(f"[STATE_RESET] Sweeps and BOS levels reset after trade attempt (or signal) at {m5_bar_time}.")
                            continue # Move to next M5 bar
                    elif trade_direction == TrendContext.BEARISH:
                        if sweep_bar_actual_high is None:
                            print(f"[ERROR_SL_CALC] Bearish BOS but sweep_bar_actual_high is None. Cannot set SL. Bar: {m5_bar_time}")
                            # Reset states and continue
                            sweep_terjadi_high = False
                            sweep_terjadi_low = False
                            bos_level_to_break_high = None
                            bos_level_to_break_low = None
                            print(f"[STATE_RESET] Critical error in SL calc, states reset. Bar: {m5_bar_time}")
                            continue # Skip this trade signal

                        sl_price = sweep_bar_actual_high['high'] + (STOP_LOSS_BUFFER_PIPS * pip_size)
                        sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                        sl_pips = (sl_price - entry_price) / pip_size

                        if sl_pips < MIN_SL_PIPS:
                            print(f"[SL_ADJUST] Original Bearish SL pips {sl_pips:.1f} < Min SL pips {MIN_SL_PIPS}. Adjusting SL.")
                            sl_price = entry_price + (MIN_SL_PIPS * pip_size)
                            sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                            sl_pips = (sl_price - entry_price) / pip_size # Recalculate pips
                        
                        if sl_price is not None and sl_pips > 0:
                            take_profit_price, actual_rr = calculate_take_profit(
                                trade_direction, entry_price, sl_price, h1_dataframe, pip_size, MIN_RR, MAX_RR
                            )
                            if take_profit_price is not None:
                                print(f"[TRADE_SIM] {trade_direction.capitalize()} Entry: {entry_price:.5f}, SL: {sl_price:.5f} ({sl_pips:.1f} pips), TP: {take_profit_price:.5f} (RR: {actual_rr:.2f})")
                                last_trade_execution_date = m5_bar_time.date()
                                print(f"[TRADE_EXECUTION] Trade logged for {trade_direction} at {m5_bar_time}. SL pips: {sl_pips:.1f}, TP RR: {actual_rr:.2f}. One trade per day rule active for {last_trade_execution_date}.")
                                sweep_terjadi_high = False
                                sweep_terjadi_low = False
                                bos_level_to_break_high = None
                                bos_level_to_break_low = None
                                print(f"[STATE_RESET] Sweeps and BOS levels reset after trade signal at {m5_bar_time}.")
                            else:
                                print(f"[TRADE_REJECT] BOS Confirmed for {trade_direction} at {m5_bar_time}, but no suitable Take Profit found meeting RR criteria {MIN_RR}-{MAX_RR}. SL pips: {sl_pips:.1f}, Achieved RR: {actual_rr:.2f}. No trade.")
                                sweep_terjadi_high = False
                                sweep_terjadi_low = False
                                bos_level_to_break_high = None
                                bos_level_to_break_low = None
                                print(f"[STATE_RESET] Sweeps and BOS levels reset after trade attempt (or signal) at {m5_bar_time}.")
                            continue # Move to next M5 bar
                        else:
                            print(f"[TRADE_REJECT] SL calculation failed or SL pips is zero for {trade_direction} at {m5_bar_time}. Cannot calculate TP. No trade.")
                            sweep_terjadi_high = False
                            sweep_terjadi_low = False
                            bos_level_to_break_high = None
                            bos_level_to_break_low = None
                            print(f"[STATE_RESET] Sweeps and BOS levels reset after trade attempt (or signal) at {m5_bar_time}.")
                            continue # Move to next M5 bar
                    else: # Should not happen if direction is only "bullish" or "bearish"
                        print(f"[ERROR_DIRECTION] Unknown direction '{trade_direction}' at {m5_bar_time}. States reset.")
                        sweep_terjadi_high = False
                        sweep_terjadi_low = False
                        bos_level_to_break_high = None
                        bos_level_to_break_low = None
                        # No continue here, will fall through to the end of m5_bar loop
                    
                    # The common reset logic previously at line 505 and onwards is now handled
                    # within each specific branch (trade success, trade reject due to TP/RR, trade reject due to SL error).
                    # This ensures that states are reset appropriately for each case and avoids the SyntaxError.
                    # We also added 'continue' in some branches to ensure that after a trade decision (or failure to make one),
                    # we move to the next M5 bar, as the setup has been "consumed".
                    # However, looking back, the continue was removed and reset happens inside the block.
                    # The very last common reset block is no longer needed if all paths handle it.
                    # Let's ensure all paths that lead to a "consumed signal" reset state and then we can remove the final common one.

                    # The previous logic had a common reset at the end.
                    # I've moved the reset into each condition (TP success, TP fail, SL fail).
                    # This makes the common reset block below redundant and was the source of the error.
                    # So, I will remove the `global` declaration at line 505 and the subsequent resets,
                    # as they are now handled within the conditional blocks above.
                    
                    # After a BOS is confirmed and an attempt to trade is made (successful or not),
                    # we typically want to move to the next M5 bar as this specific setup is now processed.
                    continue # Process next M5 bar

    print("\n--- Backtesting processing complete ---")


if __name__ == '__main__':
    import data_fetcher
    import config

    if config.TWELVE_DATA_API_KEY == "YOUR_API_KEY_HERE":
        print("Please set your TWELVE_DATA_API_KEY in config.py before running the main bot logic.")
    else:
        symbol_to_trade = SYMBOL_TO_TRADE # Use the constant from top of the file
        # Define the backtesting period
        # Example: The problematic period from your C# logs
        # User mentioned 14.05.2025 - assuming typo for 2024 for real data
        # Let's pick a small recent range for testing that includes a few days
        backtest_start_date = "2024-05-10 00:00:00"
        backtest_end_date = "2024-05-15 23:59:59" # Ensure it covers the full last day

        print(f"Starting H3M Bot backtest for {symbol_to_trade} from {backtest_start_date} to {backtest_end_date}")

        # 1. Fetch Data
        print("\nFetching H1 data...")
        h1_data = data_fetcher.get_historical_data(
            symbol_to_trade, "1h", 
            backtest_start_date, backtest_end_date, 
            config.TWELVE_DATA_API_KEY
        )
        sleep_timer.sleep(1) # Respect API rate limits if on a free plan

        print("\nFetching M5 data...")
        m5_data = data_fetcher.get_historical_data(
            symbol_to_trade, "5min", 
            backtest_start_date, backtest_end_date, 
            config.TWELVE_DATA_API_KEY
        )
        # No need for sleep after the last API call in this sequence

        if h1_data is not None and not h1_data.empty and m5_data is not None and not m5_data.empty:
            print("\nData fetched successfully. Starting strategy processing...")
            # Ensure data is UTC (already handled by data_fetcher, but good to be aware)
            # h1_data.index = h1_data.index.tz_localize('UTC') if h1_data.index.tz is None else h1_data.index.tz_convert('UTC')
            # m5_data.index = m5_data.index.tz_localize('UTC') if m5_data.index.tz is None else m5_data.index.tz_convert('UTC')
            
            process_bar_data(h1_data, m5_data, symbol_to_trade)
        else:
            print("\nFailed to fetch necessary data. Aborting backtest.") 