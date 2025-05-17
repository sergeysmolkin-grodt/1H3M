import pandas as pd
from datetime import time, datetime, timedelta
import pytz # For timezone handling

# --- Bot Configuration & Parameters ---
SYMBOL_TO_TRADE = "EUR/USD" # Default, can be overridden in main
STOP_LOSS_BUFFER_PIPS = 1.0 # Adjusted from C# 0.3 for M5, needs tuning. Was 5 for H1 SL rule. Let's use a small practical value.
MIN_SL_PIPS = 5.0           # Minimum stop loss in pips
# MIN_RR = 1.3                # Minimum Risk/Reward ratio - will add later with TP logic
# MAX_RR = 5.0                # Maximum Risk/Reward ratio - will add later with TP logic
# MAX_BOS_DISTANCE_PIPS = 15.0 # Maximum distance for BOS confirmation - will add later in check_bos

PIP_SIZE_DEFAULT = 0.0001     # For EURUSD like pairs
PIP_SIZE_JPY = 0.01         # For JPY pairs

# Global state variables (аналогично вашим переменным в C#)
asia_high = None
asia_low = None
asia_high_time = None
asia_low_time = None

fractal_level_asia_high = None
fractal_level_asia_low = None

sweep_ terjadi_high = False
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
ASIA_END_HOUR_UTC = 6 # Сессия длится до этого часа, но фракталы по свечам ДО этого часа
ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE = 5 # H1 свечи *до* этого часа (т.е. 00,01,02,03,04) используются для поиска фрактала

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
    asia_high = None
    asia_low = None
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

# --- Placeholder for Core Logic --- 
def find_asia_fractals(h1_bars):
    """
    Identifies the highest high and lowest low during the Asian session 
    from H1 bars up to ASIA_FRACTAL_EVAL_HOUR_UTC_EXCLUSIVE.
    These will be our initial fractal_level_asia_high/low.
    """
    global asia_high, asia_low, asia_high_time, asia_low_time
    global fractal_level_asia_high, fractal_level_asia_low

    # Filter for relevant Asian session bars
    # Assuming h1_bars is a DataFrame indexed by datetime
    asia_h1_bars = h1_bars[h1_bars.index.to_series().apply(is_in_asia_session_for_fractal_search)]

    if asia_h1_bars.empty:
        # print("[ASIA_FRACTAL] No H1 bars found in Asia session for fractal search.")
        return

    current_asia_high = asia_h1_bars['high'].max()
    current_asia_low = asia_h1_bars['low'].min()
    
    # Store the actual levels that will be used as fractals
    fractal_level_asia_high = current_asia_high
    fractal_level_asia_low = current_asia_low
    
    # For logging/tracking, find the times of these extremes
    # If multiple bars have the same high/low, take the first one (though time doesn't strictly matter for the level itself)
    asia_high_bar = asia_h1_bars[asia_h1_bars['high'] == current_asia_high].iloc[0]
    asia_low_bar = asia_h1_bars[asia_h1_bars['low'] == current_asia_low].iloc[0]
    
    asia_high_time = asia_high_bar.name # Index is datetime
    asia_low_time = asia_low_bar.name

    print(f"[ASIA_FRACTAL] Asia H1 Fractals for {asia_h1_bars.index.min().date()}: High={fractal_level_asia_high} at {asia_high_time}, Low={fractal_level_asia_low} at {asia_low_time}")


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

    bar_time = m5_bar.name
    bar_close = m5_bar['close']

    if not is_in_london_session_for_bos(bar_time):
        return False, None # Not in session for BOS

    # Bullish BOS: After Asian Low was swept, M5 bar closes above the CLOSE of the sweep bar.
    if sweep_terjadi_low and bos_level_to_break_low is not None:
        if bar_close > bos_level_to_break_low:
            distance_pips = (bar_close - bos_level_to_break_low) / pip_size
            # Original C# code had: Math.Abs(MarketSeries.Close.Last(1) - levelToBreak) >= _minBOSDistancePips * Symbol.PipSize;
            # Here we just check if it's broken. Distance check for entry validity might be separate or incorporated.
            # For now, let's assume any break is a BOS signal, distance can be logged.
            print(f"[BOS_DEBUG] Bullish BOS! M5 {bar_time} C: {bar_close} broke above SweepBarClose: {bos_level_to_break_low}. Dist: {distance_pips:.1f} pips.")
            # Invalidate further bearish checks for this sequence
            sweep_terjadi_high = False 
            bos_level_to_break_high = None
            return True, "bullish"

    # Bearish BOS: After Asian High was swept, M5 bar closes below the CLOSE of the sweep bar.
    if sweep_terjadi_high and bos_level_to_break_high is not None:
        if bar_close < bos_level_to_break_high:
            distance_pips = (bos_level_to_break_high - bar_close) / pip_size
            print(f"[BOS_DEBUG] Bearish BOS! M5 {bar_time} C: {bar_close} broke below SweepBarClose: {bos_level_to_break_high}. Dist: {distance_pips:.1f} pips.")
            # Invalidate further bullish checks for this sequence
            sweep_terjadi_low = False
            bos_level_to_break_low = None
            return True, "bearish"
            
    return False, None


def process_bar_data(h1_dataframe, m5_dataframe, symbol):
    """ 
    Main loop to process historical data bar by bar.
    Simulates OnBar/OnTick behavior.
    """
    global last_processed_h1_bar_time, last_processed_m5_bar_time
    global fractal_level_asia_high, fractal_level_asia_low
    global last_trade_execution_date # Ensure we can modify it

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

        # 1. Identify Asia Fractals for the current_processing_date using H1 bars up to that day
        # We need H1 bars of current_processing_date for Asia session.
        # The H1 bars should be available before M5 bars of the same session.
        h1_bars_for_asia_today = h1_dataframe[h1_dataframe.index.date == current_processing_date]
        if not h1_bars_for_asia_today.empty:
            find_asia_fractals(h1_bars_for_asia_today)
        else:
            print(f"[PROCESS_BAR_DATA] No H1 data for {current_processing_date} to find Asia fractals.")
            # Potentially load more H1 data if needed, or skip if it implies no trading day

        # If no fractals were found (e.g. weekend, holiday, missing data), skip M5 processing for this day
        if fractal_level_asia_high is None and fractal_level_asia_low is None:
            print(f"[PROCESS_BAR_DATA] No Asian fractals identified for {current_processing_date}. Skipping M5 processing for this day.")
            continue

        # 2. Process M5 bars for the current_processing_date
        m5_bars_today = m5_dataframe[m5_dataframe.index.date == current_processing_date].sort_index()

        for m5_bar_time, m5_bar_data in m5_bars_today.iterrows():
            # m5_bar_data is a Pandas Series with O,H,L,C,V for that M5 bar
            # m5_bar_time is the datetime index of the bar
            
            # print(f"Processing M5 bar: {m5_bar_time} O:{m5_bar_data['open']} H:{m5_bar_data['high']} L:{m5_bar_data['low']} C:{m5_bar_data['close']}")
            
            # A. Check for Sweep (during Frankfurt & London)
            if (fractal_level_asia_low or fractal_level_asia_high):
                 check_sweep(m5_bar_data) # m5_bar_data is a Series, its name is the timestamp

            # B. Check for BOS (during London)
            # BOS can only happen *after* a sweep has occurred and a bos_level is set.
            if (sweep_terjadi_low or sweep_terjadi_high) and (bos_level_to_break_low or bos_level_to_break_high):
                bos_confirmed, direction = check_bos(m5_bar_data, pip_size)
                if bos_confirmed:
                    # Check if a trade has already been made today (important after BOS confirmation)
                    if last_trade_execution_date == m5_bar_time.date():
                        print(f"[TRADE_LOGIC] BOS Confirmed at {m5_bar_time} but trade already made today ({last_trade_execution_date}). Skipping new trade.")
                        # Reset sweeps here to prevent re-triggering on the same day even if no new trade
                        sweep_terjadi_high = False
                        sweep_terjadi_low = False
                        bos_level_to_break_high = None
                        bos_level_to_break_low = None
                        continue # Move to next M5 bar

                    print(f"[TRADE_LOGIC] BOS Confirmed: {direction} at {m5_bar_time}, Entry Price (Bar Close): {m5_bar_data['close']}")
                    
                    entry_price = m5_bar_data['close']
                    sl_price = None
                    sl_pips = 0

                    if direction == "bullish":
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


                        # TP logic was 1:1 or 1:2 based on SL distance from entry
                        # TODO: Implement advanced TP logic from C#
                        tp1_price = entry_price + (sl_pips * pip_size) # 1:1
                        tp2_price = entry_price + (sl_pips * 2 * pip_size) # 1:2
                        tp1_price = round(tp1_price, 5 if pip_size == 0.0001 else 3)
                        tp2_price = round(tp2_price, 5 if pip_size == 0.0001 else 3)
                        print(f"[TRADE_SIM] Bullish Entry: {entry_price:.5f}, SL: {sl_price:.5f} ({sl_pips:.1f} pips), TP1: {tp1_price:.5f}, TP2: {tp2_price:.5f}")
                    
                    elif direction == "bearish":
                        if sweep_bar_actual_high is None:
                            print(f"[ERROR_SL_CALC] Bearish BOS but sweep_bar_actual_high is None. Cannot set SL. Bar: {m5_bar_time}")
                            continue # Skip this trade signal
                        # SL above the high of the M5 sweep bar + buffer
                        sl_price = sweep_bar_actual_high['high'] + (STOP_LOSS_BUFFER_PIPS * pip_size)
                        sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                        sl_pips = (sl_price - entry_price) / pip_size

                        if sl_pips < MIN_SL_PIPS:
                            print(f"[SL_ADJUST] Original Bearish SL pips {sl_pips:.1f} < Min SL pips {MIN_SL_PIPS}. Adjusting SL.")
                            sl_price = entry_price + (MIN_SL_PIPS * pip_size)
                            sl_price = round(sl_price, 5 if pip_size == 0.0001 else 3)
                            sl_pips = (sl_price - entry_price) / pip_size
                        
                        # TODO: Implement advanced TP logic from C#
                        tp1_price = entry_price - (sl_pips * pip_size) # 1:1
                        tp2_price = entry_price - (sl_pips * 2 * pip_size) # 1:2
                        tp1_price = round(tp1_price, 5 if pip_size == 0.0001 else 3)
                        tp2_price = round(tp2_price, 5 if pip_size == 0.0001 else 3)
                        print(f"[TRADE_SIM] Bearish Entry: {entry_price:.5f}, SL: {sl_price:.5f} ({sl_pips:.1f} pips), TP1: {tp1_price:.5f}, TP2: {tp2_price:.5f}")
                    
                    if sl_price is not None: # If SL was successfully calculated
                        last_trade_execution_date = m5_bar_time.date() # Mark that a trade was made today
                        print(f"[TRADE_EXECUTION] Trade logged for {direction} at {m5_bar_time}. SL pips: {sl_pips:.1f}. One trade per day rule active for {last_trade_execution_date}.")


                    # Reset sweeps to prevent re-entry on the same signal immediately
                    # A more sophisticated trade management would be needed for multiple positions or re-entries
                    global sweep_terjadi_high, sweep_terjadi_low, bos_level_to_break_high, bos_level_to_break_low
                    sweep_terjadi_high = False
                    sweep_terjadi_low = False
                    bos_level_to_break_high = None
                    bos_level_to_break_low = None
                    print(f"[STATE_RESET] Sweeps and BOS levels reset after trade signal at {m5_bar_time}.")
                    # Potentially break from inner loop for the day if only one trade per day is desired
                    # break # Uncomment if one trade per day

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
        time.sleep(1) # Respect API rate limits if on a free plan

        print("\nFetching M5 data...")
        m5_data = data_fetcher.get_historical_data(
            symbol_to_trade, "5min", 
            backtest_start_date, backtest_end_date, 
            config.TWELVE_DATA_API_KEY
        )

        if h1_data is not None and not h1_data.empty and m5_data is not None and not m5_data.empty:
            print("\nData fetched successfully. Starting strategy processing...")
            # Ensure data is UTC (already handled by data_fetcher, but good to be aware)
            # h1_data.index = h1_data.index.tz_localize('UTC') if h1_data.index.tz is None else h1_data.index.tz_convert('UTC')
            # m5_data.index = m5_data.index.tz_localize('UTC') if m5_data.index.tz is None else m5_data.index.tz_convert('UTC')
            
            process_bar_data(h1_data, m5_data, symbol_to_trade)
        else:
            print("\nFailed to fetch necessary data. Aborting backtest.") 