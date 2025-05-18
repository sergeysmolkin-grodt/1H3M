using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using cAlgo.API;
using cAlgo.API.Collections;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    public class AsianFractal
    {
        public double Level { get; set; }
        public DateTime Time { get; set; }
        public bool IsSwept { get; set; }
        public double? SweepLevel { get; set; }  // Уровень который привел к свипу
        public double? SweepExtreme { get; set; } // Экстремум бара свипа (low/high)
        public bool EntryDone { get; set; } // Был ли вход по этому фракталу
        public int? SweepBarIndex { get; set; } // Индекс бара, который сделал свип
        public int LastBosCheckBarIndex { get; set; } // Индекс последнего M3 бара, проверенного на BOS для этого фрактала
        public double? BosLevel { get; set; } // New property for BOS level
    }

    [Robot(AccessRights = AccessRights.None, AddIndicators = true)]
    public class H3M : Robot
    {
        [Parameter("Fractal Period", DefaultValue = 3)]
        public int FractalPeriod { get; set; }

        [Parameter("Risk Percent", DefaultValue = 1.0)]
        public double RiskPercent { get; set; }

        [Parameter("Manual Trend Mode", DefaultValue = ManualTrendMode.Auto)]
        public ManualTrendMode TrendMode { get; set; }

        [Parameter("Broker Min Stop Level Pips", DefaultValue = 2.0, MinValue = 0.0)]
        public double BrokerMinStopLevelPips { get; set; }

        private const double _minRR = 1.3;
        private const double _maxRR = 5.0;
        private const int H1_TP_FRACTAL_PERIOD = 10; // New constant for H1 TP fractals
        private const double ASIAN_FRACTAL_SL_FIXED_BUFFER_PIPS = 1.5; // Fixed buffher for Asian fractal SL

        private Fractals _fractals;
        private List<AsianFractal> _asianFractals = new List<AsianFractal>();
        private DateTime _lastAsianSessionCheck = DateTime.MinValue;
        private bool _isAsianSession = false;
        private double? _currentFractalLevel = null;
        private Bars _m3Bars;
        private Bars _h1Bars;
        private DateTime _lastH1BarTime = DateTime.MinValue;
        private static readonly int AsiaStartHour = 0;
        private static readonly int AsiaEndHour = 9;
        private DateTime _lastTradeDate = DateTime.MinValue;
        private bool _loggedSpecificBarDataThisInstance = false;
        private DateTime _debugSpecificTimestamp = DateTime.MinValue;
        private HashSet<DateTime> _loggedOHLCBarsForTargetDate = new HashSet<DateTime>();

        // New fields for daily setup
        private TrendContext _dailyTrendContext = TrendContext.Neutral;
        private DateTime _lastDailySetupDate = DateTime.MinValue;
        private bool _asianFractalsFoundForToday = false; // New flag

        private StreamWriter _chartDataWriter;
        private string _csvFilePath;
        private static readonly string CSV_HEADER = "Timestamp;EventType;H1_Open;H1_High;H1_Low;H1_Close;Price1;Price2;Price3;TradeType;Notes";

        protected override void OnStart()
        {
            _fractals = Indicators.Fractals(Bars, FractalPeriod);
            _m3Bars = MarketData.GetBars(TimeFrame.Minute3);
            _h1Bars = MarketData.GetBars(TimeFrame.Hour);

            _debugSpecificTimestamp = new DateTime(2025, 5, 14, 6, 15, 0, DateTimeKind.Utc);
            _loggedOHLCBarsForTargetDate.Clear();

            try
            {
                string robotName = GetType().Name;
                string logsDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "cAlgo", "Robots", "Logs", robotName);
                Directory.CreateDirectory(logsDirectory);
                _csvFilePath = Path.Combine(logsDirectory, $"{robotName}_ChartData_{SymbolName.Replace("/", "")}_{Server.Time:yyyyMMddHHmmss}.csv");
                
                _chartDataWriter = new StreamWriter(_csvFilePath, false, System.Text.Encoding.UTF8);
                _chartDataWriter.WriteLine(CSV_HEADER);
                Print($"Chart data logging started. File: {_csvFilePath}");
            }
            catch (Exception ex)
            {
                Print($"Error initializing chart data logger: {ex.Message}");
                _chartDataWriter = null;
            }
        }

        private double? FindNearestFractalLevel(TradeType tradeType, double currentPrice)
        {
            double? nearestLevel = null;
            double minDistance = double.MaxValue;

            for (int i = 2; i < Bars.Count - 2; i++)
            {
                if (tradeType == TradeType.Buy)
                {
                    if (!double.IsNaN(_fractals.UpFractal[i]))
                    {
                        var distance = _fractals.UpFractal[i] - currentPrice;
                        if (distance > 0 && distance < minDistance)
                        {
                            minDistance = distance;
                            nearestLevel = _fractals.UpFractal[i];
                        }
                    }
                }
                else
                {
                    if (!double.IsNaN(_fractals.DownFractal[i]))
                    {
                        var distance = currentPrice - _fractals.DownFractal[i];
                        if (distance > 0 && distance < minDistance)
                        {
                            minDistance = distance;
                            nearestLevel = _fractals.DownFractal[i];
                        }
                    }
                }
            }

            return nearestLevel;
        }

        private double? FindNextFractalLevel(TradeType tradeType, double currentPrice, double firstFractalLevel)
        {
            double? nextLevel = null;
            double minDistance = double.MaxValue;

            for (int i = 2; i < Bars.Count - 2; i++)
            {
                if (tradeType == TradeType.Buy)
                {
                    if (!double.IsNaN(_fractals.UpFractal[i]))
                    {
                        var distance = _fractals.UpFractal[i] - currentPrice;
                        if (distance > 0 && _fractals.UpFractal[i] > firstFractalLevel && distance < minDistance)
                        {
                            minDistance = distance;
                            nextLevel = _fractals.UpFractal[i];
                        }
                    }
                }
                else
                {
                    if (!double.IsNaN(_fractals.DownFractal[i]))
                    {
                        var distance = currentPrice - _fractals.DownFractal[i];
                        if (distance > 0 && _fractals.DownFractal[i] < firstFractalLevel && distance < minDistance)
                        {
                            minDistance = distance;
                            nextLevel = _fractals.DownFractal[i];
                        }
                    }
                }
            }

            return nextLevel;
        }

        private double CalculatePositionSize(double stopLossPips)
        {
            var accountBalance = Account.Balance;
            var riskAmount = accountBalance * (RiskPercent / 100.0);
            var pipValue = Symbol.PipValue;
            var stopLossAmount = stopLossPips * pipValue;
            
            if (stopLossAmount == 0) return 0;
            
            // Calculate raw position size
            var rawPositionSize = riskAmount / stopLossAmount;
            
            // Round to valid volume step
            var volumeStep = Symbol.VolumeInUnitsStep;
            var normalizedVolume = Math.Floor(rawPositionSize / volumeStep) * volumeStep;
            
            // Ensure volume is within allowed range
            normalizedVolume = Math.Max(Symbol.VolumeInUnitsMin, normalizedVolume);
            normalizedVolume = Math.Min(Symbol.VolumeInUnitsMax, normalizedVolume);
            
            return normalizedVolume;
        }

        // --- Find nearest H1 fractal for take profit ---
        private double? FindNearestH1FractalForTP(TradeType tradeType, double entryPrice)
        {
            double? nearestLevel = null;
            double minDistance = double.MaxValue;
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, H1_TP_FRACTAL_PERIOD);
            
            for (int i = H1_TP_FRACTAL_PERIOD; i < h1Bars.Count - H1_TP_FRACTAL_PERIOD; i++)
            {
                if (tradeType == TradeType.Buy && !double.IsNaN(hourlyFractals.UpFractal[i]))
                {
                    var level = hourlyFractals.UpFractal[i];
                    DebugLog($"[TP_FIND_NEAREST_RAW_UP] i={i}, Raw H1 UpFractal: {level:F5} at {h1Bars.OpenTimes[i]}, Entry: {entryPrice:F5}"); 
                    var distance = level - entryPrice;
                    if (distance > 0 && distance < minDistance)
                    {
                        minDistance = distance;
                        nearestLevel = level;
                        DebugLog($"[TP_FIND_NEAREST_CANDIDATE_UP] Found NEW nearest H1 UpFractal: {nearestLevel:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
                if (tradeType == TradeType.Sell && !double.IsNaN(hourlyFractals.DownFractal[i]))
                {
                    var level = hourlyFractals.DownFractal[i];
                    DebugLog($"[TP_FIND_NEAREST_RAW_DOWN] i={i}, Raw H1 DownFractal: {level:F5} at {h1Bars.OpenTimes[i]}, Entry: {entryPrice:F5}");
                    var distance = entryPrice - level;
                    if (distance > 0 && distance < minDistance)
                    {
                        minDistance = distance;
                        nearestLevel = level;
                        DebugLog($"[TP_FIND_NEAREST_CANDIDATE_DOWN] Found NEW nearest H1 DownFractal: {nearestLevel:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
            }
            return nearestLevel;
        }

        // --- Find next H1 frкactal after the nearest one ---
        private double? TryFindNextH1Fractal(TradeType tradeType, double entryPrice, double firstFractalLevel)
        {
            double? nextLevel = null;
            double minDistance = double.MaxValue;
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, H1_TP_FRACTAL_PERIOD);
            
            for (int i = H1_TP_FRACTAL_PERIOD; i < h1Bars.Count - H1_TP_FRACTAL_PERIOD; i++)
            {
                if (tradeType == TradeType.Buy && !double.IsNaN(hourlyFractals.UpFractal[i]))
                {
                    var level = hourlyFractals.UpFractal[i];
                    DebugLog($"[TP_FIND_NEXT_RAW_UP] i={i}, Raw H1 UpFractal: {level:F5} at {h1Bars.OpenTimes[i]}, Entry: {entryPrice:F5}, FirstTP: {firstFractalLevel:F5}");
                    var distance = level - entryPrice;
                    if (distance > 0 && level > firstFractalLevel && distance < minDistance) 
                    {
                        minDistance = distance;
                        nextLevel = level;
                        DebugLog($"[TP_FIND_NEXT_CANDIDATE_UP] Found NEW next H1 UpFractal: {nextLevel:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
                if (tradeType == TradeType.Sell && !double.IsNaN(hourlyFractals.DownFractal[i]))
                {
                    var level = hourlyFractals.DownFractal[i];
                    DebugLog($"[TP_FIND_NEXT_RAW_DOWN] i={i}, Raw H1 DownFractal: {level:F5} at {h1Bars.OpenTimes[i]}, Entry: {entryPrice:F5}, FirstTP: {firstFractalLevel:F5}");
                    var distance = entryPrice - level;
                    if (distance > 0 && level < firstFractalLevel && distance < minDistance) 
                    {
                        minDistance = distance;
                        nextLevel = level;
                        DebugLog($"[TP_FIND_NEXT_CANDIDATE_DOWN] Found NEW next H1 DownFractal: {nextLevel:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
            }
            return nextLevel;
        }

        private (double? takeProfitPrice, double rr) CalculateTakeProfit(TradeType tradeType, double entryPrice, double stopLossPrice)
        {
            DebugLog($"[TP_DEBUG] CalculateTakeProfit called. TradeType: {tradeType}, Entry: {entryPrice:F5}, SL: {stopLossPrice:F5}");
            var stopLossDistance = Math.Abs(entryPrice - stopLossPrice);
            if (stopLossDistance == 0) 
            {
                DebugLog("[TP_DEBUG] CalculateTakeProfit: Stop Loss distance is 0. Cannot calculate RR.");
                return (null, 0);
            }
            DebugLog($"[TP_DEBUG] Stop Loss Distance (Pips): {stopLossDistance/Symbol.PipSize:F1}");

            double? candidateTp = FindNearestH1FractalForTP(tradeType, entryPrice);
            int attempts = 0;
            const int maxAttempts = 10; // Prevent infinite loops, look for at most 10 fractals
            double lastConsideredRr = 0;

            while (candidateTp.HasValue && attempts < maxAttempts)
            {
                attempts++;
                DebugLog($"[TP_DEBUG] Attempt {attempts}: Evaluating TP Candidate ({candidateTp.Value:F5})");
                double currentRr = Math.Abs(candidateTp.Value - entryPrice) / stopLossDistance;
                lastConsideredRr = currentRr; // Store for final reject log
                DebugLog($"[TP_DEBUG] RR for Candidate ({candidateTp.Value:F5}): {currentRr:F2}");

                if (currentRr >= _minRR && currentRr <= _maxRR)
                {
                    DebugLog($"[TP_DEBUG] SUITABLE TP found: {candidateTp.Value:F5} with RR {currentRr:F2}. Using it.");
                    return (candidateTp.Value, currentRr);
                }
                
                DebugLog($"[TP_DEBUG] TP Candidate {candidateTp.Value:F5} (RR {currentRr:F2}) is NOT SUITABLE (Range: {_minRR:F2}-{_maxRR:F2}). Searching for next.");
                candidateTp = TryFindNextH1Fractal(tradeType, entryPrice, candidateTp.Value); 
            }

            if (attempts == maxAttempts && candidateTp.HasValue) 
            {
                DebugLog($"[TP_DEBUG] Max attempts ({maxAttempts}) reached trying to find suitable TP. Last candidate {candidateTp.Value:F5} (RR {lastConsideredRr:F2}) was not suitable.");
            } else if (!candidateTp.HasValue && attempts > 0) 
            {
                 DebugLog($"[TP_DEBUG] No more H1 fractals found to check after {attempts} attempts. Last RR considered: {lastConsideredRr:F2}.");
            } else if (attempts == 0 && !candidateTp.HasValue) {
                 DebugLog($"[TP_DEBUG] No H1 fractals found at all by FindNearestH1FractalForTP.");
            }
            
            DebugLog($"[TP_DEBUG] No suitable H1 fractal TP found that meets RR criteria.");
            return (null, lastConsideredRr); // Return last RR for more informative reject log in EnterPosition
        }

        private double CalculateStopLoss(TradeType tradeType, double asianFractalLevelToPlaceSLBehind)
        {
            double stopLossPrice;
            if (tradeType == TradeType.Buy)
            {
                stopLossPrice = asianFractalLevelToPlaceSLBehind - ASIAN_FRACTAL_SL_FIXED_BUFFER_PIPS * Symbol.PipSize;
                DebugLog($"[DEBUG_CALC_SL] Buy. AsianFractal Level: {asianFractalLevelToPlaceSLBehind.ToString(CultureInfo.InvariantCulture)}. Buffer: {ASIAN_FRACTAL_SL_FIXED_BUFFER_PIPS.ToString(CultureInfo.InvariantCulture)} pips. Final SL: {stopLossPrice.ToString(CultureInfo.InvariantCulture)}");
            }
            else // Sell
            {
                stopLossPrice = asianFractalLevelToPlaceSLBehind + ASIAN_FRACTAL_SL_FIXED_BUFFER_PIPS * Symbol.PipSize;
                DebugLog($"[DEBUG_CALC_SL] Sell. AsianFractal Level: {asianFractalLevelToPlaceSLBehind.ToString(CultureInfo.InvariantCulture)}. Buffer: {ASIAN_FRACTAL_SL_FIXED_BUFFER_PIPS.ToString(CultureInfo.InvariantCulture)} pips. Final SL: {stopLossPrice.ToString(CultureInfo.InvariantCulture)}");
            }
            return stopLossPrice;
        }

        private void EnterPosition(TradeType tradeType, double entryPrice, AsianFractal fractal)
        {
            DebugLog($"[DEBUG_ENTER_POS] Entered. TradeType: {tradeType}, EntryPrice: {entryPrice.ToString(CultureInfo.InvariantCulture)}, Fractal Level: {fractal.Level.ToString(CultureInfo.InvariantCulture)}");

            double slPriceCalculated = CalculateStopLoss(tradeType, fractal.Level);
            double slPriceNormalized = NormalizePriceManually(slPriceCalculated); // Using manual normalization
            DebugLog($"[DEBUG_ENTER_POS] SL Calculated: {slPriceCalculated.ToString(CultureInfo.InvariantCulture)}, SL Normalized (Manual): {slPriceNormalized.ToString(CultureInfo.InvariantCulture)}");

            (double? takeProfitPriceCalculated, double rr) tpResult = CalculateTakeProfit(tradeType, entryPrice, slPriceNormalized);
            DebugLog($"[DEBUG_ENTER_POS] TP Calculated: {tpResult.takeProfitPriceCalculated?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}, RR: {tpResult.rr.ToString(CultureInfo.InvariantCulture)}");

            if (!tpResult.takeProfitPriceCalculated.HasValue || tpResult.rr < _minRR || tpResult.rr > _maxRR)
            {
                DebugLog($"[DEBUG_ENTER_POS] TP/RR criteria not met. TP: {tpResult.takeProfitPriceCalculated?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}, RR: {tpResult.rr.ToString(CultureInfo.InvariantCulture)}. MinRR: {_minRR}, MaxRR: {_maxRR}");
                LogChartEvent(Server.Time, "TP_RR_REJECT", price1: entryPrice, price2: slPriceNormalized, price3: tpResult.takeProfitPriceCalculated, tradeType: tradeType.ToString(), notes: $"Calculated RR: {tpResult.rr.ToString("F2", CultureInfo.InvariantCulture)}");
                fractal.EntryDone = true; 
                return;
            }

            double tpPriceNormalized = NormalizePriceManually(tpResult.takeProfitPriceCalculated.Value); // Using manual normalization
            DebugLog($"[DEBUG_ENTER_POS] TP Normalized (Manual): {tpPriceNormalized.ToString(CultureInfo.InvariantCulture)}");

            // Adjust SL and TP based on BrokerMinStopLevelPips parameter
            double stopLevelPips = BrokerMinStopLevelPips;
            double minStopDistance = stopLevelPips * Symbol.PipSize;
            DebugLog($"[DEBUG_ENTER_POS] Broker Min Stop Level: {stopLevelPips} pips, MinStopDistance: {minStopDistance.ToString(CultureInfo.InvariantCulture)}");

            if (tradeType == TradeType.Buy)
            {
                if (entryPrice - slPriceNormalized < minStopDistance)
                {
                    slPriceNormalized = NormalizePriceManually(entryPrice - minStopDistance);
                    DebugLog($"[DEBUG_ENTER_POS] SL for BUY adjusted due to StopLevel. New SL: {slPriceNormalized.ToString(CultureInfo.InvariantCulture)}");
                }
                if (tpPriceNormalized - entryPrice < minStopDistance)
                {
                    tpPriceNormalized = NormalizePriceManually(entryPrice + minStopDistance);
                    DebugLog($"[DEBUG_ENTER_POS] TP for BUY adjusted due to StopLevel. New TP: {tpPriceNormalized.ToString(CultureInfo.InvariantCulture)}");
                }
            }
            else // Sell
            {
                if (slPriceNormalized - entryPrice < minStopDistance)
                {
                    slPriceNormalized = NormalizePriceManually(entryPrice + minStopDistance);
                    DebugLog($"[DEBUG_ENTER_POS] SL for SELL adjusted due to StopLevel. New SL: {slPriceNormalized.ToString(CultureInfo.InvariantCulture)}");
                }
                if (entryPrice - tpPriceNormalized < minStopDistance)
                {
                    tpPriceNormalized = NormalizePriceManually(entryPrice - minStopDistance);
                    DebugLog($"[DEBUG_ENTER_POS] TP for SELL adjusted due to StopLevel. New TP: {tpPriceNormalized.ToString(CultureInfo.InvariantCulture)}");
                }
            }
            
            // Recalculate RR with potentially adjusted SL/TP
            // Important: This might change the RR and it could fall out of the desired _minRR/_maxRR range.
            // For now, we will proceed with adjusted SL/TP even if RR changes. A more advanced logic could re-evaluate or reject the trade here.
            double adjustedStopLossPips = Math.Abs(entryPrice - slPriceNormalized) / Symbol.PipSize;
            double adjustedTakeProfitPips = Math.Abs(tpPriceNormalized - entryPrice) / Symbol.PipSize;
            double newRR = (adjustedStopLossPips > 0) ? adjustedTakeProfitPips / adjustedStopLossPips : 0;
            DebugLog($"[DEBUG_ENTER_POS] SL/TP potentially adjusted. New SL pips: {adjustedStopLossPips}, New TP pips: {adjustedTakeProfitPips}, New RR: {newRR.ToString(CultureInfo.InvariantCulture)}");

            double positionSize = CalculatePositionSize(adjustedStopLossPips);
            DebugLog($"[DEBUG_ENTER_POS] Position Size Calculated: {positionSize.ToString(CultureInfo.InvariantCulture)} for SL pips: {adjustedStopLossPips.ToString(CultureInfo.InvariantCulture)}");

            if (positionSize <= 0)
            {
                DebugLog($"[DEBUG_ENTER_POS] Invalid position size: {positionSize}. SL pips: {adjustedStopLossPips}");
                LogChartEvent(Server.Time, "POS_SIZE_ERROR", price1: entryPrice, price2: slPriceNormalized, tradeType: tradeType.ToString(), notes: $"Calculated Position Size: {positionSize}");
                fractal.EntryDone = true; 
                return;
            }

            string label = $"H3M_{tradeType}_{Server.Time:HHmm}";
            var result = ExecuteMarketOrder(tradeType, Symbol.Name, positionSize, label, slPriceNormalized, tpPriceNormalized, comment: "H3M Trade");

            if (result.IsSuccessful)
            {
                fractal.EntryDone = true;
                _lastTradeDate = Server.Time.Date;
                DebugLog($"[TRADE_OPEN] {tradeType} order successful. Entry: {result.Position.EntryPrice}, Actual SL: {result.Position.StopLoss}, Actual TP: {result.Position.TakeProfit}. \n" +
                         $"Attempted SL: {slPriceNormalized.ToString(CultureInfo.InvariantCulture)} (Raw: {slPriceCalculated.ToString(CultureInfo.InvariantCulture)}), \n" +
                         $"Attempted TP: {tpPriceNormalized.ToString(CultureInfo.InvariantCulture)} (Raw: {tpResult.takeProfitPriceCalculated?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}), \n" +
                         $"Intended RR: {tpResult.rr.ToString(CultureInfo.InvariantCulture)}. Size: {positionSize.ToString(CultureInfo.InvariantCulture)}");
                
                LogChartEvent(result.Position.EntryTime, "TRADE_ENTRY", 
                              price1: result.Position.EntryPrice, 
                              price2: result.Position.StopLoss, 
                              price3: result.Position.TakeProfit, 
                              tradeType: tradeType.ToString(), 
                              notes: $"Intended RR: {tpResult.rr.ToString("F2", CultureInfo.InvariantCulture)}; Initial SL Calc: {slPriceNormalized.ToString(CultureInfo.InvariantCulture)}; Initial TP Calc: {tpPriceNormalized.ToString(CultureInfo.InvariantCulture)}; Label: {label}");
            }
            else
            {
                DebugLog($"[DEBUG_ENTER_POS] Failed to open position. Error: {result.Error}");
                LogChartEvent(Server.Time, "TRADE_FAILED", 
                              price1: entryPrice, 
                              price2: slPriceNormalized, 
                              price3: tpPriceNormalized, 
                              tradeType: tradeType.ToString(), 
                              notes: $"Error: {result.Error?.ToString() ?? "Unknown"}. SL Raw: {slPriceCalculated.ToString(CultureInfo.InvariantCulture)}, TP Raw: {tpResult.takeProfitPriceCalculated?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}");
            }
        }

        private bool IsAsianSession()
        {
            var serverTime = Server.Time;
            var hour = serverTime.Hour;
            
            // Asian session: 00:00 - 09:00 UTC
            return hour >= 0 && hour < 9;
        }

        private bool IsLondonOrFrankfurtSession(DateTime time)
        {
            // Frankfurt: 09:00-10:00 UTC+3
            // London:    10:00-15:00 UTC+3
            // Assumes 'time' parameter is UTC+3
            return IsInFrankfurtSession(time) || IsInLondonSession(time);
        }

        private void CheckAsianSession()
        {
            var currentTime = Server.Time;
            if (currentTime.Date != _lastAsianSessionCheck.Date)
            {
                _isAsianSession = IsAsianSession();
                _lastAsianSessionCheck = currentTime;
                _currentFractalLevel = null;
            }
        }

        private void FindFractals()
        {
            if (!_isAsianSession) return;

            var trendContext = DetermineTrendContext();
            
            if (trendContext == TrendContext.Bearish)
            {
                for (int i = 2; i < Bars.Count - 2; i++)
                {
                    if (!double.IsNaN(_fractals.UpFractal[i]))
                    {
                        _currentFractalLevel = _fractals.UpFractal[i];
                        break;
                    }
                }
            }
            else if (trendContext == TrendContext.Bullish)
            {
                for (int i = 2; i < Bars.Count - 2; i++)
                {
                    if (!double.IsNaN(_fractals.DownFractal[i]))
                    {
                        _currentFractalLevel = _fractals.DownFractal[i];
                        break;
                    }
                }
            }
        }

        private bool IsFractalSwept()
        {
            if (_currentFractalLevel == null) return false;

            var trendContext = DetermineTrendContext();
            var currentPrice = Bars.ClosePrices.Last(0);

            if (trendContext == TrendContext.Bearish)
            {
                // For bearish trend, check if price went above the upper fractal
                return currentPrice > _currentFractalLevel.Value;
            }
            else if (trendContext == TrendContext.Bullish)
            {
                // For bullish trend, check if price went below the lower fractal
                return currentPrice < _currentFractalLevel.Value;
            }

            return false;
        }

        private TrendContext SimpleTrendContext()
        {
            if (_h1Bars == null || _h1Bars.Count < 25) return TrendContext.Neutral;
            
            int last = _h1Bars.Count - 1;
            int bullish = 0, bearish = 0;
            
            // Счетчик бычьих/медвежьих баров
            for (int i = 0; i < 25; i++)
            {
                if (_h1Bars.ClosePrices[last - i] > _h1Bars.OpenPrices[last - i]) bullish++;
                else if (_h1Bars.ClosePrices[last - i] < _h1Bars.OpenPrices[last - i]) bearish++;
            }
            
            // Проверка на наличие сильного импульса (более 40 пипсов за последние 5 баров)
            double recentMovement = _h1Bars.ClosePrices[last] - _h1Bars.ClosePrices[last - 5];
            bool strongBullishImpulse = recentMovement > Symbol.PipSize * 40;
            bool strongBearishImpulse = recentMovement < -Symbol.PipSize * 40;
            
            // Анализ структуры (HH-HL или LL-LH)
            bool hasHigherHighs = false;
            bool hasHigherLows = false;
            bool hasLowerLows = false;
            bool hasLowerHighs = false;
            
            double prevHigh = _h1Bars.HighPrices[last - 10];
            double prevLow = _h1Bars.LowPrices[last - 10];
            
            for (int i = 9; i >= 0; i--)
            {
                if (_h1Bars.HighPrices[last - i] > prevHigh)
                {
                    hasHigherHighs = true;
                }
                else if (_h1Bars.HighPrices[last - i] < prevHigh)
                {
                    hasLowerHighs = true;
                }
                
                if (_h1Bars.LowPrices[last - i] > prevLow)
                {
                    hasHigherLows = true;
                }
                else if (_h1Bars.LowPrices[last - i] < prevLow)
                {
                    hasLowerLows = true;
                }
                
                prevHigh = _h1Bars.HighPrices[last - i];
                prevLow = _h1Bars.LowPrices[last - i];
            }
            
            // Принятие решения о тренде
            if ((bullish > bearish + 5) || (hasHigherHighs && hasHigherLows) || strongBullishImpulse)
            {
                DebugLog($"[DEBUG] Определен БЫЧИЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, HH={hasHigherHighs}, HL={hasHigherLows}, импульс={recentMovement/Symbol.PipSize:F1} пипсов");
                return TrendContext.Bullish;
            }
            
            if ((bearish > bullish + 5) || (hasLowerLows && hasLowerHighs) || strongBearishImpulse)
            {
                DebugLog($"[DEBUG] Определен МЕДВЕЖИЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, LL={hasLowerLows}, LH={hasLowerHighs}, импульс={recentMovement/Symbol.PipSize:F1} пипсов");
                return TrendContext.Bearish;
            }
            
            DebugLog($"[DEBUG] Определен НЕЙТРАЛЬНЫЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, движение={recentMovement/Symbol.PipSize:F1} пипсов");
            return TrendContext.Neutral;
        }

        private TrendContext DetermineTrendContext()
        {
            // Если выбран ручной режим, возвращаем выбранный тренд
            if (TrendMode == ManualTrendMode.Bullish)
                return TrendContext.Bullish;
            if (TrendMode == ManualTrendMode.Bearish)
                return TrendContext.Bearish;
            // Auto — старая логика
            if (_h1Bars == null || _h1Bars.Count < 10) return TrendContext.Neutral;

            int last = _h1Bars.Count - 1;
            
            // Stronger bias toward trend detection by looking at last 12 H1 bars
            int bullishBars = 0;
            int bearishBars = 0;
            double priceDirectionChange = 0; // Calculated as close-to-close price movement
            
            // Check overall H1 direction over more bars to detect obvious trends
            for (int i = 0; i < Math.Min(12, _h1Bars.Count - 1); i++)
            {
                if (_h1Bars.ClosePrices[last - i] > _h1Bars.OpenPrices[last - i])
                    bullishBars++;
                else if (_h1Bars.ClosePrices[last - i] < _h1Bars.OpenPrices[last - i])
                    bearishBars++;
                    
                if (i < 10) // Check direction over last 10 bars
                    priceDirectionChange += (_h1Bars.ClosePrices[last - i] - _h1Bars.ClosePrices[last - i - 1]);
            }
            
            double netMovement = _h1Bars.ClosePrices[last] - _h1Bars.ClosePrices[last - Math.Min(8, _h1Bars.Count - 1)];
            bool strongDirectionalMove = Math.Abs(netMovement) > Symbol.PipSize * 20; // At least 20 pips movement
            
            DebugLog($"[DEBUG] Trend Analysis: Bulls/Bears={bullishBars}/{bearishBars}, Net Movement={netMovement / Symbol.PipSize:F1} pips");

            // Check for strong and obvious trends
            if (bullishBars >= 7 && priceDirectionChange > 0)
            {
                DebugLog("[DEBUG] Очевидный бычий тренд: преобладание бычьих баров и положительное движение цены");
                return TrendContext.Bullish;
            }
            
            if (bearishBars >= 7 && priceDirectionChange < 0)
            {
                DebugLog("[DEBUG] Очевидный медвежий тренд: преобладание медвежьих баров и отрицательное движение цены");
                return TrendContext.Bearish;
            }
            
            // Проверка на сильное однонаправленное движение
            if (strongDirectionalMove)
            {
                if (netMovement > 0)
                {
                    DebugLog($"[DEBUG] Бычий контекст: сильное направленное движение вверх на {netMovement / Symbol.PipSize:F1} пипсов");
                    return TrendContext.Bullish;
                }
                else
                {
                    DebugLog($"[DEBUG] Медвежий контекст: сильное направленное движение вниз на {Math.Abs(netMovement) / Symbol.PipSize:F1} пипсов");
                    return TrendContext.Bearish;
                }
            }
            
            // Находим локальные минимумы и максимумы за последние 10 баров
            var localHighs = new List<double>();
            var localLows = new List<double>();
            
            for (int i = 2; i < Math.Min(10, _h1Bars.Count - 2); i++)
            {
                // Локальный максимум
                if (_h1Bars.HighPrices[last - i] > _h1Bars.HighPrices[last - i + 1] && 
                    _h1Bars.HighPrices[last - i] > _h1Bars.HighPrices[last - i - 1])
                {
                    localHighs.Add(_h1Bars.HighPrices[last - i]);
                }
                
                // Локальный минимум
                if (_h1Bars.LowPrices[last - i] < _h1Bars.LowPrices[last - i + 1] && 
                    _h1Bars.LowPrices[last - i] < _h1Bars.LowPrices[last - i - 1])
                {
                    localLows.Add(_h1Bars.LowPrices[last - i]);
                }
            }
            
            // Проверяем последние 3 бара на импульс
            bool recentBullishMomentum = true;
            bool recentBearishMomentum = true;
            
            for (int i = 1; i < 4; i++)
            {
                if (_h1Bars.LowPrices[last - i + 1] <= _h1Bars.LowPrices[last - i])
                    recentBullishMomentum = false;
                if (_h1Bars.HighPrices[last - i + 1] >= _h1Bars.HighPrices[last - i])
                    recentBearishMomentum = false;
            }
            
            // Проверяем структуру HH/HL для бычьего тренда
            bool hasHigherHighs = localHighs.Count >= 2 && localHighs[0] > localHighs[localHighs.Count - 1];
            bool hasHigherLows = localLows.Count >= 2 && localLows[0] > localLows[localLows.Count - 1];
            
            // Проверяем структуру LL/LH для медвежьего тренда
            bool hasLowerLows = localLows.Count >= 2 && localLows[0] < localLows[localLows.Count - 1];
            bool hasLowerHighs = localHighs.Count >= 2 && localHighs[0] < localHighs[localHighs.Count - 1];
            
            double currentPrice = _h1Bars.ClosePrices[last];
            
            DebugLog($"[DEBUG] Structure: HH={hasHigherHighs}, HL={hasHigherLows}, LL={hasLowerLows}, LH={hasLowerHighs}");
            DebugLog($"[DEBUG] Recent Momentum: Bull={recentBullishMomentum}, Bear={recentBearishMomentum}");
            
            // Определяем тренд на основе структуры и импульса
            if ((hasHigherHighs || hasHigherLows) && bullishBars >= 3 || recentBullishMomentum)
            {
                DebugLog("[DEBUG] Bullish Context: Higher Highs/Lows or Strong Momentum");
                return TrendContext.Bullish;
            }
            
            if ((hasLowerLows || hasLowerHighs) && bearishBars >= 3 || recentBearishMomentum)
            {
                DebugLog("[DEBUG] Bearish Context: Lower Lows/Highs or Strong Momentum");
                return TrendContext.Bearish;
            }
            
            // Если нет явного тренда, проверяем последние 3 бара
            var last3BarsDirection = _h1Bars.ClosePrices[last] - _h1Bars.ClosePrices[last - 3];
            if (Math.Abs(last3BarsDirection) > Symbol.PipSize * 10) // Если движение больше 10 пипсов
            {
                if (last3BarsDirection > 0)
                {
                    DebugLog("[DEBUG] Bullish Context: Strong Recent Movement");
                    return TrendContext.Bullish;
                }
                if (last3BarsDirection < 0)
                {
                    DebugLog("[DEBUG] Bearish Context: Strong Recent Movement");
                    return TrendContext.Bearish;
                }
            }
            
            DebugLog("[DEBUG] No Clear Trend Context");
            return TrendContext.Neutral;
        }

        private void DebugLog(string message)
        {
            var time = Server.Time; // Current server time for context

            // Special handling for the target debugging date
            if (time.Date == new DateTime(2025, 5, 14))
            {
                // For the target date, print messages that have any of our key debug tags
                // or are specifically marked for this date.
                bool shouldPrintOnTargetDate =
                    message.Contains("[DEBUG]") || // General debug prefix
                    message.Contains("[SWEEP_") || // Covers [SWEEP_BULL], [SWEEP_BEAR]
                    message.Contains("[BOS_") ||   // Covers [BOS_DEBUG_...], [BOS_SUCCESS_...], [BOS_REJECT_...]
                    message.Contains("[TP_DEBUG]") ||
                    message.Contains("[TP_REJECT]") ||
                    message.Contains("[TRADE_") || // Covers [TRADE_OPEN], [TRADE_FAIL]
                    message.Contains("[ERROR_") || // Covers [ERROR_ENTRY]
                    message.Contains("[VOL_REJECT]") ||
                    message.Contains("[SL_ADJUST]") ||
                    message.Contains("[DEBUG_OHLC_M3]") || // Specific M3 OHLC logging for 06:03, 06:15
                    message.Contains("[DEBUG_OHLC_BAR_SPECIFIC_FOR_BOS_CHECK]") || // Specific OHLC from Is3mStructureBreak
                    message.Contains("[USER_TARGET_LOG 14.05.2025]") || // User's explicit tag
                    message.Contains("Азиатский фрактал") || // Fractal finding logs
                    message.Contains("Найден") || // Fractal finding logs
                    message.Contains("Нет очевидного тренда") ||
                    message.Contains("НОВЫЙ ТИК") ||
                    message.Contains("КОНЕЦ ТИКА") ||
                    message.Contains("Текущий тренд") ||
                    message.Contains("Уже была сделка сегодня") ||
                    message.Contains("Поиск фракталов") ||
                    message.Contains("Проверка свипа фракталов");

                if (shouldPrintOnTargetDate)
                {
                    Print(message);
                }
                return; // Explicitly return to process logs only based on above for this date
            }

            // Default behavior for other dates: suppress logs unless a global debug flag is enabled (not implemented here)
            // For now, this means logs will only appear for 2025-05-14 based on the logic above.
        }

        protected override void OnTick()
        {
            _loggedSpecificBarDataThisInstance = false; // Reset per tick

            // --- Daily Setup: Trend and Asian Fractals ---
            if (Server.Time.Date != _lastDailySetupDate)
            {
                _lastDailySetupDate = Server.Time.Date;
                _asianFractals.Clear(); 
                _asianFractalsFoundForToday = false; // Reset flag for the new day

                DebugLog($"[DAILY_SETUP] Performing daily setup for {Server.Time.Date:yyyy-MM-dd}");

                if (!IsStrongTrend(out _dailyTrendContext))
                {
                    DebugLog($"[DAILY_SETUP] No strong trend identified for {Server.Time.Date:yyyy-MM-dd}. Bot will be inactive today. Trend found: {_dailyTrendContext}");
                }
                else
                {
                    DebugLog($"[DAILY_SETUP] Strong trend for {Server.Time.Date:yyyy-MM-dd} is: {_dailyTrendContext}.");
                    // We no longer call FindAsianSessionFractals here immediately.
                }
            }

            // If no trend established for the day, or it's neutral, do nothing further.
            if (_dailyTrendContext == TrendContext.Neutral)
            {
                // Minimal logging to avoid spam, or just return. 
                // For 14.05, if it becomes Neutral, this log is important.
                if (Server.Time.Date == new DateTime(2025,5,14) && Server.Time.Minute % 15 == 0 && Server.Time.Second < 5) // Log periodically on target date
                {
                     DebugLog($"[DEBUG_ONTICK] Daily trend is Neutral for {Server.Time.Date:yyyy-MM-dd}. No trading actions.");
                }
                return; 
            }
            
            // Original H1 bar logging for chart plotter can remain if needed outside daily setup,
            // but ensure it doesn't interfere with the new daily logic.
            if (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime != _lastH1BarTime)
            {
                var h1Bar = _h1Bars.Last(0);
                LogChartEvent(h1Bar.OpenTime, "H1_BAR", h1Open: h1Bar.Open, h1High: h1Bar.High, h1Low: h1Bar.Low, h1Close: h1Bar.Close);
                _lastH1BarTime = h1Bar.OpenTime; 

                // --- Attempt to find Asian Fractals on new H1 bar if conditions met ---
                if (_dailyTrendContext != TrendContext.Neutral && 
                    !_asianFractalsFoundForToday && 
                    Server.Time.Date == _lastDailySetupDate && // Ensure it's for the current day's setup
                    Server.Time.Hour >= AsiaStartHour && Server.Time.Hour < AsiaEndHour)
                {
                    DebugLog($"[DEBUG_ONTICK] New H1 bar ({h1Bar.OpenTime:HH:mm}). Attempting to find Asian fractals for {_dailyTrendContext} trend.");
                    FindAsianSessionFractals(_dailyTrendContext);
                    if (_asianFractals.Count > 0)
                    {
                        DebugLog($"[DEBUG_ONTICK] Asian fractals found and loaded: {_asianFractals.Count}");
                        _asianFractalsFoundForToday = true; // Mark as found
                    }
                    else
                    {
                        DebugLog($"[DEBUG_ONTICK] No Asian fractals found on this attempt ({h1Bar.OpenTime:HH:mm}). Will retry on next H1 bar within Asian session.");
                        // _asianFractalsFoundForToday remains false, so we will try again on next H1 bar in Asian session.
                    }
                }
            }

            // Check if a trade has already been executed today
            if (Positions.Any(p => p.SymbolName == SymbolName && p.EntryTime.Date == Server.Time.Date) || _lastTradeDate.Date == Server.Time.Date) 
            {
                 if (Server.Time.Date == new DateTime(2025,5,14) && Server.Time.Minute % 15 == 0 && Server.Time.Second < 5) // Log periodically on target date
                 {
                    DebugLog($"[DEBUG_ONTICK] Trade already executed or position exists for {SymbolName} on {Server.Time.Date:yyyy-MM-dd}. No new entries.");
                 }
                return;
            }
            
            // The rest of the OnTick logic will use _dailyTrendContext
            // DebugLog($"[DEBUG] ======= НОВЫЙ ТИК ======= {Server.Time} ======="); // This can be noisy
            // DebugLog($"[DEBUG] Текущий тренд (daily): {_dailyTrendContext}, цена = {Symbol.Bid:F5}, время = {Server.Time}");
            // DebugLog($"[DEBUG] Настройки: MinRR={_minRR:F2}, MaxRR={_maxRR:F2}");

            // --- Removed old logic for CheckAsianSession and shouldFindFractals ---
            // CheckAsianSession(); // No longer needed here, handled by daily setup
            // bool shouldFindFractals = _asianFractals.Count == 0 || (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime > _lastH1BarTime);
            // bool newH1BarJustLogged = (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime == _lastH1BarTime);
            // if (_asianFractals.Count == 0 || (newH1BarJustLogged && Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date))
            // {
            //    DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию для тренда: {_dailyTrendContext}...");
            //    FindAsianSessionFractals(_dailyTrendContext); // Already called in daily setup
            // }
            
            // DebugLog($"[DEBUG_ONTICK] Processing tick for {_dailyTrendContext} trend. Asian fractals found: {_asianFractals.Count}");
            // Conditional logging based on whether fractals are found or not
            if (_asianFractalsFoundForToday)
            {
                 DebugLog($"[DEBUG_ONTICK] Processing tick. Daily Trend: {_dailyTrendContext}. Asian fractals loaded ({_asianFractals.Count}). Sweeps will be checked.");
            }
            else if (_dailyTrendContext != TrendContext.Neutral && Server.Time.Hour >= AsiaStartHour && Server.Time.Hour < AsiaEndHour)
            {
                 DebugLog($"[DEBUG_ONTICK] Processing tick. Daily Trend: {_dailyTrendContext}. Waiting for Asian fractals to be identified (current time: {Server.Time:HH:mm}).");
            }
            // No specific log if outside Asian session and fractals not found yet, to avoid spam.

            if (!_asianFractalsFoundForToday && _dailyTrendContext != TrendContext.Neutral) 
            {
                // If fractals are not yet found for today (and trend is active), don't proceed to sweep/BOS logic.
                // This also implicitly handles the case where we are outside Asian session hours before fractals were found.
                return; 
            }

            CheckFractalsSweep(_dailyTrendContext);
            
            foreach (var fractal in _asianFractals.Where(f => f.IsSwept && !f.EntryDone && f.BosLevel.HasValue).ToList())
            {
                TradeType entryTradeType;
                if (TrendContext.Bullish == _dailyTrendContext)
                {
                    entryTradeType = TradeType.Buy;
                }
                else if (TrendContext.Bearish == _dailyTrendContext)
                {
                    entryTradeType = TradeType.Sell;
                }
                else
                {
                    continue; 
                }

                var bosResult = Is3mStructureBreak(fractal, _dailyTrendContext);
                if (bosResult.IsBreak)
                {
                    // Логирование BOS
                    if (fractal.LastBosCheckBarIndex >= 0 && fractal.LastBosCheckBarIndex < _m3Bars.Count)
                    {
                        Bar m3BosBar = _m3Bars[fractal.LastBosCheckBarIndex];
                        LogChartEvent(bosResult.BreakTime, 
                                      "BOS_CONFIRMED", 
                                      h1Open: m3BosBar.Open, // Прямой доступ, т.к. Bar - структура
                                      h1High: m3BosBar.High,
                                      h1Low: m3BosBar.Low,
                                      h1Close: m3BosBar.Close,
                                      price1: bosResult.EntryPrice, 
                                      tradeType: entryTradeType.ToString(), 
                                      notes: $"Fractal Level: {fractal.Level.ToString(CultureInfo.InvariantCulture)}. M3 Bar Time: {m3BosBar.OpenTime.ToString("HH:mm:ss")}");
                    }
                    else
                    {
                        // Логируем BOS без данных M3 бара, если он не доступен
                        LogChartEvent(bosResult.BreakTime, 
                                      "BOS_CONFIRMED", 
                                      price1: bosResult.EntryPrice, 
                                      tradeType: entryTradeType.ToString(), 
                                      notes: $"Fractal Level: {fractal.Level.ToString(CultureInfo.InvariantCulture)}. M3 Bar data unavailable.");
                    }
                    
                    DebugLog($"[DEBUG_ONTICK_PRE_ENTER_POS] Attempting to call EnterPosition for fractal at {fractal.Time} with BOS Price {bosResult.EntryPrice}");
                    EnterPosition(entryTradeType, bosResult.EntryPrice, fractal);
                    DebugLog($"[DEBUG_ONTICK_POST_ENTER_POS] Returned from EnterPosition for fractal at {fractal.Time}");
                }
            }
            
            DebugLog($"[DEBUG] ======= КОНЕЦ ТИКА ======= {Server.Time} =======");
        }

        protected override void OnStop()
        {
            if (_chartDataWriter != null)
            {
                try
                {
                    _chartDataWriter.Flush();
                    _chartDataWriter.Close();
                    Print("Chart data logging stopped.");
                }
                catch (Exception ex)
                {
                    Print($"Error closing chart data logger: {ex.Message}");
                }
                _chartDataWriter = null;
            }
        }

        private void FindAsianSessionFractals(TrendContext trendContext)
        {
            DebugLog($"[DEBUG] Entered FindAsianSessionFractals for trend: {trendContext} on {Server.Time:yyyy-MM-dd HH:mm:ss}");
            _asianFractals.Clear();
            var h1Bars = _h1Bars;
            DebugLog($"[DEBUG] FindAsianSessionFractals: h1Bars.Count = {h1Bars.Count}"); // Log H1 bars count

            if (h1Bars.Count < FractalPeriod * 2 + 1) // Ensure enough bars for fractal calculation and loop
            {
                DebugLog($"[DEBUG] FindAsianSessionFractals: Not enough H1 bars ({h1Bars.Count}) for FractalPeriod {FractalPeriod}. Need at least {FractalPeriod * 2 + 1}. Skipping fractal search.");
                return;
            }

            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            var today = Server.Time.Date;
            DebugLog($"[DEBUG] FindAsianSessionFractals: Starting search for {trendContext}. Today: {today:yyyy-MM-dd}, AsiaStartHour: {AsiaStartHour}, AsiaEndHour: {AsiaEndHour}, FractalPeriod: {FractalPeriod}");

            for (int i = FractalPeriod; i < h1Bars.Count - FractalPeriod; i++)
            {
                var barTime = h1Bars.OpenTimes[i];
                bool isInAsianSession = barTime.Date == today && barTime.Hour >= AsiaStartHour && barTime.Hour < AsiaEndHour;
                DebugLog($"[DEBUG] FindAsianSessionFractals: Checking H1 bar at index {i}, Time: {barTime:yyyy-MM-dd HH:mm}, IsInAsianSession: {isInAsianSession}");
                
                if (!isInAsianSession)
                    continue;
                
                // Для бычьего тренда ищем нижние фракталы для лонгов
                if (trendContext == TrendContext.Bullish)
                {
                    double downFractalValue = hourlyFractals.DownFractal[i];
                    DebugLog($"[DEBUG] FindAsianSessionFractals (Bullish): Bar {barTime:HH:mm}, Raw DownFractal[i]: {downFractalValue}");
                    if (!double.IsNaN(downFractalValue))
                    {
                        _asianFractals.Add(new AsianFractal
                        {
                            Level = downFractalValue,
                            Time = barTime,
                            IsSwept = false
                        });
                        DebugLog($"[DEBUG] FindAsianSessionFractals (Bullish): Added Asian H1 DownFractal. Level={downFractalValue:F5}, Time={barTime}");
                        LogChartEvent(barTime, "ASIAN_FRACTAL", price1: downFractalValue, tradeType: "Bullish", notes: "Lower fractal");
                    }
                }
                // Для медвежьего тренда ищем верхние фракталы для шортов
                else if (trendContext == TrendContext.Bearish)
                {
                    double upFractalValue = hourlyFractals.UpFractal[i];
                    DebugLog($"[DEBUG] FindAsianSessionFractals (Bearish): Bar {barTime:HH:mm}, Raw UpFractal[i]: {upFractalValue}");
                    if (!double.IsNaN(upFractalValue))
                    {
                        _asianFractals.Add(new AsianFractal
                        {
                            Level = upFractalValue,
                            Time = barTime,
                            IsSwept = false
                        });
                        DebugLog($"[DEBUG] FindAsianSessionFractals (Bearish): Added Asian H1 UpFractal. Level={upFractalValue:F5}, Time={barTime}");
                        LogChartEvent(barTime, "ASIAN_FRACTAL", price1: upFractalValue, tradeType: "Bearish", notes: "Upper fractal");
                    }
                }
            }
            
            // Сортируем фракталы
            var currentPrice = Bars.ClosePrices.Last(0);
            if (trendContext == TrendContext.Bullish)
            {
                _asianFractals = _asianFractals.OrderBy(f => f.Level).ToList();
            }
            else if (trendContext == TrendContext.Bearish)
            {
                _asianFractals = _asianFractals.OrderByDescending(f => f.Level).ToList();
            }

            if (today == new DateTime(2025, 5, 14)) // Логируем только для целевой даты
            {
                foreach (var f in _asianFractals)
                {
                    DebugLog($"    - Level: {f.Level:F5}, Time: {f.Time}, Diff Pips: {Math.Abs(f.Level - currentPrice) / Symbol.PipSize:F1}");
                }
            }
        }

        private void CheckFractalsSweep(TrendContext trendContext)
        {
            var m3Bars = _m3Bars;
            if (m3Bars.Count < FractalPeriod * 2 + 1) return;

            var currentM3Bar = m3Bars.Last(1);

            foreach (var fractal in _asianFractals.Where(f => !f.IsSwept && !f.EntryDone).ToList())
            {
                if (currentM3Bar.OpenTime < fractal.Time) continue;

                bool sweptThisTick = false;
                string sweepType = "";

                if (trendContext == TrendContext.Bullish || TrendMode == ManualTrendMode.Bullish)
                {
                    if (currentM3Bar.Low < fractal.Level)
                    {
                        fractal.IsSwept = true;
                        sweptThisTick = true;
                        fractal.SweepLevel = fractal.Level;
                        fractal.SweepExtreme = currentM3Bar.Low;
                        fractal.SweepBarIndex = m3Bars.Count - 2;

                        fractal.BosLevel = currentM3Bar.High;
                        fractal.LastBosCheckBarIndex = m3Bars.Count - 2;

                        DebugLog($"[SWEEP_BULL] Low fractal {fractal.Level} at {fractal.Time} swept by M3 bar {currentM3Bar.OpenTime}. Bar L: {currentM3Bar.Low}, H: {currentM3Bar.High}. New BOS Level: {fractal.BosLevel}");
                        sweptThisTick = true;
                        sweepType = "BullishSweep";
                    }
                }
                else if (trendContext == TrendContext.Bearish || TrendMode == ManualTrendMode.Bearish)
                {
                    if (currentM3Bar.High > fractal.Level)
                    {
                        fractal.IsSwept = true;
                        sweptThisTick = true;
                        fractal.SweepLevel = fractal.Level;
                        fractal.SweepExtreme = currentM3Bar.High;
                        fractal.SweepBarIndex = m3Bars.Count - 2;

                        fractal.BosLevel = currentM3Bar.Low;
                        fractal.LastBosCheckBarIndex = m3Bars.Count - 2;

                        DebugLog($"[SWEEP_BEAR] High fractal {fractal.Level} at {fractal.Time} swept by M3 bar {currentM3Bar.OpenTime}. Bar H: {currentM3Bar.High}, L: {currentM3Bar.Low}. New BOS Level: {fractal.BosLevel}");
                        sweptThisTick = true;
                        sweepType = "BearishSweep";
                    }
                }

                if (sweptThisTick)
                {
                    LogChartEvent(currentM3Bar.OpenTime, "SWEEP", price1: fractal.SweepLevel, price2: fractal.SweepExtreme, tradeType: sweepType, notes: $"Original fractal at {fractal.Time.ToString("HH:mm:ss")} level {fractal.Level.ToString(CultureInfo.InvariantCulture)}");
                }
            }
        }

        private class StructureBreakResult
        {
            public bool IsBreak { get; set; }
            public double EntryPrice { get; set; }
            public DateTime BreakTime { get; set; }
        }

        private StructureBreakResult Is3mStructureBreak(AsianFractal fractal, TrendContext trendContext)
        {
            var result = new StructureBreakResult { IsBreak = false };
            if (fractal == null || !fractal.IsSwept || fractal.BosLevel == null || fractal.SweepBarIndex == null)
            {
                return result;
            }

            var m3Bars = _m3Bars;
            if (m3Bars.Count < fractal.SweepBarIndex.Value + 2)
            {
                return result;
            }
            
            int firstPotentialBosBarIndex = fractal.SweepBarIndex.Value + 1;

            int startIndexToCheck = Math.Max(firstPotentialBosBarIndex, fractal.LastBosCheckBarIndex + 1);

            for (int i = startIndexToCheck; i < m3Bars.Count; i++)
            {
                var candidateBar = m3Bars[i];
                fractal.LastBosCheckBarIndex = i;

                if (candidateBar.OpenTime == _debugSpecificTimestamp && !_loggedSpecificBarDataThisInstance)
                {
                     DebugLog($"[DEBUG_OHLC_BAR_SPECIFIC_FOR_BOS_CHECK] Target Time: {_debugSpecificTimestamp}. Bar {candidateBar.OpenTime} (Index {i}) O:{candidateBar.Open} H:{candidateBar.High} L:{candidateBar.Low} C:{candidateBar.Close} Vol:{candidateBar.TickVolume}. Checking against BOS level: {fractal.BosLevel}");
                    _loggedSpecificBarDataThisInstance = true;
                }

                if (trendContext == TrendContext.Bullish || TrendMode == ManualTrendMode.Bullish)
                {
                    if (candidateBar.Close > fractal.BosLevel.Value)
                    {
                        result.IsBreak = true;
                        result.EntryPrice = candidateBar.Close;
                        result.BreakTime = candidateBar.OpenTime;
                        DebugLog($"[BOS_SUCCESS_BULL] Bullish BOS Confirmed by M3 bar {candidateBar.OpenTime}. Close: {candidateBar.Close} > BOS Level: {fractal.BosLevel.Value}. Entry at market.");
                        return result;
                    }
                }
                else if (trendContext == TrendContext.Bearish || TrendMode == ManualTrendMode.Bearish)
                {
                    if (candidateBar.Close < fractal.BosLevel.Value)
                    {
                        result.IsBreak = true;
                        result.EntryPrice = candidateBar.Close;
                        result.BreakTime = candidateBar.OpenTime;
                        DebugLog($"[BOS_SUCCESS_BEAR] Bearish BOS Confirmed by M3 bar {candidateBar.OpenTime}. Close: {candidateBar.Close} < BOS Level: {fractal.BosLevel.Value}. Entry at market.");
                        return result;
                    }
                }
            }
            return result;
        }

        private bool IsInAsiaSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= 0 && hour < 6; 
        }
        private bool IsInFrankfurtSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= 6 && hour < 7;
        }
        private bool IsInLondonSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= 7 && hour < 12;
        }

        private double? FindKeyLevelForTP(TradeType tradeType, double entryPrice)
        {
            var h1Bars = _h1Bars;
            if (h1Bars.Count < 10) return null;
            
            double highest = double.MinValue;
            double lowest = double.MaxValue;
            int lookback = Math.Min(24, h1Bars.Count);
            
            for (int i = 0; i < lookback; i++)
            {
                highest = Math.Max(highest, h1Bars.HighPrices[h1Bars.Count - 1 - i]);
                lowest = Math.Min(lowest, h1Bars.LowPrices[h1Bars.Count - 1 - i]);
            }
            
            if (tradeType == TradeType.Buy)
            {
                if (highest > entryPrice)
                {
                    DebugLog($"[DEBUG] Найден ключевой уровень для лонга (максимум 24ч): {highest:F5}");
                    return highest;
                }
                
                double targetLevel = entryPrice + (entryPrice - lowest) * 0.618;
                DebugLog($"[DEBUG] Расчетный ключевой уровень для лонга: {targetLevel:F5}");
                return targetLevel;
            }
            else
            {
                if (lowest < entryPrice)
                {
                    DebugLog($"[DEBUG] Найден ключевой уровень для шорта (минимум 24ч): {lowest:F5}");
                    return lowest;
                }
                
                double targetLevel = entryPrice - (highest - entryPrice) * 0.618;
                DebugLog($"[DEBUG] Расчетный ключевой уровень для шорта: {targetLevel:F5}");
                return targetLevel;
            }
        }

        private bool IsStrongTrend(out TrendContext context)
        {
            context = SimpleTrendContext();
            return context != TrendContext.Neutral;
        }

        private void LogChartEvent(DateTime timestamp, string eventType, double? h1Open = null, double? h1High = null, double? h1Low = null, double? h1Close = null, double? price1 = null, double? price2 = null, double? price3 = null, string tradeType = "", string notes = "")
        {
            if (_chartDataWriter == null) return;

            try
            {
                string h1OpenStr = h1Open?.ToString(CultureInfo.InvariantCulture) ?? "";
                string h1HighStr = h1High?.ToString(CultureInfo.InvariantCulture) ?? "";
                string h1LowStr = h1Low?.ToString(CultureInfo.InvariantCulture) ?? "";
                string h1CloseStr = h1Close?.ToString(CultureInfo.InvariantCulture) ?? "";
                string price1Str = price1?.ToString(CultureInfo.InvariantCulture) ?? "";
                string price2Str = price2?.ToString(CultureInfo.InvariantCulture) ?? "";
                string price3Str = price3?.ToString(CultureInfo.InvariantCulture) ?? "";

                // Очистка notes от запятых и кавычек во избежание проблем с CSV
                // Заменяем также точку с запятой в notes, так как она теперь наш основной разделитель
                string sanitizedNotes = notes?.Replace(";", ":")?.Replace(",", ".")?.Replace("\"", "'") ?? "";

                // Изменяем разделитель на точку с запятой
                _chartDataWriter.WriteLine($"{timestamp:yyyy-MM-dd HH:mm:ss.fff};{eventType};{h1OpenStr};{h1HighStr};{h1LowStr};{h1CloseStr};{price1Str};{price2Str};{price3Str};{tradeType};{sanitizedNotes}");
            }
            catch (Exception ex)
            {
                Print($"Error writing to chart data file: {ex.Message}");
            }
        }

        // Helper function for manual price normalization
        private double NormalizePriceManually(double price)
        {
            return Math.Round(price, Symbol.Digits);
        }
    }

    public enum TrendContext
    {
        Bullish,
        Bearish,
        Neutral
    }

    public enum ManualTrendMode
    {
        Auto,
        Bullish,
        Bearish
    }
} 