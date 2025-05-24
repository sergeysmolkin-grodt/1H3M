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

        [Parameter("Stop Loss Buffer Pips", DefaultValue = 1.6)]
        public double StopLossBufferPips { get; set; }

        [Parameter("Max BOS Distance Pips", DefaultValue = 15.0)]
        public double MaxBOSDistancePips { get; set; }

        // --- Контекст Параметры ---
        [Parameter("Enable Manual Trend", DefaultValue = ManualTrendMode.Auto, Group = "Context")]
        public ManualTrendMode TrendMode { get; set; }

        [Parameter("Enable Candle Counting", DefaultValue = true, Group = "Context")]
        public bool EnableCandleCounting { get; set; }

        [Parameter("Trend Candle Lookback", DefaultValue = 25, MinValue = 5, Group = "Context")]
        public int TrendCandleLookback { get; set; }

        [Parameter("Trend Candle Threshold", DefaultValue = 5, MinValue = 1, Group = "Context")]
        public int TrendCandleThreshold { get; set; }

        [Parameter("Enable Impulse Analysis", DefaultValue = true, Group = "Context")]
        public bool EnableImpulseAnalysis { get; set; }

        [Parameter("Impulse Lookback", DefaultValue = 5, MinValue = 2, Group = "Context")]
        public int ImpulseLookback { get; set; }

        [Parameter("Impulse Pip Threshold", DefaultValue = 40.0, MinValue = 1.0, Group = "Context")]
        public double ImpulsePipThreshold { get; set; }

        [Parameter("Enable Structure Analysis", DefaultValue = true, Group = "Context")]
        public bool EnableStructureAnalysis { get; set; }

        [Parameter("Structure Lookback", DefaultValue = 10, MinValue = 3, Group = "Context")]
        public int StructureLookback { get; set; }

        [Parameter("Structure Confirmation Count", DefaultValue = 2, MinValue = 1, Group = "Context")]
        public int StructureConfirmationCount { get; set; }
        // --- Конец Контекст Параметры ---

        private const double _fixedRiskPercent = 1.0; // Fixed risk at 1%

        private const double _minRR = 1.3;
        private const double _maxRR = 5.0;

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
        // private bool _loggedSpecificBarData = false; // Флаг, чтобы залогировать данные только один раз - Warning CS0414, комментируем
        private bool _loggedSpecificBarDataThisInstance = false;
        private DateTime _debugSpecificTimestamp = DateTime.MinValue;
        private HashSet<DateTime> _loggedOHLCBarsForTargetDate = new HashSet<DateTime>();

        private string _lastTrendContextLogMessage = null; // For storing the last trend context message determined by DebugLog

        private StreamWriter _chartDataWriter;
        private string _csvFilePath;
        private static readonly string CSV_HEADER = "Timestamp;EventType;H1_Open;H1_High;H1_Low;H1_Close;Price1;Price2;TradeType;Notes";

        protected override void OnStart()
        {
            _fractals = Indicators.Fractals(Bars, FractalPeriod);
            _m3Bars = MarketData.GetBars(TimeFrame.Minute3);
            _h1Bars = MarketData.GetBars(TimeFrame.Hour);

            _debugSpecificTimestamp = new DateTime(2025, 5, 14, 6, 15, 0, DateTimeKind.Utc);
            _loggedOHLCBarsForTargetDate.Clear();

            try
            {
                DebugLog($"[INITIAL_SYMBOL_INFO] Symbol: {SymbolName}, PipSize: {Symbol.PipSize}, TickSize: {Symbol.TickSize}, Digits: {Symbol.Digits}, MinStopLossDistance: {Symbol.MinStopLossDistance}, MinTakeProfitDistance: {Symbol.MinTakeProfitDistance}");
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
            var riskAmount = accountBalance * (_fixedRiskPercent / 100.0);
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
        private List<double> FindAllH1FractalsForTP(TradeType tradeType, double entryPrice)
        {
            var h1Fractals = new List<double>();
            var h1Bars = _h1Bars;
            var hourlyFractalsIndicator = Indicators.Fractals(h1Bars, FractalPeriod); // Renamed to avoid conflict

            DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] --- Verifying: All Raw H1 UpFractals (Before Filter/Sort) for Entry: {entryPrice:F5} ---");
            for (int i = FractalPeriod; i < h1Bars.Count - FractalPeriod; i++)
            {
                if (!double.IsNaN(hourlyFractalsIndicator.UpFractal[i]))
                {
                     DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] Index {i}: Raw H1 UpFractal: {hourlyFractalsIndicator.UpFractal[i]:F5} at {h1Bars.OpenTimes[i]}");
                }
                if (tradeType == TradeType.Buy && !double.IsNaN(hourlyFractalsIndicator.UpFractal[i]))
                {
                    var level = hourlyFractalsIndicator.UpFractal[i];
                    if (level > entryPrice) // Only consider fractals above entry for buy
                    {
                        h1Fractals.Add(level);
                        DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] Found {level:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
            }

            DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] --- All Raw H1 DownFractals (Before Filter/Sort) for Entry: {entryPrice:F5} ---");
            for (int i = FractalPeriod; i < h1Bars.Count - FractalPeriod; i++)
            {
                if (!double.IsNaN(hourlyFractalsIndicator.DownFractal[i]))
                {
                    DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] Index {i}: Raw H1 DownFractal: {hourlyFractalsIndicator.DownFractal[i]:F5} at {h1Bars.OpenTimes[i]}");
                }
                if (tradeType == TradeType.Sell && !double.IsNaN(hourlyFractalsIndicator.DownFractal[i]))
                {
                    var level = hourlyFractalsIndicator.DownFractal[i];
                    if (level < entryPrice) // Only consider fractals below entry for sell
                    {
                        h1Fractals.Add(level);
                        DebugLog($"[TP_DEBUG_ALL_RAW_FRACTALS] Found {level:F5} at {h1Bars.OpenTimes[i]}");
                    }
                }
            }

            // Sort fractals by distance from entry price (nearest first)
            if (tradeType == TradeType.Buy)
            {
                h1Fractals.Sort((a, b) => (a - entryPrice).CompareTo(b - entryPrice));
            }
            else // Sell
            {
                h1Fractals.Sort((a, b) => (entryPrice - a).CompareTo(entryPrice - b));
            }
            
            if(h1Fractals.Count > 0)
            {
                DebugLog($"[TP_DEBUG_SORTED_LIST] Found {h1Fractals.Count} fractals. Nearest is {h1Fractals[0]:F5}");
            } else {
                DebugLog($"[TP_DEBUG_SORTED_LIST] No suitable H1 fractals found by FindAllH1FractalsForTP.");
            }

            return h1Fractals;
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

            List<double> allCandidateTps = FindAllH1FractalsForTP(tradeType, entryPrice);
            
            // Log the entire sorted list of candidate TPs
            if (allCandidateTps.Count > 0)
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                sb.Append("[TP_DEBUG_SORTED_LIST_FULL] Sorted Candidate TPs: ");
                foreach (var tp_val in allCandidateTps)
                {
                    sb.AppendFormat("{0:F5}; ", tp_val);
                }
                DebugLog(sb.ToString());
            }
            else
            {
                DebugLog("[TP_DEBUG_SORTED_LIST_FULL] No candidate TPs found after sorting/filtering.");
            }

            if (allCandidateTps.Count == 0)
            {
                DebugLog($"[TP_DEBUG] No H1 fractals found by FindAllH1FractalsForTP.");
                return (null, 0);
            }

            DebugLog($"[TP_DEBUG] Found {allCandidateTps.Count} H1 fractals to check.");

            foreach (var candidateTp in allCandidateTps)
            {
                DebugLog($"[TP_DEBUG] Evaluating TP Candidate ({candidateTp:F5})");
                double currentRr = Math.Abs(candidateTp - entryPrice) / stopLossDistance;
                DebugLog($"[TP_DEBUG] RR for Candidate ({candidateTp:F5}): {currentRr:F2}");

                if (currentRr >= _minRR && currentRr <= _maxRR)
                {
                    DebugLog($"[TP_DEBUG] SUITABLE TP found: {candidateTp:F5} with RR {currentRr:F2}. Using it as it's the first suitable in sorted list.");
                    return (candidateTp, currentRr); // Используем первый подходящий
                }
                DebugLog($"[TP_DEBUG] TP Candidate {candidateTp:F5} (RR {currentRr:F2}) is NOT SUITABLE (Range: {_minRR:F2}-{_maxRR:F2}). Checking next.");
            }
            
            DebugLog($"[TP_DEBUG] No suitable H1 fractal TP found that meets RR criteria after checking all {allCandidateTps.Count} fractals.");
            // Return the RR of the last checked (furthest valid direction) fractal if any were checked, otherwise 0.
            double lastConsideredRr = 0;
            if (allCandidateTps.Count > 0)
            {
                // As fractals are sorted by distance, the last one might not be the "last considered" in terms of loop,
                // but it's the furthest valid one. For logging, let's use the RR of the closest one if none matched.
                 lastConsideredRr = Math.Abs(allCandidateTps[0] - entryPrice) / stopLossDistance;
                 DebugLog($"[TP_DEBUG] Closest fractal {allCandidateTps[0]:F5} had RR {lastConsideredRr:F2}, but was not suitable or no fractals were suitable.");
            }
            return (null, lastConsideredRr); 
        }

        private double CalculateStopLoss(TradeType tradeType, double asianFractalLevelToPlaceSLBehind)
        {
            if (tradeType == TradeType.Buy)
            {
                // SL is below the Asian H1 fractal level
                return asianFractalLevelToPlaceSLBehind - (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
            else
            {
                // SL is above the Asian H1 fractal level
                return asianFractalLevelToPlaceSLBehind + (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
        }

        private void EnterPosition(TradeType tradeType, double entryPrice, AsianFractal fractal)
        {
            if (_lastTradeDate.Date == Server.Time.Date && Positions.Count > 0)
            {
                DebugLog("Trading limit: One trade per symbol per day. Position already exists or trade executed today.");
                return;
            }

            if (fractal == null || fractal.SweepExtreme == null || fractal.BosLevel == null)
            {
                DebugLog("[ERROR_ENTRY] Fractal or its SweepExtreme/BosLevel is null. Cannot calculate SL.");
                return;
            }
            
            double slPrice;
            string slCalculationBasis;

            if (tradeType == TradeType.Buy)
            {
                // SL is below the Asian H1 fractal level
                slPrice = Math.Round(fractal.Level - StopLossBufferPips * Symbol.PipSize, Symbol.Digits);
                slCalculationBasis = $"AsianFractal.Level ({fractal.Level:F5}) - Buffer ({StopLossBufferPips} pips)";
            }
            else // Sell
            {
                // SL is above the Asian H1 fractal level
                slPrice = Math.Round(fractal.Level + StopLossBufferPips * Symbol.PipSize, Symbol.Digits);
                slCalculationBasis = $"AsianFractal.Level ({fractal.Level:F5}) + Buffer ({StopLossBufferPips} pips)";
            }
            DebugLog($"[DEBUG_SL_CALC] EntryPrice for SL calc: {entryPrice:F5}, AsianFractal Level: {fractal.Level:F5}, SL Buffer: {StopLossBufferPips}, Calculated SL Price: {slPrice:F5}");
            
            var stopLossPips = Math.Abs(entryPrice - slPrice) / Symbol.PipSize;
            if (stopLossPips < 1.0) // Minimum 1 pip SL
            {
                DebugLog($"[SL_ADJUST] Calculated SL ({stopLossPips} pips) is too small. Adjusting SL.");
                if (tradeType == TradeType.Buy)
                    slPrice = Math.Round(entryPrice - Symbol.PipSize * 5, Symbol.Digits); // Min 5 pips SL
                else
                    slPrice = Math.Round(entryPrice + Symbol.PipSize * 5, Symbol.Digits);
                stopLossPips = Math.Abs(entryPrice - slPrice) / Symbol.PipSize;
            }

            var tpResult = CalculateTakeProfit(tradeType, entryPrice, slPrice);
            if (tpResult.takeProfitPrice == null || tpResult.rr < _minRR)
            {
                DebugLog($"[TP_REJECT] TP calculation failed or RR too low ({tpResult.rr}). MinRR: {_minRR}. Entry: {entryPrice}, SL: {slPrice}");
                return;
            }

            var positionSize = CalculatePositionSize(stopLossPips);
            if (positionSize < Symbol.VolumeInUnitsMin)
            {
                DebugLog($"[VOL_REJECT] Calculated position size {positionSize} is less than min volume {Symbol.VolumeInUnitsMin}. SL pips: {stopLossPips}");
                return;
            }

            var label = $"H3M_{tradeType}_{Server.Time.ToShortTimeString()}";
            
            // НОВЫЙ ЛОГ ПЕРЕД ИСПОЛНЕНИЕМ
            DebugLog($"[PRE_EXECUTE_ORDER] Attempting to execute market order. Symbol: {SymbolName}, Type: {tradeType}, Size: {positionSize}, Label: {label}, SL: {slPrice:F5}, TP: {tpResult.takeProfitPrice?.ToString("F5") ?? "N/A"}");

            // ВРЕМЕННО ДЛЯ ДИАГНОСТИКИ: Вызываем без SL/TP
            var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, label, null, null);
            // var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, label, slPrice, tpResult.takeProfitPrice); // ОРИГИНАЛЬНЫЙ ВЫЗОВ

            if (result.IsSuccessful)
            {
                fractal.EntryDone = true;
                _lastTradeDate = Server.Time.Date;
                var position = result.Position;

                DebugLog($"[TRADE_OPEN_ACTUAL_PRE_MODIFY] {tradeType} order successful. Price: {position.EntryPrice:F5}, Initial SL: {position.StopLoss?.ToString("F5") ?? "N/A"}, Initial TP: {position.TakeProfit?.ToString("F5") ?? "N/A"}. Attempting to modify SL to {slPrice:F5} and TP to {tpResult.takeProfitPrice?.ToString("F5") ?? "N/A"}.");

                // Modify Stop Loss and Take Profit asynchronously in a single call
                if (position.StopLoss != slPrice || position.TakeProfit != tpResult.takeProfitPrice)
                {
                    ModifyPositionAsync(position, slPrice, tpResult.takeProfitPrice);
                }

                // Поскольку это асинхронный вызов, мы не будем здесь ждать его завершения в OnTick,
                // но для целей отладки мы могли бы добавить обработку завершения (не в этом шаге).

                // Лог TRADE_OPEN_ACTUAL будет теперь отражать состояние SL/TP *после* попытки модификации,
                // но это произойдет на следующем тике, когда позиция обновится.
                // Для более точного лога моментальной установки, мы бы использовали .Result или await,
                // но это может заблокировать OnTick, что нежелательно.
                // Поэтому, пока оставим как есть и посмотрим на логи следующего тика или на итоговое состояние сделки.

                DebugLog($"[TRADE_OPEN_ACTUAL_POST_MODIFY] {tradeType} order successful. Price: {position.EntryPrice:F5}, SL sent for modification: {slPrice:F5}, TP sent for modification: {tpResult.takeProfitPrice?.ToString("F5") ?? "N/A"}. Intended SL (Original Calc): {slPrice:F5} (Basis: {slCalculationBasis}), Intended TP (Original Calc): {tpResult.takeProfitPrice?.ToString("F5") ?? "N/A"} (RR: {tpResult.rr}). Size: {positionSize}");

                LogChartEvent(position.EntryTime, "TRADE_ENTRY", price1: position.EntryPrice, price2: slPrice, tradeType: tradeType.ToString(), notes: $"Intended TP: {tpResult.takeProfitPrice?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}, RR: {tpResult.rr.ToString("F2", CultureInfo.InvariantCulture)}, Label: {label}, Modified SL: {slPrice.ToString(CultureInfo.InvariantCulture)}, Modified TP: {tpResult.takeProfitPrice?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}");
            }
            else
            {
                DebugLog($"[TRADE_FAIL] {tradeType} order failed: {result.Error}");
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
            if (currentTime.Date != _lastAsianSessionCheck)
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

        // Renamed from SimpleTrendContext and reinstated full logic
        private TrendContext EvaluateTrendContextCriteria()
        {
            // Initial check based on the parameterized lookback for candle counting
            if (_h1Bars == null || _h1Bars.Count < TrendCandleLookback) return TrendContext.Neutral; 
            
            int last = _h1Bars.Count - 1;
            int bullish = 0, bearish = 0;
            
            // Счетчик бычьих/медвежьих баров using TrendCandleLookback
            if (EnableCandleCounting)
            {
                for (int i = 0; i < TrendCandleLookback; i++) 
                {
                    if (last - i < 0) break; 
                    if (_h1Bars.ClosePrices[last - i] > _h1Bars.OpenPrices[last - i]) bullish++;
                    else if (_h1Bars.ClosePrices[last - i] < _h1Bars.OpenPrices[last - i]) bearish++;
                }
            }
            
            // --- Impulse Logic ---
            bool strongBullishImpulse = false;
            bool strongBearishImpulse = false;
            double recentMovement = 0;

            if (EnableImpulseAnalysis && _h1Bars.Count >= ImpulseLookback + 1) 
            {
                recentMovement = _h1Bars.ClosePrices[last] - _h1Bars.ClosePrices[last - ImpulseLookback];
                strongBullishImpulse = recentMovement > Symbol.PipSize * ImpulsePipThreshold;
                strongBearishImpulse = recentMovement < -Symbol.PipSize * ImpulsePipThreshold;
            }
            
            // --- Structure Analysis Logic (HH/HL, LL/LH) ---
            bool hasHigherHighs = false;
            bool hasHigherLows = false;
            bool hasLowerLows = false;
            bool hasLowerHighs = false;

            if (EnableStructureAnalysis && _h1Bars.Count >= StructureLookback + 1)
            {
                double prevHigh = _h1Bars.HighPrices[last - StructureLookback];
                double prevLow = _h1Bars.LowPrices[last - StructureLookback];
                
                for (int i = StructureLookback - 1; i >= 0; i--) 
                {
                    int currentIndex = last - i;
                    if (_h1Bars.HighPrices[currentIndex] > prevHigh)
                    {
                        hasHigherHighs = true;
                    }
                    // Note: Original logic for LH might be slightly different if only one LH is needed.
                    // This checks if *any* subsequent high is lower than a *previous* high in the sequence.
                    // A more common definition of LH for downtrend would be current high < previous significant high.
                    // For simplicity, this structure check might need refinement based on precise strategy.
                    else if (_h1Bars.HighPrices[currentIndex] < prevHigh) 
                    {
                        hasLowerHighs = true;
                    }
                    
                    if (_h1Bars.LowPrices[currentIndex] > prevLow)
                    {                       
                        hasHigherLows = true;
                    }
                    else if (_h1Bars.LowPrices[currentIndex] < prevLow)
                    {
                        hasLowerLows = true;
                    }
                    
                    prevHigh = _h1Bars.HighPrices[currentIndex];
                    prevLow = _h1Bars.LowPrices[currentIndex];
                }
            }
            
            // Принятие решения о тренде
            bool bullishCondition = false;
            if (EnableCandleCounting)
            {
                bullishCondition = bullishCondition || (bullish > bearish + TrendCandleThreshold);
            }
            if (EnableImpulseAnalysis)
            {
                bullishCondition = bullishCondition || strongBullishImpulse;
            }
            if (EnableStructureAnalysis)
            {
                bullishCondition = bullishCondition || (hasHigherHighs && hasHigherLows);
            }

            if (bullishCondition) 
            {
                DebugLog($"[DEBUG] Определен БЫЧИЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, HH={hasHigherHighs}, HL={hasHigherLows}, импульс={recentMovement/Symbol.PipSize:F1} пипсов (Свечи вкл: {EnableCandleCounting}, Структура вкл: {EnableStructureAnalysis}, Импульс вкл: {EnableImpulseAnalysis})");
                return TrendContext.Bullish;
            }
            
            bool bearishCondition = false;
            if (EnableCandleCounting)
            {
                bearishCondition = bearishCondition || (bearish > bullish + TrendCandleThreshold);
            }
            if (EnableImpulseAnalysis)
            {
                bearishCondition = bearishCondition || strongBearishImpulse;
            }
            if (EnableStructureAnalysis)
            {
                bearishCondition = bearishCondition || (hasLowerLows && hasLowerHighs);
            }

            if (bearishCondition) 
            {
                DebugLog($"[DEBUG] Определен МЕДВЕЖИЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, LL={hasLowerLows}, LH={hasLowerHighs}, импульс={recentMovement/Symbol.PipSize:F1} пипсов (Свечи вкл: {EnableCandleCounting}, Структура вкл: {EnableStructureAnalysis}, Импульс вкл: {EnableImpulseAnalysis})");
                return TrendContext.Bearish;
            }
            
            DebugLog($"[DEBUG] Определен НЕЙТРАЛЬНЫЙ тренд: быч.свечей={bullish}, медв.свечей={bearish}, HH={hasHigherHighs}, HL={hasHigherLows}, LL={hasLowerLows}, LH={hasLowerHighs}, движение={recentMovement/Symbol.PipSize:F1} пипсов (Свечи вкл: {EnableCandleCounting}, Структура вкл: {EnableStructureAnalysis}, Импульс вкл: {EnableImpulseAnalysis})");
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
            // Выводить в консоль только сообщения, касающиеся определения контекста тренда,
            // параметров исполняемой сделки или ошибки исполнения сделки.
            // if (message.Contains("[DEBUG] Определен") ||      // Сообщения об определении контекста тренда
            //     message.Contains("[PRE_EXECUTE_ORDER]") ||        // Попытка исполнить сделку с её параметрами
            //     message.Contains("[TRADE_OPEN_ACTUAL_PRE_MODIFY]") || // Детали успешно открытой сделки до модификации SL/TP
            //     message.Contains("[TRADE_OPEN_ACTUAL_POST_MODIFY]") || // Детали успешно открытой сделки после модификации SL/TP
            //     message.Contains("[TRADE_FAIL]"))                 // Сообщение о неудачной попытке открыть сделку
            // {
            //     Print(message); // Выводим сообщение в лог cTrader
            // }
            // Все остальные вызовы DebugLog с другими сообщениями будут проигнорированы (не будут выведены в консоль).

            // Check if it's a trend context message
            bool isTrendContextMessage = message.Contains("[DEBUG] Определен") ||
                                         message.Contains("[DEBUG] Очевидный бычий тренд") ||
                                         message.Contains("[DEBUG] Очевидный медвежий тренд") ||
                                         message.Contains("[DEBUG] Бычий контекст:") ||
                                         message.Contains("[DEBUG] Медвежий контекст:") ||
                                         message.Contains("[DEBUG] Bullish Context:") ||
                                         message.Contains("[DEBUG] Bearish Context:") ||
                                         message.Contains("[DEBUG] No Clear Trend Context");

            if (isTrendContextMessage)
            {
                _lastTrendContextLogMessage = message; // Store it, don't print immediately
            }
            // Check if it's one of the other types of messages that should be printed immediately
            else if (message.Contains("[PRE_EXECUTE_ORDER]") ||
                     message.Contains("[TRADE_OPEN_ACTUAL_PRE_MODIFY]") ||
                     message.Contains("[TRADE_OPEN_ACTUAL_POST_MODIFY]") ||
                     message.Contains("[TRADE_FAIL]"))
            {
                Print(message); // Print these immediately
            }
            // Any other message passed to DebugLog will not be printed, maintaining previous behavior for non-listed keywords.
        }

        protected override void OnTick()
        {
            _loggedSpecificBarDataThisInstance = false;

            if (Bars.TimeFrame != TimeFrame.Hour) 
            {
                DebugLog($"[DEBUG] Неверный таймфрейм для OnTick: {Bars.TimeFrame}");
                return;
            }

            var serverTime = Server.Time;
            int currentHour = serverTime.Hour;

            // Ограничение на Нью-Йоркскую сессию: не торговать с 15:00 до 22:59 UTC+3.
            // Предполагается, что Server.Time - это UTC+0 (на 3 часа раньше UTC+3).
            // Соответственно, блокируемый интервал по серверному времени: 12:00 до 19:59 UTC+0.
            // Блокируемые часы сервера (UTC+0): 12, 13, 14, 15, 16, 17, 18, 19.
            if (currentHour >= 12 && currentHour < 20) 
            {
                DebugLog($"[SESSION_SKIP_NY] Торговля пропущена. Текущее время сервера {serverTime:HH:mm} (предположительно UTC+0). Это соответствует {serverTime.AddHours(3):HH:mm} UTC+3, что внутри ограничения Нью-Йоркской сессии (15:00-22:59 UTC+3).");
                return;
            }

            // Фильтр для торговли только во время Франкфуртской или Лондонской сессии
            // Франкфурт (Сервер UTC+0: 06:00-06:59) -> 09:00-09:59 UTC+3
            // Лондон    (Сервер UTC+0: 07:00-11:59) -> 10:00-14:59 UTC+3
            // Общий разрешенный диапазон серверного времени: 06:00-11:59 UTC+0 (соответствует 09:00-14:59 UTC+3)
            if (!IsLondonOrFrankfurtSession(serverTime)) // serverTime - это Server.Time (UTC+0)
            {
                DebugLog($"[SESSION_SKIP_FL] Торговля пропущена. Текущее время сервера {serverTime:HH:mm} (UTC+0). Это соответствует {serverTime.AddHours(3):HH:mm} UTC+3, что находится вне разрешенных торговых сессий: Франкфурт (09:00-10:00 UTC+3) и Лондон (10:00-15:00 UTC+3).");
                return;
            }

            // --- Логирование H1 бара (корректное местоположение) ---
            if (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime != _lastH1BarTime)
            {
                var h1Bar = _h1Bars.Last(0);
                LogChartEvent(h1Bar.OpenTime, "H1_BAR", h1Open: h1Bar.Open, h1High: h1Bar.High, h1Low: h1Bar.Low, h1Close: h1Bar.Close);
                _lastH1BarTime = h1Bar.OpenTime;
            }
            // --- Конец логирования H1 бара ---

            TrendContext trendContext;
            var currentPrice = Symbol.Bid; // Для лонгов используем Bid, для шортов Ask

            if (Server.Time.Date != _lastAsianSessionCheck) // Проверяем один раз в день в начале дня
            {
                CheckAsianSession(); // Определяем, находимся ли мы в Азиатской сессии
                _lastAsianSessionCheck = Server.Time.Date;
            }

            bool strongTrend = IsStrongTrend(out trendContext); 

            if (!strongTrend)
            {
                // If there's a stored trend message from a previous tick where trend was strong, 
                // and now it's not, we might want to clear it or let it persist.
                // Current DebugLog logic will overwrite it if trend becomes neutral and logs that.
                // For now, _lastTrendContextLogMessage persists if trend becomes neutral AND DebugLog doesn't update it for neutral.
                // Let's assume DebugLog correctly updates _lastTrendContextLogMessage to a "neutral" or "no trend" message if IsStrongTrend returns false and logs that.
                return;
            }
            // DebugLog($"[DEBUG] ======= НОВЫЙ ТИК ======= {Server.Time} =======");
            

            // DebugLog($"[DEBUG] Настройки: MinRR={_minRR:F2}, MaxRR={_maxRR:F2}");
            
            if (_lastTradeDate.Date == Server.Time.Date)
            {
                // DebugLog($"[DEBUG] Уже была сделка сегодня, день пропускается");
                return;
            }
            
            bool shouldFindFractals = _asianFractals.Count == 0 || (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime > _lastH1BarTime);
            bool newH1BarJustLogged = (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime == _lastH1BarTime);

            if (_asianFractals.Count == 0 || (newH1BarJustLogged && Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date))
            {
                // DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию для тренда: {trendContext}...");
                FindAsianSessionFractals(trendContext);
            }
            
            // DebugLog($"[DEBUG] Проверка свипа фракталов для тренда: {trendContext}...");
            CheckFractalsSweep(trendContext);
            
            foreach (var fractal in _asianFractals.Where(f => f.IsSwept && !f.EntryDone && f.BosLevel.HasValue).ToList())
            {
                TradeType entryTradeType;
                if (trendContext == TrendContext.Bullish)
                {
                    entryTradeType = TradeType.Buy;
                }
                else if (trendContext == TrendContext.Bearish)
                {
                    entryTradeType = TradeType.Sell;
                }
                else
                {
                    continue; 
                }

                var bosResult = Is3mStructureBreak(fractal, trendContext);
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
                    
                    // Print the last determined trend context before attempting to enter position
                    if (!string.IsNullOrEmpty(_lastTrendContextLogMessage))
                    {
                        Print(_lastTrendContextLogMessage);
                        _lastTrendContextLogMessage = null; // Clear after printing, so it only prints once per trade entry attempt
                    }
                    
                    DebugLog($"[DEBUG_ONTICK_PRE_ENTER_POS] Attempting to call EnterPosition for fractal at {fractal.Time} with BOS Price {bosResult.EntryPrice}");
                    EnterPosition(entryTradeType, bosResult.EntryPrice, fractal);
                    DebugLog($"[DEBUG_ONTICK_POST_ENTER_POS] Returned from EnterPosition for fractal at {fractal.Time}");
                }
            }
            
            // DebugLog($"[DEBUG] ======= КОНЕЦ ТИКА ======= {Server.Time} =======");
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
            _asianFractals.Clear();
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            var today = Server.Time.Date;
            //DebugLog($"[DEBUG] FindAsianSessionFractals: Поиск для тренда {trendContext} с FractalPeriod = {FractalPeriod}");

            for (int i = FractalPeriod; i < h1Bars.Count - FractalPeriod; i++)
            {
                var barTime = h1Bars.OpenTimes[i];
                if (!(barTime.Date == today && barTime.Hour >= AsiaStartHour && barTime.Hour < AsiaEndHour))
                    continue;
                
                // Для бычьего тренда ищем нижние фракталы для лонгов
                if (trendContext == TrendContext.Bullish)
                {
                    if (!double.IsNaN(hourlyFractals.DownFractal[i]))
                    {
                        _asianFractals.Add(new AsianFractal
                        {
                            Level = hourlyFractals.DownFractal[i],
                            Time = barTime,
                            IsSwept = false
                        });
                        DebugLog($"[DEBUG] Найден бычий (нижний) фрактал: {hourlyFractals.DownFractal[i]:F5} в {barTime}");
                        // ИСПРАВЛЕНИЕ ЗДЕСЬ: убраны h1Open и т.д., так как они не нужны для этого события
                        LogChartEvent(barTime, "ASIAN_FRACTAL", price1: hourlyFractals.DownFractal[i], tradeType: "Bullish", notes: "Lower fractal");
                    }
                }
                // Для медвежьего тренда ищем верхние фракталы для шортов
                else if (trendContext == TrendContext.Bearish)
                {
                    if (!double.IsNaN(hourlyFractals.UpFractal[i]))
                    {
                        _asianFractals.Add(new AsianFractal
                        {
                            Level = hourlyFractals.UpFractal[i],
                            Time = barTime,
                            IsSwept = false
                        });
                        DebugLog($"[DEBUG] Найден медвежий (верхний) фрактал: {hourlyFractals.UpFractal[i]:F5} в {barTime}");
                        // ИСПРАВЛЕНИЕ ЗДЕСЬ: убраны h1Open и т.д.
                        LogChartEvent(barTime, "ASIAN_FRACTAL", price1: hourlyFractals.UpFractal[i], tradeType: "Bearish", notes: "Upper fractal");
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

            if (today == new DateTime(2025, 5, 14)) // MODIFIED: Reverted date for fractal logging
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
                        double distanceToBosLevelPips = (candidateBar.Close - fractal.BosLevel.Value) / Symbol.PipSize;
                        DebugLog($"[BOS_DEBUG_BULL] Candidate Bar {candidateBar.OpenTime} C: {candidateBar.Close} vs BOS Level (SweepBarHigh): {fractal.BosLevel.Value}. Dist: {distanceToBosLevelPips:F1} pips.");

                        if (distanceToBosLevelPips <= MaxBOSDistancePips)
                        {
                            result.IsBreak = true;
                            result.EntryPrice = candidateBar.Close;
                            result.BreakTime = candidateBar.OpenTime;
                            DebugLog($"[BOS_SUCCESS_BULL] Bullish BOS Confirmed by M3 bar {candidateBar.OpenTime}. Close: {candidateBar.Close} > BOS Level: {fractal.BosLevel.Value}. Entry at market.");
                            return result;
                        }
                        else
                        {
                            DebugLog($"[BOS_REJECT_BULL] Bullish BOS attempt on bar {candidateBar.OpenTime} rejected. Distance {distanceToBosLevelPips:F1} pips > MaxBOSDistancePips ({MaxBOSDistancePips}). BOS Level: {fractal.BosLevel.Value}, Close: {candidateBar.Close}. Fractal invalidated for future entries.");
                            fractal.EntryDone = true;
                            return result;
                        }
                    }
                }
                else if (trendContext == TrendContext.Bearish || TrendMode == ManualTrendMode.Bearish)
                {
                    if (candidateBar.Close < fractal.BosLevel.Value)
                    {
                        double distanceToBosLevelPips = (fractal.BosLevel.Value - candidateBar.Close) / Symbol.PipSize;
                        DebugLog($"[BOS_DEBUG_BEAR] Candidate Bar {candidateBar.OpenTime} C: {candidateBar.Close} vs BOS Level (SweepBarLow): {fractal.BosLevel.Value}. Dist: {distanceToBosLevelPips:F1} pips.");
                        
                        if (distanceToBosLevelPips <= MaxBOSDistancePips)
                        {
                            result.IsBreak = true;
                            result.EntryPrice = candidateBar.Close;
                            result.BreakTime = candidateBar.OpenTime;
                            DebugLog($"[BOS_SUCCESS_BEAR] Bearish BOS Confirmed by M3 bar {candidateBar.OpenTime}. Close: {candidateBar.Close} < BOS Level: {fractal.BosLevel.Value}. Entry at market.");
                            return result;
                        }
                        else
                        {
                             DebugLog($"[BOS_REJECT_BEAR] Bearish BOS attempt on bar {candidateBar.OpenTime} rejected. Distance {distanceToBosLevelPips:F1} pips > MaxBOSDistancePips ({MaxBOSDistancePips}). BOS Level: {fractal.BosLevel.Value}, Close: {candidateBar.Close}. Fractal invalidated for future entries.");
                            fractal.EntryDone = true;
                            return result;
                        }
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
            context = EvaluateTrendContextCriteria(); // Changed to call the new method
            return context != TrendContext.Neutral;
        }

        private void LogChartEvent(DateTime timestamp, string eventType, double? h1Open = null, double? h1High = null, double? h1Low = null, double? h1Close = null, double? price1 = null, double? price2 = null, string tradeType = "", string notes = "")
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

                // Очистка кnotes от запятых и кавычек во избежание проблем с CSV
                // Заменяем также точку с запятой в notes, так как она теперь наш основной разделитель
                string sanitizedNotes = notes?.Replace(";", ":")?.Replace(",", ".")?.Replace("\"", "'") ?? "";

                // Изменяем разделитель на точку с запятой
                _chartDataWriter.WriteLine($"{timestamp:yyyy-MM-ddTHH:mm:ss};{eventType};{h1OpenStr};{h1HighStr};{h1LowStr};{h1CloseStr};{price1Str};{price2Str};{tradeType};{sanitizedNotes}");
            }
            catch (Exception ex)
            {
                Print($"Error writing to chart data file: {ex.Message}");
            }
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
    
