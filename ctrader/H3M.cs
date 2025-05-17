using System;
using System.Linq;
using System.Collections.Generic;
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

        [Parameter("Stop Loss Buffer Pips", DefaultValue = 5)]
        public double StopLossBufferPips { get; set; }

        [Parameter("Min RR", DefaultValue = 1.3)]
        public double MinRR { get; set; }

        [Parameter("Max RR", DefaultValue = 5.0)]
        public double MaxRR { get; set; }

        [Parameter("Max BOS Distance Pips", DefaultValue = 15.0)]
        public double MaxBOSDistancePips { get; set; }

        [Parameter("Manual Trend Mode", DefaultValue = ManualTrendMode.Auto)]
        public ManualTrendMode TrendMode { get; set; }

        private Fractals _fractals;
        private List<AsianFractal> _asianFractals = new List<AsianFractal>();
        private DateTime _lastAsianSessionCheck = DateTime.MinValue;
        private bool _isAsianSession = false;
        private double? _currentFractalLevel = null;
        private Bars _m3Bars;
        private Bars _h1Bars;
        private DateTime _lastH1BarTime = DateTime.MinValue; // <<< ДОБАВЛЕНО
        // --- Asia session time helpers ---
        private static readonly int AsiaStartHour = 0; // 00:00 UTC+3
        private static readonly int AsiaEndHour = 9;   // 09:00 UTC+3
        private DateTime _lastTradeDate = DateTime.MinValue;
        private bool _loggedSpecificBarData = false; // Флаг, чтобы залогировать данные только один раз
        private bool _loggedSpecificBarDataThisInstance = false; // Added for specific bar logging
        private DateTime _debugSpecificTimestamp = DateTime.MinValue; // Declare _debugSpecificTimestamp
        private HashSet<DateTime> _loggedOHLCBarsForTargetDate = new HashSet<DateTime>(); // For M3 OHLC logging

        protected override void OnStart()
        {
            _fractals = Indicators.Fractals(Bars, FractalPeriod);
            _m3Bars = MarketData.GetBars(TimeFrame.Minute3);
            _h1Bars = MarketData.GetBars(TimeFrame.Hour);

            // For 14.05.2025 debugging:
            _debugSpecificTimestamp = new DateTime(2025, 5, 14, 6, 15, 0, DateTimeKind.Utc); // For Is3mStructureBreak logging of 06:15 bar
            _loggedOHLCBarsForTargetDate.Clear(); // Reset for this run
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
            // Create hourly fractals
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            
            for (int i = 2; i < h1Bars.Count - 2; i++)
            {
                if (tradeType == TradeType.Buy && !double.IsNaN(hourlyFractals.UpFractal[i]))
                {
                    var level = hourlyFractals.UpFractal[i];
                    var distance = level - entryPrice;
                    if (distance > 0 && distance < minDistance)
                    {
                        minDistance = distance;
                        nearestLevel = level;
                    }
                }
                if (tradeType == TradeType.Sell && !double.IsNaN(hourlyFractals.DownFractal[i]))
                {
                    var level = hourlyFractals.DownFractal[i];
                    var distance = entryPrice - level;
                    if (distance > 0 && distance < minDistance)
                    {
                        minDistance = distance;
                        nearestLevel = level;
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
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            
            for (int i = 2; i < h1Bars.Count - 2; i++)
            {
                if (tradeType == TradeType.Buy && !double.IsNaN(hourlyFractals.UpFractal[i]))
                {
                    var level = hourlyFractals.UpFractal[i];
                    var distance = level - entryPrice;
                    // For buy, find next fractal higher than the first one
                    if (distance > 0 && level > firstFractalLevel && distance < minDistance)
                    {
                        minDistance = distance;
                        nextLevel = level;
                    }
                }
                if (tradeType == TradeType.Sell && !double.IsNaN(hourlyFractals.DownFractal[i]))
                {
                    var level = hourlyFractals.DownFractal[i];
                    var distance = entryPrice - level;
                    // For sell, find next fractal lower than the first one
                    if (distance > 0 && level < firstFractalLevel && distance < minDistance)
                    {
                        minDistance = distance;
                        nextLevel = level;
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

            // 1. Ищем ближайший H1 фрактал для TP
            double? firstTpCandidate = FindNearestH1FractalForTP(tradeType, entryPrice);
            DebugLog($"[TP_DEBUG] Nearest H1 Fractal for TP: {firstTpCandidate?.ToString("F5") ?? "N/A"}");

            if (firstTpCandidate.HasValue)
            {
                double rr1 = Math.Abs(firstTpCandidate.Value - entryPrice) / stopLossDistance;
                DebugLog($"[TP_DEBUG] RR for First TP Candidate ({firstTpCandidate.Value:F5}): {rr1:F2}");

                if (rr1 >= MinRR && rr1 <= MaxRR)
                {
                    DebugLog($"[TP_DEBUG] First TP ({firstTpCandidate.Value:F5}) is SUITABLE. Using it.");
                    return (firstTpCandidate.Value, rr1);
                }
                else if (rr1 < MinRR)
                {
                    DebugLog($"[TP_DEBUG] RR for First TP ({firstTpCandidate.Value:F5}) is TOO LOW ({rr1:F2} < {MinRR:F2}). Searching for next H1 fractal.");
                    double? secondTpCandidate = TryFindNextH1Fractal(tradeType, entryPrice, firstTpCandidate.Value);
                    DebugLog($"[TP_DEBUG] Next H1 Fractal for TP: {secondTpCandidate?.ToString("F5") ?? "N/A"}");
                    if (secondTpCandidate.HasValue)
                    {
                        double rr2 = Math.Abs(secondTpCandidate.Value - entryPrice) / stopLossDistance;
                        DebugLog($"[TP_DEBUG] RR for Second TP Candidate ({secondTpCandidate.Value:F5}): {rr2:F2}");

                        if (rr2 >= MinRR && rr2 <= MaxRR)
                        {
                            DebugLog($"[TP_DEBUG] Second TP ({secondTpCandidate.Value:F5}) is SUITABLE. Using it.");
                            return (secondTpCandidate.Value, rr2);
                        }
                        else
                        {
                            DebugLog($"[TP_DEBUG] RR for Second TP ({secondTpCandidate.Value:F5}) is NOT SUITABLE ({rr2:F2}). Valid RR Range: {MinRR:F2}-{MaxRR:F2}. Entry might be cancelled.");
                            return (null, rr2); 
                        }
                    }
                    else
                    {
                        DebugLog($"[TP_DEBUG] No next H1 fractal found. First TP ({firstTpCandidate.Value:F5}) had RR={rr1:F2}. Entry might be cancelled.");
                        return (null, rr1); 
                    }
                }
                else // rr1 > MaxRR
                {
                    DebugLog($"[TP_DEBUG] RR for First TP ({firstTpCandidate.Value:F5}) is TOO HIGH ({rr1:F2} > {MaxRR:F2}). Entry might be cancelled.");
                    return (null, rr1);
                }
            }
            else
            {
                DebugLog($"[TP_DEBUG] No H1 fractals found for TP. Entry might be cancelled based on other TP logic (if any) or lack of TP.");
                return (null, 0);
            }
        }

     

       

        private bool IsAsianSession()
        {
            var serverTime = Server.Time;
            var hour = serverTime.Hour;
            
            // Asian session: 00:00 - 09:00 UTC
            return hour >= 0 && hour < 9;
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
            _loggedSpecificBarDataThisInstance = false; // Reset this flag at the start of each OnTick

            // --- Logging specific M3 bars for 14.05.2025 ---
            if (Server.Time.Date == new DateTime(2025, 5, 14) && _m3Bars.Count > 0)
            {
                var targetTimesToLog = new List<DateTime>
                {
                    new DateTime(2025, 5, 14, 6, 3, 0, DateTimeKind.Utc),
                    new DateTime(2025, 5, 14, 6, 15, 0, DateTimeKind.Utc)
                };

                // Check the last few M3 bars to see if they match our target times
                for (int i = 0; i < Math.Min(5, _m3Bars.Count); i++) 
                {
                    var bar = _m3Bars.Last(i);
                    if (targetTimesToLog.Contains(bar.OpenTime) && !_loggedOHLCBarsForTargetDate.Contains(bar.OpenTime))
                    {
                        DebugLog($"[DEBUG_OHLC_M3] Time: {bar.OpenTime:HH:mm:ss}, O={bar.Open:F5}, H={bar.High:F5}, L={bar.Low:F5}, C={bar.Close:F5}");
                        _loggedOHLCBarsForTargetDate.Add(bar.OpenTime);
                    }
                }
            }
            // --- End of specific M3 bar logging ---

            if (Bars.TimeFrame != TimeFrame.Hour) // Убедимся, что OnTick работает на основном таймфрейме робота (H1)
            {
                DebugLog($"[DEBUG] Неверный таймфрейм для OnTick: {Bars.TimeFrame}");
                return;
            }

            TrendContext trendContext; // Объявляем переменную для тренда
            var currentPrice = Symbol.Bid; // Для лонгов используем Bid, для шортов Ask

            if (Server.Time.Date != _lastAsianSessionCheck) // Проверяем один раз в день в начале дня
            {
                CheckAsianSession(); // Определяем, находимся ли мы в Азиатской сессии
                _lastAsianSessionCheck = Server.Time.Date;
                //DebugLog($"[DEBUG] MaxTrackedFractals Parameter Value: {MaxTrackedFractals}"); // <--- ЗАКОММЕНТИРОВАНО
            }

            if (!IsStrongTrend(out trendContext))
            {
                DebugLog($"[DEBUG] Нет очевидного тренда, день пропускается");
                return;
            }
            DebugLog($"[DEBUG] ======= НОВЫЙ ТИК ======= {Server.Time} =======");
            DebugLog($"[DEBUG] Текущий тренд: {trendContext}, цена = {currentPrice:F5}, время = {Server.Time}");
            

            DebugLog($"[DEBUG] Настройки: MinRR={MinRR:F2}, MaxRR={MaxRR:F2}");
            
            if (_lastTradeDate.Date == Server.Time.Date)
            {
                DebugLog($"[DEBUG] Уже была сделка сегодня, день пропускается");
                return;
            }
            
            // --- Оптимизация вызова FindAsianSessionFractals ---
            bool shouldFindFractals = _asianFractals.Count == 0 || (_h1Bars.Count > 0 && _h1Bars.Last(0).OpenTime > _lastH1BarTime);
            if (shouldFindFractals && Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date) // Дополнительно проверяем, что не ищем для прошлого дня, если уже есть фракталы
            {
                DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию для тренда: {trendContext}...");
                FindAsianSessionFractals(trendContext);
                if (_h1Bars.Count > 0)
                {
                    _lastH1BarTime = _h1Bars.Last(0).OpenTime; // Обновляем время последнего H1 бара
                }
            }
            else if (_asianFractals.Count > 0 && Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date)
            {
                // Если текущая дата не совпадает с датой найденных фракталов, значит наступил новый день,
                // и нужно очистить старые фракталы и запустить поиск новых.
                DebugLog($"[DEBUG] Наступил новый день ({Server.Time.Date}), а фракталы от {_asianFractals.FirstOrDefault()?.Time.Date}. Очистка и поиск новых.");
                _asianFractals.Clear();
                _lastH1BarTime = DateTime.MinValue; // Сбрасываем время H1 бара для нового дня
                FindAsianSessionFractals(trendContext); // Поиск для нового дня
                if (_h1Bars.Count > 0)
                {
                    _lastH1BarTime = _h1Bars.Last(0).OpenTime;
                }
            }
            // --- Конец оптимизации ---
            
            DebugLog($"[DEBUG] Проверка свипа фракталов для тренда: {trendContext}...");
            CheckFractalsSweep(trendContext);
            
            DebugLog($"[DEBUG] ======= КОНЕЦ ТИКА ======= {Server.Time} =======");
        }

        protected override void OnStop()
        {
            // Handle cBot stop here
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
                    }
                }
            }
            
            // Сортируем фракталы
            var currentPrice = Bars.ClosePrices.Last(0); // <--- ОБЪЯВЛЕНИЕ currentPrice ВОЗВРАЩЕНО
            if (trendContext == TrendContext.Bullish)
            {
                _asianFractals = _asianFractals.OrderBy(f => f.Level).ToList(); // Closest (lowest) for bullish
            }
            else if (trendContext == TrendContext.Bearish)
            {
                _asianFractals = _asianFractals.OrderByDescending(f => f.Level).ToList(); // Closest (highest) for bearish
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
            var m3Bars = _m3Bars; // MarketData.GetBars(TimeFrame.Minute3);
            if (m3Bars.Count < FractalPeriod * 2 + 1) return;

            var currentM3Bar = m3Bars.Last(1); // Last completed M3 bar

            foreach (var fractal in _asianFractals.Where(f => !f.IsSwept && !f.EntryDone).ToList())
            {
                // Ensure we only check sweeps during or after the fractal formation time
                if (currentM3Bar.OpenTime < fractal.Time) continue;

                bool sweptThisTick = false;

                if (trendContext == TrendContext.Bullish || TrendMode == ManualTrendMode.Bullish) // Looking for bullish entries
                {
                    // Check for sweep of LOW fractal
                    if (currentM3Bar.Low < fractal.Level)
                    {
                        fractal.IsSwept = true;
                        sweptThisTick = true;
                        fractal.SweepLevel = fractal.Level;
                        fractal.SweepExtreme = currentM3Bar.Low; // Low of the bar that swept
                        fractal.SweepBarIndex = m3Bars.Count - 2; // Index of the bar that swept (currentM3Bar is Last(1), so its index is Count-2 in 0-based)

                        // BOS level is the HIGH of the sweep bar
                        fractal.BosLevel = currentM3Bar.High; 
                        fractal.LastBosCheckBarIndex = m3Bars.Count -2; // Reset BOS check index for this fractal

                        DebugLog($"[SWEEP_BULL] Low fractal {fractal.Level} at {fractal.Time} swept by M3 bar {currentM3Bar.OpenTime}. Bar L: {currentM3Bar.Low}, H: {currentM3Bar.High}. New BOS Level: {fractal.BosLevel}");
                    }
                }
                else if (trendContext == TrendContext.Bearish || TrendMode == ManualTrendMode.Bearish) // Looking for bearish entries
                {
                    // Check for sweep of HIGH fractal
                    if (currentM3Bar.High > fractal.Level)
                    {
                        fractal.IsSwept = true;
                        sweptThisTick = true;
                        fractal.SweepLevel = fractal.Level;
                        fractal.SweepExtreme = currentM3Bar.High; // High of the bar that swept
                        fractal.SweepBarIndex = m3Bars.Count - 2;

                        // BOS level is the LOW of the sweep bar
                        fractal.BosLevel = currentM3Bar.Low;
                        fractal.LastBosCheckBarIndex = m3Bars.Count - 2;

                        DebugLog($"[SWEEP_BEAR] High fractal {fractal.Level} at {fractal.Time} swept by M3 bar {currentM3Bar.OpenTime}. Bar H: {currentM3Bar.High}, L: {currentM3Bar.Low}. New BOS Level: {fractal.BosLevel}");
                    }
                }

                if (sweptThisTick)
                {
                    // Additional logic after a sweep can be placed here if needed immediately
                }
            }
        }

      

       

        
        private bool IsInFrankfurtSession(DateTime time)
        {
            // Frankfurt: 09:00 - 10:00 local time (e.g. UTC+3)
            // Corresponds to 06:00 - 07:00 UTC
            int hour = time.Hour; // time is UTC
            return hour >= 6 && hour < 7;
        }
        private bool IsInLondonSession(DateTime time)
        {
            // London: 10:00 - 15:00 local time (e.g. UTC+3)
            // Corresponds to 07:00 - 12:00 UTC
            int hour = time.Hour; // time is UTC
            return hour >= 7 && hour < 12;
        }

        // Добавим новый метод для поиска ключевых уровней
        

        private bool IsStrongTrend(out TrendContext context)
        {
            context = SimpleTrendContext();
            return context != TrendContext.Neutral;
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