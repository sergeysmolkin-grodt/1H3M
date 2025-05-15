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
    }

    [Robot(AccessRights = AccessRights.None, AddIndicators = true)]
    public class H3M : Robot
    {
        [Parameter("EMA Period", DefaultValue = 200)]
        public int EmaPeriod { get; set; }

        [Parameter("Fractal Period", DefaultValue = 3)]
        public int FractalPeriod { get; set; }

        [Parameter("Risk Percent", DefaultValue = 1.0)]
        public double RiskPercent { get; set; }

        [Parameter("Stop Loss Buffer Pips", DefaultValue = 5)]
        public double StopLossBufferPips { get; set; }

        [Parameter("Min RR", DefaultValue = 1.3)]
        public double MinRR { get; set; }

        [Parameter("Max RR", DefaultValue = 3.0)]
        public double MaxRR { get; set; }

        private ExponentialMovingAverage _ema;
        private Fractals _fractals;
        private List<AsianFractal> _asianFractals = new List<AsianFractal>();
        private DateTime _lastAsianSessionCheck = DateTime.MinValue;
        private bool _isAsianSession = false;
        private double? _currentFractalLevel = null;
        private Bars _m3Bars;
        private Bars _h1Bars;
        // --- Asia/Frankfurt session time helpers ---
        private static readonly int AsiaStartHour = 0; // 00:00 UTC+3
        private static readonly int AsiaEndHour = 9;   // 09:00 UTC+3
        private static readonly int FrankfurtStartHour = 9; // 09:00 UTC+3
        private static readonly int FrankfurtEndHour = 10;  // 10:00 UTC+3
        private static readonly int LondonStartHour = 10; // 10:00 UTC+3
        private static readonly int LondonEndHour = 15;   // 15:00 UTC+3

        protected override void OnStart()
        {
            _ema = Indicators.ExponentialMovingAverage(Bars.ClosePrices, EmaPeriod);
            _fractals = Indicators.Fractals(Bars, FractalPeriod);
            _m3Bars = MarketData.GetBars(TimeFrame.Minute3);
            _h1Bars = MarketData.GetBars(TimeFrame.Hour);
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

        // --- Find next H1 fractal after the nearest one ---
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
            var stopLossDistance = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;
            var takeProfitLevel = FindNearestH1FractalForTP(tradeType, entryPrice);
            if (takeProfitLevel == null) return (null, 0);
            
            var takeProfitDistance = Math.Abs(takeProfitLevel.Value - entryPrice) / Symbol.PipSize;
            var rr = takeProfitDistance / stopLossDistance;
            
            Print($"Первый найденный уровень TP: {takeProfitLevel:F5} с RR = {rr:F2}");
            
            // Всегда пытаемся найти следующий фрактал для лучшего RR
            var nextFractal = TryFindNextH1Fractal(tradeType, entryPrice, takeProfitLevel.Value);
            if (nextFractal != null)
            {
                var nextTpDistance = Math.Abs(nextFractal.Value - entryPrice) / Symbol.PipSize;
                var nextRr = nextTpDistance / stopLossDistance;
                Print($"Следующий уровень TP: {nextFractal:F5} с RR = {nextRr:F2}");
                
                // Используем следующий фрактал если он дает лучший RR и находится в допустимом диапазоне
                if (nextRr > rr && nextRr <= MaxRR)
                {
                    Print($"Используем следующий уровень TP так как он дает лучший RR в пределах MaxRR");
                    takeProfitLevel = nextFractal;
                    rr = nextRr;
                }
            }
            
            // RR фильтр
            if (rr < MinRR || rr > MaxRR)
            {
                Print($"Финальный RR {rr:F2} вне разрешенного диапазона ({MinRR}-{MaxRR})");
                return (null, rr);
            }
            
            Print($"Финальный уровень TP: {takeProfitLevel:F5} с RR = {rr:F2}");
            return (takeProfitLevel.Value, rr);
        }

        private double CalculateStopLoss(TradeType tradeType, double sweptFractalLevel)
        {
            if (tradeType == TradeType.Buy)
            {
                // For buy orders, place stop loss below the swept fractal
                return sweptFractalLevel - (StopLossBufferPips * Symbol.PipSize);
            }
            else
            {
                // For sell orders, place stop loss above the swept fractal
                return sweptFractalLevel + (StopLossBufferPips * Symbol.PipSize);
            }
        }

        private void EnterPosition(TradeType tradeType)
        {
            if (Positions.Find("H3M") != null) { DebugLog("[DEBUG] Уже есть открытая позиция H3M"); return; }

            var entryPrice = tradeType == TradeType.Buy ? Symbol.Ask : Symbol.Bid;
            
            // Находим последний свипнутый фрактал
            var lastSweptFractal = _asianFractals.LastOrDefault(f => f.IsSwept && f.SweepLevel.HasValue);
            if (lastSweptFractal == null) { DebugLog("[DEBUG] Нет свипнутого фрактала для входа"); return; }
            
            // Логируем процесс входа для отладки
            DebugLog($"[DEBUG] Попытка входа в {tradeType} позицию после свипа фрактала {lastSweptFractal.Level:F5}");
            DebugLog($"[DEBUG] Уровень свипа: {lastSweptFractal.SweepLevel:F5}, текущая цена: {entryPrice:F5}");
            
            var stopLossPrice = CalculateStopLoss(tradeType, lastSweptFractal.Level);
            var (takeProfitPrice, rr) = CalculateTakeProfit(tradeType, entryPrice, stopLossPrice);
            
            DebugLog($"[DEBUG] Расчет RR: Цена входа={entryPrice:F5}, SL={stopLossPrice:F5}, TP={takeProfitPrice:F5}, RR={rr:F2}");
            
            if (takeProfitPrice == null)
            {
                DebugLog($"[DEBUG] Пропуск входа - RR {rr:F2} вне диапазона ({MinRR}-{MaxRR})");
                return;
            }

            var positionSize = CalculatePositionSize(Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize);
            if (positionSize == 0) { DebugLog("[DEBUG] Размер позиции 0, вход невозможен"); return; }

            DebugLog($"[DEBUG] Вход в позицию: {tradeType}, entry={entryPrice}, SL={stopLossPrice}, TP={takeProfitPrice}, RR={rr:F2}, size={positionSize}");
            var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, "H3M", stopLossPrice, takeProfitPrice);
            
            if (result.IsSuccessful)
            {
                DebugLog($"[DEBUG] Вход выполнен: {tradeType} по {result.Position.EntryPrice}");
                DebugLog($"[DEBUG] Stop Loss: {stopLossPrice} (за фракталом {lastSweptFractal.Level})");
                DebugLog($"[DEBUG] Take Profit: {takeProfitPrice}");
                DebugLog($"[DEBUG] Risk/Reward: {rr:F2}");
                
                // Очищаем фракталы после успешного входа
                _asianFractals.Clear();
            }
            else
            {
                DebugLog($"[DEBUG] Ошибка входа: {result.Error}");
            }
        }

        private bool IsAsianSession()
        {
            var serverTime = Server.Time;
            var hour = serverTime.Hour;
            
            // Asian session: 00:00 - 09:00 UTC
            return hour >= 0 && hour < 9;
        }

        private bool IsLondonOrFrankfurtSession()
        {
            var serverTime = Server.Time;
            var hour = serverTime.Hour;
            
            // London: 08:00 - 16:00 UTC
            // Frankfurt: 07:00 - 15:00 UTC
            return hour >= 7 && hour < 16;
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

        private TrendContext DetermineTrendContext()
        {
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
            var hour = Server.Time.Hour;
            // Последний час Азии (AsiaEndHour - 1), Франкфурт, Лондон
            if (hour == AsiaEndHour - 1 || (hour >= FrankfurtStartHour && hour < FrankfurtEndHour) || (hour >= LondonStartHour && hour < LondonEndHour))
                Print(message);
        }

        protected override void OnTick()
        {
            var trendContext = DetermineTrendContext();
            DebugLog($"[DEBUG] Текущий тренд: {trendContext}, цена = {Symbol.Bid:F5}, время = {Server.Time}");

            // 1. Найти фракталы в азиатскую сессию (раз в день)
            if (_asianFractals.Count == 0 || Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date)
            {
                DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию...");
                FindAsianSessionFractals();
            }
            
            // 2. Проверить свип фракталов
            DebugLog($"[DEBUG] Проверка свипа фракталов...");
            CheckFractalsSweep();
            
            // Выводим текущее состояние фракталов после проверки свипа
            foreach (var fractal in _asianFractals)
            {
                DebugLog($"[DEBUG] Фрактал: {fractal.Level:F5} время: {fractal.Time}, свипнут={fractal.IsSwept}, уровень свипа={fractal.SweepLevel:F5}");
            }
            
            // Если есть свипнутые фракталы, проверяем структурный слом
            var sweptFractals = _asianFractals.Where(f => f.IsSwept && f.SweepLevel.HasValue).ToList();
            if (sweptFractals.Any())
            {
                // 3. После свипа ждать слом на 3м
                DebugLog($"[DEBUG] Найдены свипнутые фракталы ({sweptFractals.Count}). Проверка слома структуры на 3м...");
                if (Is3mStructureBreak())
                {
                    DebugLog($"[DEBUG] Обнаружен слом структуры! Попытка входа...");
                    if (trendContext == TrendContext.Bullish)
                    {
                        DebugLog($"[DEBUG] Вход в лонг...");
                        EnterPosition(TradeType.Buy);
                    }
                    else if (trendContext == TrendContext.Bearish)
                    {
                        DebugLog($"[DEBUG] Вход в шорт...");
                        EnterPosition(TradeType.Sell);
                    }
                    else
                    {
                        DebugLog($"[DEBUG] Нейтральный тренд, вход невозможен");
                    }
                }
                else
                {
                    DebugLog($"[DEBUG] Слом структуры не обнаружен");
                }
            }
            else
            {
                DebugLog($"[DEBUG] Нет свипнутых фракталов, ожидание свипа");
            }
        }

        protected override void OnStop()
        {
            // Handle cBot stop here
        }

        private void FindAsianSessionFractals()
        {
            _asianFractals.Clear();
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            var today = Server.Time.Date;
            var yesterday = today.AddDays(-1);
            var trendContext = DetermineTrendContext();
            int found = 0;
            
            DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию. Текущий тренд: {trendContext}");
            
            // Ищем фракталы в Азиатскую сессию (0:00-9:00 UTC+3)
            for (int i = 2; i < h1Bars.Count - 2; i++)
            {
                var barTime = h1Bars.OpenTimes[i];
                
                // Ищем фракталы только сегодня или вчера (если сейчас утро)
                bool isInTimeFrame = 
                    (barTime.Date == today && barTime.Hour >= AsiaStartHour && barTime.Hour < AsiaEndHour) ||
                    (barTime.Date == yesterday && Server.Time.Hour < 12); // Расширяем поиск на вчерашний день если сейчас первая половина дня
                
                if (!isInTimeFrame) continue;
                
                DebugLog($"[DEBUG] Проверка бара {i}: время={barTime}, в азиатской сессии={IsInAsiaSession(barTime)}");
                
                // Для бычьего тренда ищем нижние фракталы для лонгов
                if (trendContext == TrendContext.Bullish && !double.IsNaN(hourlyFractals.DownFractal[i]))
                {
                    _asianFractals.Add(new AsianFractal 
                    { 
                        Level = hourlyFractals.DownFractal[i],
                        Time = barTime,
                        IsSwept = false
                    });
                    found++;
                    DebugLog($"[DEBUG] Найден бычий (нижний) фрактал: {hourlyFractals.DownFractal[i]:F5} в {barTime}");
                }
                // Для медвежьего тренда ищем верхние фракталы для шортов
                else if (trendContext == TrendContext.Bearish && !double.IsNaN(hourlyFractals.UpFractal[i]))
                {
                    _asianFractals.Add(new AsianFractal 
                    { 
                        Level = hourlyFractals.UpFractal[i],
                        Time = barTime,
                        IsSwept = false
                    });
                    found++;
                    DebugLog($"[DEBUG] Найден медвежий (верхний) фрактал: {hourlyFractals.UpFractal[i]:F5} в {barTime}");
                }
            }
            
            // Если не нашли фракталы строго в азиатскую сессию, расширяем поиск
            if (found == 0)
            {
                DebugLog($"[DEBUG] Не найдены фракталы в азиатскую сессию, расширяем поиск на последние 24 часа");
                for (int i = 2; i < Math.Min(h1Bars.Count - 2, 24); i++)
                {
                    var barTime = h1Bars.OpenTimes[i];
                    
                    // Для бычьего тренда ищем нижние фракталы для лонгов
                    if (trendContext == TrendContext.Bullish && !double.IsNaN(hourlyFractals.DownFractal[i]))
                    {
                        _asianFractals.Add(new AsianFractal 
                        { 
                            Level = hourlyFractals.DownFractal[i],
                            Time = barTime,
                            IsSwept = false
                        });
                        found++;
                        DebugLog($"[DEBUG] Найден бычий (нижний) фрактал (запасной): {hourlyFractals.DownFractal[i]:F5} в {barTime}");
                    }
                    // Для медвежьего тренда ищем верхние фракталы для шортов
                    else if (trendContext == TrendContext.Bearish && !double.IsNaN(hourlyFractals.UpFractal[i]))
                    {
                        _asianFractals.Add(new AsianFractal 
                        { 
                            Level = hourlyFractals.UpFractal[i],
                            Time = barTime,
                            IsSwept = false
                        });
                        found++;
                        DebugLog($"[DEBUG] Найден медвежий (верхний) фрактал (запасной): {hourlyFractals.UpFractal[i]:F5} в {barTime}");
                    }
                }
            }
            
            // Сортируем фракталы по близости к текущей цене
            var currentPrice = Bars.ClosePrices.Last(0);
            _asianFractals = _asianFractals.OrderBy(f => Math.Abs(f.Level - currentPrice)).ToList();
            
            DebugLog($"[DEBUG] Найдено фракталов: {found}. Текущая цена: {currentPrice:F5}");
            foreach (var fractal in _asianFractals)
            {
                DebugLog($"[DEBUG] Фрактал: {fractal.Level:F5} время: {fractal.Time}, разница с ценой: {Math.Abs(fractal.Level - currentPrice) / Symbol.PipSize:F1} пипсов");
            }
        }

        private void CheckFractalsSweep()
        {
            if (_asianFractals.Count == 0) { DebugLog("[DEBUG] Нет фракталов для свипа"); return; }
            var now = Server.Time;
            if (!IsInFrankfurtSession(now) && !IsInLondonSession(now)) { DebugLog("[DEBUG] Не Франкфурт и не Лондон сессия"); return; }
            
            var price = Bars.ClosePrices.Last(0);
            var emaValue = _ema.Result.Last(0);
            var context = DetermineTrendContext();
            
            DebugLog($"[DEBUG] Проверка свипа фракталов. Найдено фракталов: {_asianFractals.Count}");
            DebugLog($"[DEBUG] Текущая цена: {price:F5}, EMA: {emaValue:F5}");
            
            foreach (var fractal in _asianFractals)
            {
                DebugLog($"[DEBUG] Фрактал: уровень={fractal.Level:F5}, свипнут={fractal.IsSwept}");
            }
            
            foreach (var fractal in _asianFractals.Where(f => !f.IsSwept))
            {
                // Для лонгов проверяем свип нижних фракталов (цена должна быть ВЫШЕ фрактала)
                if (context == TrendContext.Bullish)
                {
                    // Проверяем, была ли цена выше уровня фрактала
                    bool priceWentAbove = false;
                    for (int i = 0; i < Math.Min(5, _m3Bars.Count); i++)
                    {
                        if (_m3Bars.HighPrices.Last(i) >= fractal.Level)
                        {
                            priceWentAbove = true;
                            DebugLog($"[DEBUG] Цена {_m3Bars.HighPrices.Last(i):F5} прошла выше фрактала {fractal.Level:F5} на 3м баре {i}");
                            break;
                        }
                    }
                    
                    if (priceWentAbove)
                    {
                        fractal.IsSwept = true;
                        fractal.SweepLevel = _m3Bars.HighPrices.Last(0);  // Сохраняем уровень свипа
                        DebugLog($"[DEBUG] Свип фрактала для лонга: {fractal.Level:F5} (время: {fractal.Time}, текущая цена: {price:F5})");
                    }
                }
                // Для шортов проверяем свип верхних фракталов (цена должна быть НИЖЕ фрактала)
                else if (context == TrendContext.Bearish)
                {
                    // Проверяем, была ли цена ниже уровня фрактала
                    bool priceWentBelow = false;
                    for (int i = 0; i < Math.Min(5, _m3Bars.Count); i++)
                    {
                        if (_m3Bars.LowPrices.Last(i) <= fractal.Level)
                        {
                            priceWentBelow = true;
                            DebugLog($"[DEBUG] Цена {_m3Bars.LowPrices.Last(i):F5} прошла ниже фрактала {fractal.Level:F5} на 3м баре {i}");
                            break;
                        }
                    }
                    
                    if (priceWentBelow)
                    {
                        fractal.IsSwept = true;
                        fractal.SweepLevel = _m3Bars.LowPrices.Last(0);  // Сохраняем уровень свипа
                        DebugLog($"[DEBUG] Свип фрактала для шорта: {fractal.Level:F5} (время: {fractal.Time}, текущая цена: {price:F5})");
                    }
                }
            }
        }

        private bool Is3mStructureBreak()
        {
            var sweptFractals = _asianFractals.Where(f => f.IsSwept && f.SweepLevel.HasValue).ToList();
            if (!sweptFractals.Any()) { DebugLog("[DEBUG] Нет свипнутых фракталов для проверки слома"); return false; }
            
            var m3 = _m3Bars;
            if (m3.Count < 4) return false;
            
            // Получаем данные последних баров
            var lastIndex = m3.Count - 1;
            var last = new { 
                Open = m3.OpenPrices[lastIndex],
                High = m3.HighPrices[lastIndex],
                Low = m3.LowPrices[lastIndex],
                Close = m3.ClosePrices[lastIndex]
            };
            var prev = new {
                Open = m3.OpenPrices[lastIndex - 1],
                High = m3.HighPrices[lastIndex - 1],
                Low = m3.LowPrices[lastIndex - 1],
                Close = m3.ClosePrices[lastIndex - 1]
            };
            var prev2 = new {
                Open = m3.OpenPrices[lastIndex - 2],
                High = m3.HighPrices[lastIndex - 2],
                Low = m3.LowPrices[lastIndex - 2],
                Close = m3.ClosePrices[lastIndex - 2]
            };
            
            var context = DetermineTrendContext();
            
            if (context == TrendContext.Neutral) { DebugLog("[DEBUG] Тренд нейтральный, слом не проверяем"); return false; }
            
            // Более детальный лог
            DebugLog($"[DEBUG] Анализ свипнутого фрактала: {sweptFractals.Count}");
            foreach (var fractal in sweptFractals)
            {
                DebugLog($"[DEBUG] Свипнутый фрактал: уровень={fractal.Level:F5}, уровень свипа={fractal.SweepLevel:F5}");
            }
            
            // Calculate candle body and wick sizes
            double bodySize = Math.Abs(last.Close - last.Open);
            double upperWick = last.High - Math.Max(last.Open, last.Close);
            double lowerWick = Math.Min(last.Open, last.Close) - last.Low;
            double averageBodySize = (Math.Abs(prev.Close - prev.Open) + Math.Abs(prev2.Close - prev2.Open)) / 2;
            
            // Детальный лог свечей
            DebugLog($"[DEBUG] Последняя свеча: O={last.Open:F5}, H={last.High:F5}, L={last.Low:F5}, C={last.Close:F5}, тело={bodySize / Symbol.PipSize:F1} пипсов");
            DebugLog($"[DEBUG] Предыдущая свеча: O={prev.Open:F5}, H={prev.High:F5}, L={prev.Low:F5}, C={prev.Close:F5}");
            
            foreach (var fractal in sweptFractals)
            {
                // Слом структуры - перебитие уровня, который привел к свипу фрактала
                if (context == TrendContext.Bullish)
                {
                    // В лонговом контексте, цена должна закрыться выше уровня свипа фрактала
                    bool breakAboveSweepLevel = last.Close > fractal.SweepLevel.Value;
                    
                    if (breakAboveSweepLevel)
                    {
                        // Дополнительные проверки для подтверждения качества слома
                        bool isBullishCandle = last.Close > last.Open;
                        bool hasDecent3mVolume = bodySize > Symbol.PipSize * 2; // Минимальное тело 2 пипса
                        
                        if (isBullishCandle && hasDecent3mVolume)
                        {
                            DebugLog($"[DEBUG] 3м слом структуры: цена закрылась выше уровня свипа {fractal.SweepLevel:F5} с ценой {last.Close:F5}");
                            DebugLog($"[DEBUG] Бычья свеча с телом {bodySize / Symbol.PipSize:F1} пипсов");
                            return true;
                        }
                        else
                        {
                            DebugLog($"[DEBUG] Цена выше уровня свипа, но не соответствует дополнительным условиям: бычья={isBullishCandle}, объем={hasDecent3mVolume}");
                        }
                    }
                    else
                    {
                        DebugLog($"[DEBUG] Нет слома структуры: цена {last.Close:F5} не закрылась выше уровня свипа {fractal.SweepLevel:F5}");
                    }
                }
                else if (context == TrendContext.Bearish)
                {
                    // В шортовом контексте, цена должна закрыться ниже уровня свипа фрактала
                    bool breakBelowSweepLevel = last.Close < fractal.SweepLevel.Value;
                    
                    if (breakBelowSweepLevel)
                    {
                        // Дополнительные проверки для подтверждения качества слома
                        bool isBearishCandle = last.Close < last.Open;
                        bool hasDecent3mVolume = bodySize > Symbol.PipSize * 2; // Минимальное тело 2 пипса
                        
                        if (isBearishCandle && hasDecent3mVolume)
                        {
                            DebugLog($"[DEBUG] 3м слом структуры: цена закрылась ниже уровня свипа {fractal.SweepLevel:F5} с ценой {last.Close:F5}");
                            DebugLog($"[DEBUG] Медвежья свеча с телом {bodySize / Symbol.PipSize:F1} пипсов");
                            return true;
                        }
                        else
                        {
                            DebugLog($"[DEBUG] Цена ниже уровня свипа, но не соответствует дополнительным условиям: медвежья={isBearishCandle}, объем={hasDecent3mVolume}");
                        }
                    }
                    else
                    {
                        DebugLog($"[DEBUG] Нет слома структуры: цена {last.Close:F5} не закрылась ниже уровня свипа {fractal.SweepLevel:F5}");
                    }
                }
            }
            
            DebugLog("[DEBUG] Слом структуры не найден ни по одному фракталу");
            return false;
        }

        private bool IsInAsiaSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= AsiaStartHour && hour < AsiaEndHour;
        }
        private bool IsInFrankfurtSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= FrankfurtStartHour && hour < FrankfurtEndHour;
        }
        private bool IsInLondonSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= LondonStartHour && hour < LondonEndHour;
        }
    }

    public enum TrendContext
    {
        Bullish,
        Bearish,
        Neutral
    }
} 