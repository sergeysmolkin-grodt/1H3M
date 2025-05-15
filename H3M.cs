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
        // Добавляем флаг для контроля входа по текущему свипу
        private bool _entryDoneForCurrentSweep = false;
        private DateTime _lastTradeDate = DateTime.MinValue;

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
            DebugLog($"[DEBUG] Первый найденный уровень TP: {takeProfitLevel:F5} с RR = {rr:F2}");
            // Строгий RR фильтр: если RR < 1.3 — ищем следующий, если RR > 3.0 — не входить
            var nextFractal = TryFindNextH1Fractal(tradeType, entryPrice, takeProfitLevel.Value);
            while ((rr < MinRR || rr > MaxRR) && nextFractal != null)
            {
                takeProfitLevel = nextFractal;
                takeProfitDistance = Math.Abs(takeProfitLevel.Value - entryPrice) / Symbol.PipSize;
                rr = takeProfitDistance / stopLossDistance;
                DebugLog($"[DEBUG] Следующий уровень TP: {takeProfitLevel:F5} с RR = {rr:F2}");
                nextFractal = TryFindNextH1Fractal(tradeType, entryPrice, takeProfitLevel.Value);
            }
            if (rr < MinRR || rr > MaxRR)
            {
                DebugLog($"[DEBUG] Финальный RR {rr:F2} вне разрешенного диапазона ({MinRR}-{MaxRR}) — сделка не открывается");
                return (null, rr);
            }
            DebugLog($"[DEBUG] Финальный уровень TP: {takeProfitLevel:F5} с RR = {rr:F2}");
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

        private void EnterPosition(TradeType tradeType, double entryPrice)
        {
            if (Positions.Find("H3M") != null) { DebugLog("[DEBUG] Уже есть открытая позиция H3M"); return; }
            // Находим последний свипнутый фрактал
            var lastSweptFractal = _asianFractals.LastOrDefault(f => f.IsSwept && f.SweepLevel.HasValue);
            if (lastSweptFractal == null) { DebugLog("[DEBUG] Нет свипнутого фрактала для входа"); return; }
            DebugLog($"[DEBUG] =====ВХОД В ПОЗИЦИЮ===== Попытка входа в {tradeType} позицию после свипа фрактала {lastSweptFractal.Level:F5}");
            DebugLog($"[DEBUG] Уровень свипа: {lastSweptFractal.SweepLevel:F5}, цена входа: {entryPrice:F5}");
            var stopLossPrice = CalculateStopLoss(tradeType, lastSweptFractal.Level);
            var (takeProfitPrice, rr) = CalculateTakeProfit(tradeType, entryPrice, stopLossPrice);
            DebugLog($"[DEBUG] Расчет RR: Цена входа={entryPrice:F5}, SL={stopLossPrice:F5}, TP={takeProfitPrice:F5}, RR={rr:F2}");
            double positionSize = 0;
            if (takeProfitPrice == null)
            {
                DebugLog($"[DEBUG] TP не найден или RR {rr:F2} вне диапазона ({MinRR}-{MaxRR})");
                DebugLog($"[DEBUG] Попытка входа с минимальным размером позиции");
                positionSize = Symbol.VolumeInUnitsMin;
            }
            else
            {
                positionSize = CalculatePositionSize(Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize);
            }
            if (positionSize == 0) { DebugLog("[DEBUG] Размер позиции 0, вход невозможен"); return; }
            DebugLog($"[DEBUG] Вход в позицию: {tradeType}, entry={entryPrice}, SL={stopLossPrice}, TP={takeProfitPrice}, RR={rr:F2}, size={positionSize}");
            if (takeProfitPrice == null)
            {
                double stopLossDistance = Math.Abs(entryPrice - stopLossPrice);
                if (tradeType == TradeType.Buy)
                {
                    takeProfitPrice = entryPrice + (stopLossDistance * 2.0);
                }
                else
                {
                    takeProfitPrice = entryPrice - (stopLossDistance * 2.0);
                }
                DebugLog($"[DEBUG] Установка дефолтного TP с RR=2.0: {takeProfitPrice:F5}");
            }
            var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, "H3M", stopLossPrice, takeProfitPrice);
            if (result.IsSuccessful)
            {
                DebugLog($"[DEBUG] =====ВХОД УСПЕШЕН===== {tradeType} по {result.Position.EntryPrice}");
                DebugLog($"[DEBUG] Stop Loss: {stopLossPrice} (за фракталом {lastSweptFractal.Level})");
                DebugLog($"[DEBUG] Take Profit: {takeProfitPrice}");
                DebugLog($"[DEBUG] Risk/Reward: {rr:F2}");
                _asianFractals.Clear();
                _entryDoneForCurrentSweep = true;
                _lastTradeDate = Server.Time;
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
            TrendContext trendContext;
            if (!IsStrongTrend(out trendContext))
            {
                DebugLog($"[DEBUG] Нет очевидного тренда, день пропускается");
                return;
            }
            var currentPrice = Symbol.Bid;
            DebugLog($"[DEBUG] ======= НОВЫЙ ТИК ======= {Server.Time} =======");
            DebugLog($"[DEBUG] Текущий тренд: {trendContext}, цена = {currentPrice:F5}, время = {Server.Time}");
            if (_lastTradeDate.Date == Server.Time.Date)
            {
                DebugLog($"[DEBUG] Уже была сделка сегодня, день пропускается");
                return;
            }
            if (_asianFractals.Count == 0 || Server.Time.Date != _asianFractals.FirstOrDefault()?.Time.Date)
            {
                DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию...");
                FindAsianSessionFractals();
            }
            DebugLog($"[DEBUG] Проверка свипа фракталов...");
            CheckFractalsSweep();
            DebugLog($"[DEBUG] === Состояние фракталов после проверки свипа ===");
            foreach (var fractal in _asianFractals)
            {
                DebugLog($"[DEBUG] Фрактал: {fractal.Level:F5} время: {fractal.Time}, свипнут={fractal.IsSwept}, уровень свипа={fractal.SweepLevel:F5}");
            }
            var sweptFractals = _asianFractals.Where(f => f.IsSwept && f.SweepLevel.HasValue).ToList();
            if (sweptFractals.Any() && !_entryDoneForCurrentSweep)
            {
                DebugLog($"[DEBUG] Найдены свипнутые фракталы ({sweptFractals.Count}). Проверка слома структуры на 3м...");
                var breakResult = Is3mStructureBreak();
                DebugLog($"[DEBUG] Результат проверки слома структуры: {breakResult.IsBreak}");
                if (breakResult.IsBreak)
                {
                    DebugLog($"[DEBUG] !!! ОБНАРУЖЕН СЛОМ СТРУКТУРЫ !!! Попытка входа...");
                    if (trendContext == TrendContext.Bullish)
                    {
                        DebugLog($"[DEBUG] !!! ВХОД В ЛОНГ !!!");
                        EnterPosition(TradeType.Buy, breakResult.EntryPrice);
                        _entryDoneForCurrentSweep = true;
                        _lastTradeDate = Server.Time;
                    }
                    else if (trendContext == TrendContext.Bearish)
                    {
                        DebugLog($"[DEBUG] !!! ВХОД В ШОРТ !!!");
                        EnterPosition(TradeType.Sell, breakResult.EntryPrice);
                        _entryDoneForCurrentSweep = true;
                        _lastTradeDate = Server.Time;
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
            else if (!_entryDoneForCurrentSweep)
            {
                DebugLog($"[DEBUG] Нет свипнутых фракталов, ожидание свипа");
            }
            else
            {
                DebugLog($"[DEBUG] Вход уже был совершен по текущему свипу, ожидание новой Азии");
            }
            DebugLog($"[DEBUG] ======= КОНЕЦ ТИКА ======= {Server.Time} =======");
        }

        protected override void OnStop()
        {
            // Handle cBot stop here
        }

        private void FindAsianSessionFractals()
        {
            _asianFractals.Clear();
            _entryDoneForCurrentSweep = false;
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            var today = Server.Time.Date;
            var trendContext = DetermineTrendContext();
            int found = 0;
            DebugLog($"[DEBUG] Поиск фракталов в азиатскую сессию. Текущий тренд: {trendContext}");
            // Ищем фракталы только в сегодняшнюю Азию (0:00-9:00)
            for (int i = 2; i < h1Bars.Count - 2; i++)
            {
                var barTime = h1Bars.OpenTimes[i];
                if (!(barTime.Date == today && barTime.Hour >= AsiaStartHour && barTime.Hour < AsiaEndHour))
                    continue;
                // Для бычьего тренда ищем нижние фракталы для лонгов
                if (trendContext == TrendContext.Bullish)
                {
                    if (!double.IsNaN(hourlyFractals.DownFractal[i]) && hourlyFractals.DownFractal[i] < Bars.ClosePrices.Last(0))
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
                }
                // Для медвежьего тренда ищем верхние фракталы для шортов
                else if (trendContext == TrendContext.Bearish)
                {
                    if (!double.IsNaN(hourlyFractals.UpFractal[i]) && hourlyFractals.UpFractal[i] > Bars.ClosePrices.Last(0))
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
            }
            // Если не нашли фракталы строго в азиатскую сессию — день пропускаем
            if (found == 0)
            {
                DebugLog($"[DEBUG] Не найдено ни одного фрактала в сегодняшнюю Азию — день пропускается");
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
            
            var currentPrice = Bars.ClosePrices.Last(0);
            var context = SimpleTrendContext();
            
            DebugLog($"[DEBUG] Проверка свипа фракталов. Найдено фракталов: {_asianFractals.Count}");
            
            // Проверяем свип для каждого фрактала, который еще не свипнут
            foreach (var fractal in _asianFractals.Where(f => !f.IsSwept))
            {
                if (context == TrendContext.Bullish)
                {
                    double fractalLevel = fractal.Level;
                    DebugLog($"[DEBUG] Проверка свипа нижнего фрактала {fractalLevel:F5} в бычьем тренде");
                    if (currentPrice > fractalLevel)
                    {
                        fractal.IsSwept = true;
                        fractal.SweepLevel = currentPrice;
                        fractal.SweepExtreme = Bars.LowPrices.Last(0);
                        DebugLog($"[DEBUG] Фрактал {fractalLevel:F5} свипнут текущей ценой {currentPrice:F5}, экстремум {fractal.SweepExtreme:F5}");
                        continue;
                    }
                    for (int i = 0; i < Math.Min(1000, _m3Bars.Count); i++)
                    {
                        var barTime = _m3Bars.OpenTimes.Last(i);
                        if ((IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)) && (barTime.Date == now.Date || barTime.Date == now.Date.AddDays(-1)))
                        {
                            if (_m3Bars.HighPrices.Last(i) > fractalLevel)
                            {
                                fractal.IsSwept = true;
                                fractal.SweepLevel = _m3Bars.HighPrices.Last(i);
                                fractal.SweepExtreme = _m3Bars.LowPrices.Last(i);
                                DebugLog($"[DEBUG] Найден исторический свип фрактала {fractalLevel:F5} на баре {i}, время {barTime}, свип-уровень: {fractal.SweepLevel:F5}, экстремум: {fractal.SweepExtreme:F5}");
                                break;
                            }
                        }
                    }
                }
                else if (context == TrendContext.Bearish)
                {
                    double fractalLevel = fractal.Level;
                    DebugLog($"[DEBUG] Проверка свипа верхнего фрактала {fractalLevel:F5} в медвежьем тренде");
                    if (currentPrice < fractalLevel)
                    {
                        fractal.IsSwept = true;
                        fractal.SweepLevel = currentPrice;
                        fractal.SweepExtreme = Bars.HighPrices.Last(0);
                        DebugLog($"[DEBUG] Фрактал {fractalLevel:F5} свипнут текущей ценой {currentPrice:F5}, экстремум {fractal.SweepExtreme:F5}");
                        continue;
                    }
                    for (int i = 0; i < Math.Min(1000, _m3Bars.Count); i++)
                    {
                        var barTime = _m3Bars.OpenTimes.Last(i);
                        if ((IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)) && (barTime.Date == now.Date || barTime.Date == now.Date.AddDays(-1)))
                        {
                            if (_m3Bars.LowPrices.Last(i) < fractalLevel)
                            {
                                fractal.IsSwept = true;
                                fractal.SweepLevel = _m3Bars.LowPrices.Last(i);
                                fractal.SweepExtreme = _m3Bars.HighPrices.Last(i);
                                DebugLog($"[DEBUG] Найден исторический свип фрактала {fractalLevel:F5} на баре {i}, время {barTime}, свип-уровень: {fractal.SweepLevel:F5}, экстремум: {fractal.SweepExtreme:F5}");
                                break;
                            }
                        }
                    }
                }
            }
            
            // Выводим статус всех фракталов после проверки свипа
            if (_asianFractals.Any())
            {
                DebugLog($"[DEBUG] Статус фракталов после проверки свипа:");
                foreach (var fractal in _asianFractals)
                {
                    DebugLog($"[DEBUG] Фрактал: {fractal.Level:F5}, время: {fractal.Time}, свипнут: {fractal.IsSwept}, свип-уровень: {fractal.SweepLevel?.ToString("F5") ?? "N/A"}, экстремум: {fractal.SweepExtreme?.ToString("F5") ?? "N/A"}");
                }
            }
        }

        private class StructureBreakResult
        {
            public bool IsBreak { get; set; }
            public double EntryPrice { get; set; }
        }

        private StructureBreakResult Is3mStructureBreak()
        {
            var sweptFractals = _asianFractals.Where(f => f.IsSwept && f.SweepLevel.HasValue).ToList();
            if (!sweptFractals.Any()) { DebugLog("[DEBUG] Нет свипнутых фракталов для проверки слома"); return new StructureBreakResult { IsBreak = false }; }
            
            var m3 = _m3Bars;
            if (m3.Count < 10) return new StructureBreakResult { IsBreak = false };
            
            var context = SimpleTrendContext();
            if (context == TrendContext.Neutral) { DebugLog("[DEBUG] Тренд нейтральный, слом не проверяем"); return new StructureBreakResult { IsBreak = false }; }
            
            DebugLog($"[DEBUG] Анализ слома структуры для {sweptFractals.Count} свипнутых фракталов");
            
            // Находим все бары в сессию Франкфурта/Лондона
            var sessionBars = new List<(int index, DateTime time, double open, double close, double high, double low)>();
            
            // Собираем все бары за нужные сессии и сортируем их по времени (от самых ранних)
            for (int i = Math.Min(800, m3.Count-1); i >= 0; i--)
            {
                var barTime = m3.OpenTimes[i];
                
                // Проверяем только бары во время Франкфурта и Лондона
                if (IsInFrankfurtSession(barTime) || IsInLondonSession(barTime))
                {
                    sessionBars.Add((
                        i, 
                        barTime,
                        m3.OpenPrices[i],
                        m3.ClosePrices[i],
                        m3.HighPrices[i],
                        m3.LowPrices[i]
                    ));
                }
            }
            
            // Сортируем бары по времени (сначала самые ранние)
            sessionBars = sessionBars.OrderBy(b => b.time).ToList();
            
            // Проверяем слом структуры на каждом фрактале
            foreach (var fractal in sweptFractals)
            {
                if (!fractal.SweepLevel.HasValue || !fractal.SweepExtreme.HasValue) continue;
                
                double sweepLevel = fractal.SweepLevel.Value;
                double sweepExtreme = fractal.SweepExtreme.Value;
                DebugLog($"[DEBUG] Проверка слома для фрактала {fractal.Level:F5}, уровень свипа: {sweepLevel:F5}, экстремум: {sweepExtreme:F5}");
                
                // Проверяем каждый бар на слом структуры относительно уровня свипа
                foreach (var bar in sessionBars)
                {
                    double bodySize = Math.Abs(bar.close - bar.open);
                    bool isSignificantBar = bodySize > Symbol.PipSize * 2;
                    
                    // Логируем каждый 10-й бар или первые/последние 20 баров
                    if (sessionBars.IndexOf(bar) % 10 == 0 || 
                        sessionBars.IndexOf(bar) < 20 || 
                        sessionBars.Count - sessionBars.IndexOf(bar) <= 20)
                    {
                        DebugLog($"[DEBUG] Анализ бара {bar.index}, время {bar.time}, O={bar.open:F5}, C={bar.close:F5}, тело={bodySize/Symbol.PipSize:F1} пипсов");
                    }
                    
                    if (context == TrendContext.Bullish)
                    {
                        // Закрепление телом над sweepExtreme
                        if (bar.open <= sweepExtreme && bar.close > sweepExtreme && isSignificantBar)
                        {
                            DebugLog($"[DEBUG] НАЙДЕН СЛОМ СТРУКТУРЫ для ЛОНГА над экстремумом {sweepExtreme:F5} на баре {bar.index}: время={bar.time}");
                            return new StructureBreakResult { IsBreak = true, EntryPrice = bar.close };
                        }
                    }
                    else if (context == TrendContext.Bearish)
                    {
                        // Закрепление телом под sweepExtreme
                        if (bar.open >= sweepExtreme && bar.close < sweepExtreme && isSignificantBar)
                        {
                            DebugLog($"[DEBUG] НАЙДЕН СЛОМ СТРУКТУРЫ для ШОРТА под экстремумом {sweepExtreme:F5} на баре {bar.index}: время={bar.time}");
                            return new StructureBreakResult { IsBreak = true, EntryPrice = bar.close };
                        }
                    }
                }
            }
            
            // Если не найден слом в исторических барах, проверяем текущий бар
            int lastIndex = m3.Count - 1;
            var lastBar = new {
                Time = m3.OpenTimes[lastIndex],
                Open = m3.OpenPrices[lastIndex],
                Close = m3.ClosePrices[lastIndex],
                High = m3.HighPrices[lastIndex],
                Low = m3.LowPrices[lastIndex]
            };
            
            if (IsInFrankfurtSession(lastBar.Time) || IsInLondonSession(lastBar.Time))
            {
                double lastBodySize = Math.Abs(lastBar.Close - lastBar.Open);
                bool isSignificantBar = lastBodySize > Symbol.PipSize * 2;
                
                DebugLog($"[DEBUG] Проверка текущего бара: время={lastBar.Time}, O={lastBar.Open:F5}, C={lastBar.Close:F5}, тело={lastBodySize/Symbol.PipSize:F1} пипсов");
                
                foreach (var fractal in sweptFractals)
                {
                    if (!fractal.SweepLevel.HasValue || !fractal.SweepExtreme.HasValue) continue;
                    
                    double sweepLevel = fractal.SweepLevel.Value;
                    double sweepExtreme = fractal.SweepExtreme.Value;
                    
                    if (context == TrendContext.Bullish)
                    {
                        if (lastBar.Open <= sweepExtreme && lastBar.Close > sweepExtreme && isSignificantBar)
                        {
                            DebugLog($"[DEBUG] НАЙДЕН СЛОМ СТРУКТУРЫ для ЛОНГА над экстремумом {sweepExtreme:F5} на текущем баре: время={lastBar.Time}");
                            return new StructureBreakResult { IsBreak = true, EntryPrice = lastBar.Close };
                        }
                    }
                    else if (context == TrendContext.Bearish)
                    {
                        if (lastBar.Open >= sweepExtreme && lastBar.Close < sweepExtreme && isSignificantBar)
                        {
                            DebugLog($"[DEBUG] НАЙДЕН СЛОМ СТРУКТУРЫ для ШОРТА под экстремумом {sweepExtreme:F5} на текущем баре: время={lastBar.Time}");
                            return new StructureBreakResult { IsBreak = true, EntryPrice = lastBar.Close };
                        }
                    }
                }
            }
            
            DebugLog("[DEBUG] Слом структуры не обнаружен");
            return new StructureBreakResult { IsBreak = false };
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

        // Добавим новый метод для поиска ключевых уровней
        private double? FindKeyLevelForTP(TradeType tradeType, double entryPrice)
        {
            var h1Bars = _h1Bars;
            if (h1Bars.Count < 10) return null;
            
            // Находим максимумы и минимумы за последние 24 часа
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
                // Для лонга ищем уровень выше текущей цены
                if (highest > entryPrice)
                {
                    DebugLog($"[DEBUG] Найден ключевой уровень для лонга (максимум 24ч): {highest:F5}");
                    return highest;
                }
                
                // Если максимум не подходит, используем уровень на основе текущей цены
                double targetLevel = entryPrice + (entryPrice - lowest) * 0.618; // 61.8% фибо
                DebugLog($"[DEBUG] Расчетный ключевой уровень для лонга: {targetLevel:F5}");
                return targetLevel;
            }
            else
            {
                // Для шорта ищем уровень ниже текущей цены
                if (lowest < entryPrice)
                {
                    DebugLog($"[DEBUG] Найден ключевой уровень для шорта (минимум 24ч): {lowest:F5}");
                    return lowest;
                }
                
                // Если минимум не подходит, используем уровень на основе текущей цены
                double targetLevel = entryPrice - (highest - entryPrice) * 0.618; // 61.8% фибо
                DebugLog($"[DEBUG] Расчетный ключевой уровень для шорта: {targetLevel:F5}");
                return targetLevel;
            }
        }

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
} 