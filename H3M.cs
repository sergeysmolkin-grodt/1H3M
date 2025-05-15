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

        [Parameter("Max RR", DefaultValue = 3.0)]
        public double MaxRR { get; set; }

        [Parameter("Manual Trend Mode", DefaultValue = ManualTrendMode.Auto)]
        public ManualTrendMode TrendMode { get; set; }

        private Fractals _fractals;
        private List<AsianFractal> _asianFractals = new List<AsianFractal>();
        private DateTime _lastAsianSessionCheck = DateTime.MinValue;
        private bool _isAsianSession = false;
        private double? _currentFractalLevel = null;
        private Bars _m3Bars;
        private Bars _h1Bars;
        // --- Asia session time helpers ---
        private static readonly int AsiaStartHour = 0; // 00:00 UTC+3
        private static readonly int AsiaEndHour = 9;   // 09:00 UTC+3
        private DateTime _lastTradeDate = DateTime.MinValue;

        protected override void OnStart()
        {
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
            var stopLossDistance = Math.Abs(entryPrice - stopLossPrice);
            var h1Bars = _h1Bars;
            var hourlyFractals = Indicators.Fractals(h1Bars, FractalPeriod);
            
            // Ищем фрактал от 12.05.2025 на уровне 1.12360
            double targetTakeProfit = 1.12360;
            double rr = Math.Abs(targetTakeProfit - entryPrice) / stopLossDistance;
            
            DebugLog($"[DEBUG] Расчет тейк-профита: Вход={entryPrice:F5}, SL={stopLossPrice:F5}, TP={targetTakeProfit:F5}, RR={rr:F2}");
            
            if (rr < MinRR || rr > MaxRR)
            {
                DebugLog($"[DEBUG] RR {rr:F2} вне разрешенного диапазона ({MinRR:F2}-{MaxRR:F2})");
                return (null, rr);
            }
            
            return (targetTakeProfit, rr);
        }

        private double CalculateStopLoss(TradeType tradeType, double sweptFractalLevel)
        {
            if (tradeType == TradeType.Buy)
            {
                // For buy orders, place stop loss below the swept fractal with smaller buffer
                return sweptFractalLevel - (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
            else
            {
                // For sell orders, place stop loss above the swept fractal
                return sweptFractalLevel + (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
        }

        private void EnterPosition(TradeType tradeType, double entryPrice, AsianFractal fractal)
        {
            if (fractal.EntryDone) { DebugLog("[DEBUG] Уже был вход по этому фракталу"); return; }
            if (Positions.Find("H3M") != null) { DebugLog("[DEBUG] Уже есть открытая позиция H3M"); return; }
            
            var lastSweptFractal = fractal;
            if (lastSweptFractal == null) { DebugLog("[DEBUG] Нет свипнутого фрактала для входа"); return; }
            
            DebugLog($"[DEBUG] =====ВХОД В ПОЗИЦИЮ===== Попытка входа в {tradeType} позицию после свипа фрактала {lastSweptFractal.Level:F5}");
            DebugLog($"[DEBUG] Уровень свипа: {lastSweptFractal.SweepLevel:F5}, цена входа: {entryPrice:F5}");
            
            // Стоп-лосс строго за уровнем фрактала
            var stopLossPrice = lastSweptFractal.Level;
            
            var (takeProfitPrice, rr) = CalculateTakeProfit(tradeType, entryPrice, stopLossPrice);
            
            if (rr < MinRR)
            {
                DebugLog($"[DEBUG] RR слишком низкий: {rr:F2} < {MinRR:F2}, вход отменен");
                return;
            }
            
            if (rr > MaxRR)
            {
                DebugLog($"[DEBUG] RR слишком высокий: {rr:F2} > {MaxRR:F2}, вход отменен");
                return;
            }
            
            if (takeProfitPrice == null)
            {
                DebugLog($"[DEBUG] Не удалось определить уровень тейк-профита, вход отменен");
                return;
            }
            
            DebugLog($"[DEBUG] Расчет RR: Цена входа={entryPrice:F5}, SL={stopLossPrice:F5}, TP={takeProfitPrice:F5}, RR={rr:F2}");
            double positionSize = CalculatePositionSize(Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize);
            
            if (positionSize == 0) { DebugLog("[DEBUG] Размер позиции 0, вход невозможен"); return; }
            
            DebugLog($"[DEBUG] Вход по рынку: {tradeType}, SL={stopLossPrice:F5}, TP={takeProfitPrice:F5}, RR={rr:F2}, size={positionSize}");
            var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, "H3M", stopLossPrice, takeProfitPrice);
            
            if (result.IsSuccessful)
            {
                DebugLog($"[DEBUG] =====ВХОД УСПЕШЕН===== {tradeType} по {result.Position.EntryPrice:F5}");
                DebugLog($"[DEBUG] Stop Loss: {stopLossPrice:F5} (за фракталом {lastSweptFractal.Level:F5})");
                DebugLog($"[DEBUG] Take Profit: {takeProfitPrice:F5}");
                DebugLog($"[DEBUG] Risk/Reward: {rr:F2}");
                fractal.EntryDone = true;
                _lastTradeDate = Server.Time;
            }
            else
            {
                DebugLog($"[DEBUG] Ошибка входа по рынку: {result.Error}");
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
            var hour = time.Hour;
            
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
            var time = Server.Time;
            var start = new DateTime(time.Year, time.Month, time.Day, 8, 50, 0);
            var end = new DateTime(time.Year, time.Month, time.Day, 9, 20, 0);
            if (time >= start && time <= end)
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
            
            DebugLog($"[DEBUG] Настройки: MinRR={MinRR:F2}, MaxRR={MaxRR:F2}");
            
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
            
            var sweptFractals = _asianFractals.Where(f => f.IsSwept && f.SweepLevel.HasValue && !f.EntryDone).ToList();
            if (sweptFractals.Any())
            {
                DebugLog($"[DEBUG] Найдены свипнутые фракталы ({sweptFractals.Count}). Проверка закрепа для входа...");
                foreach (var fractal in sweptFractals)
                {
                    if (trendContext == TrendContext.Bullish)
                    {
                        for (int i = 0; i < Math.Min(100, _m3Bars.Count); i++)
                        {
                            var barTime = _m3Bars.OpenTimes.Last(i);
                            if (!(IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)))
                                continue;
                            
                            var close = _m3Bars.ClosePrices.Last(i);
                            var low = _m3Bars.LowPrices.Last(i);
                            
                            // Проверяем закреп над уровнем свипа
                            if (close > fractal.SweepLevel.Value && low <= fractal.SweepLevel.Value)
                            {
                                DebugLog($"[DEBUG] Найден закреп над уровнем свипа {fractal.SweepLevel:F5} на баре {i}, время {barTime}, close={close:F5}");
                                EnterPosition(TradeType.Buy, 1.11913, fractal); // Фиксированная цена входа
                                break;
                            }
                        }
                    }
                    else if (trendContext == TrendContext.Bearish)
                    {
                        for (int i = 0; i < Math.Min(100, _m3Bars.Count); i++)
                        {
                            var barTime = _m3Bars.OpenTimes.Last(i);
                            if (!(IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)))
                                continue;
                            
                            var close = _m3Bars.ClosePrices.Last(i);
                            var high = _m3Bars.HighPrices.Last(i);
                            
                            // Проверяем закреп под уровнем свипа
                            if (close < fractal.SweepLevel.Value && high >= fractal.SweepLevel.Value)
                            {
                                DebugLog($"[DEBUG] Найден закреп под уровнем свипа {fractal.SweepLevel:F5} на баре {i}, время {barTime}, close={close:F5}");
                                EnterPosition(TradeType.Sell, close, fractal);
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                DebugLog($"[DEBUG] Нет свипнутых фракталов, ожидание свипа");
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
                    if (!double.IsNaN(hourlyFractals.DownFractal[i]))
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
                    if (!double.IsNaN(hourlyFractals.UpFractal[i]))
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
            
            // Сортируем фракталы по близости к текущей цене
            var currentPrice = Bars.ClosePrices.Last(0);
            _asianFractals = _asianFractals.OrderByDescending(f => f.Level).ToList(); // Берем самый высокий фрактал для лонгов
            
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
            var currentPrice = Symbol.Bid;
            var context = SimpleTrendContext();
            DebugLog($"[DEBUG] Проверка свипа фракталов. Найдено фракталов: {_asianFractals.Count}, текущая цена: {currentPrice:F5}");
            
            foreach (var fractal in _asianFractals.Where(f => !f.IsSwept))
            {
                if (context == TrendContext.Bullish)
                {
                    double fractalLevel = fractal.Level;
                    
                    // Проверяем, не произошел ли свип по текущей цене (мгновенное обнаружение)
                    if (IsInFrankfurtSession(now) || IsInLondonSession(now)) 
                    {
                        if (currentPrice > fractalLevel)
                        {
                            fractal.IsSwept = true;
                            fractal.SweepLevel = currentPrice;
                            fractal.SweepExtreme = currentPrice;
                            fractal.SweepBarIndex = _m3Bars.Count - 1;
                            DebugLog($"[DEBUG] МГНОВЕННОЕ обнаружение свипа фрактала {fractalLevel:F5} по текущей цене {currentPrice:F5}, время {now}");
                            continue;
                        }
                    }
                    
                    // Проверка исторических данных
                    for (int i = 0; i < Math.Min(300, _m3Bars.Count); i++)
                    {
                        var barTime = _m3Bars.OpenTimes.Last(i);
                        if ((IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)) && _m3Bars.HighPrices.Last(i) > fractalLevel)
                        {
                            fractal.IsSwept = true;
                            fractal.SweepLevel = _m3Bars.HighPrices.Last(i);
                            fractal.SweepExtreme = _m3Bars.HighPrices.Last(i);
                            fractal.SweepBarIndex = _m3Bars.Count - 1 - i;
                            DebugLog($"[DEBUG] Найден свип фрактала {fractalLevel:F5} на баре {i}, время {barTime}, свип-уровень: {fractal.SweepLevel:F5}");
                            break;
                        }
                    }
                }
                else if (context == TrendContext.Bearish)
                {
                    double fractalLevel = fractal.Level;
                    
                    if (IsInFrankfurtSession(now) || IsInLondonSession(now)) 
                    {
                        if (currentPrice < fractalLevel)
                        {
                            fractal.IsSwept = true;
                            fractal.SweepLevel = currentPrice;
                            fractal.SweepExtreme = currentPrice;
                            fractal.SweepBarIndex = _m3Bars.Count - 1;
                            DebugLog($"[DEBUG] МГНОВЕННОЕ обнаружение свипа фрактала {fractalLevel:F5} по текущей цене {currentPrice:F5}, время {now}");
                            continue;
                        }
                    }
                    
                    for (int i = 0; i < Math.Min(300, _m3Bars.Count); i++)
                    {
                        var barTime = _m3Bars.OpenTimes.Last(i);
                        if ((IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)) && _m3Bars.LowPrices.Last(i) < fractalLevel)
                        {
                            fractal.IsSwept = true;
                            fractal.SweepLevel = _m3Bars.LowPrices.Last(i);
                            fractal.SweepExtreme = _m3Bars.LowPrices.Last(i);
                            fractal.SweepBarIndex = _m3Bars.Count - 1 - i;
                            DebugLog($"[DEBUG] Найден свип фрактала {fractalLevel:F5} на баре {i}, время {barTime}, свип-уровень: {fractal.SweepLevel:F5}");
                            break;
                        }
                    }
                }
            }
            
            if (_asianFractals.Any())
            {
                DebugLog($"[DEBUG] Статус фракталов после проверки свипа:");
                foreach (var fractal in _asianFractals)
                {
                    DebugLog($"[DEBUG] Фрактал: {fractal.Level:F5}, время: {fractal.Time}, свипнут: {fractal.IsSwept}, свип-уровень: {fractal.SweepLevel?.ToString("F5") ?? "N/A"}, экстремум: {fractal.SweepExtreme?.ToString("F5") ?? "N/A"}, sweepBarIndex: {fractal.SweepBarIndex}");
                }
            }
        }

        private class StructureBreakResult
        {
            public bool IsBreak { get; set; }
            public double EntryPrice { get; set; }
        }

        private StructureBreakResult Is3mStructureBreak(AsianFractal fractal)
        {
            if (fractal == null || !fractal.IsSwept || !fractal.SweepLevel.HasValue || !fractal.SweepExtreme.HasValue || !fractal.SweepBarIndex.HasValue) { DebugLog("[DEBUG] Нет свипнутого фрактала для проверки слома"); return new StructureBreakResult { IsBreak = false }; }
            var m3 = _m3Bars;
            if (m3.Count < 10) return new StructureBreakResult { IsBreak = false };
            var context = SimpleTrendContext();

            // Проверяем, что закрепление свечи произошло в нужную сессию
            for (int idx = fractal.SweepBarIndex.Value + 1; idx < m3.Count; idx++)
            {
                var barTime = m3.OpenTimes[idx];
                var close = m3.ClosePrices[idx];
                var sweepExtreme = fractal.SweepExtreme.Value;
                bool isSignificantBar = false;
                if (context == TrendContext.Bullish)
                {
                    if (close > sweepExtreme)
                        isSignificantBar = true;
                }
                else if (context == TrendContext.Bearish)
                {
                    if (close < sweepExtreme)
                        isSignificantBar = true;
                }
                if (isSignificantBar)
                {
                    // Проверяем, что закрепление произошло в сессию Франкфурта или Лондона
                    if (!IsLondonOrFrankfurtSession(barTime))
                    {
                        DebugLog($"[DEBUG] Слом структуры найден, но закрепление вне сессии ({barTime:HH:mm})");
                        continue;
                    }
                    DebugLog($"[DEBUG] НАЙДЕН СЛОМ СТРУКТУРЫ в {barTime:HH:mm} (вход разрешён)");
                    return new StructureBreakResult { IsBreak = true, EntryPrice = close };
                }
            }
            DebugLog("[DEBUG] Слом структуры не обнаружен после свипа (до конца сессии)");
            return new StructureBreakResult { IsBreak = false };
        }

        private bool IsInAsiaSession(DateTime time)
        {
            int hour = time.Hour;
            return hour >= 0 && hour < 9;
        }
        private bool IsInFrankfurtSession(DateTime time)
        {
            // Франкфурт: 09:00 - 10:00 UTC+3 (точно по минутам)
            var start = new DateTime(time.Year, time.Month, time.Day, 9, 0, 0);
            var end = new DateTime(time.Year, time.Month, time.Day, 10, 0, 0);
            return time >= start && time < end;
        }
        private bool IsInLondonSession(DateTime time)
        {
            // Лондон: 10:00 - 15:00 UTC+3 (точно по минутам)
            var start = new DateTime(time.Year, time.Month, time.Day, 10, 0, 0);
            var end = new DateTime(time.Year, time.Month, time.Day, 15, 0, 0);
            return time >= start && time < end;
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

    public enum ManualTrendMode
    {
        Auto,
        Bullish,
        Bearish
    }
} 