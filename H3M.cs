using System;
using cAlgo.API;
using cAlgo.API.Collections;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.None, AddIndicators = true)]
    public class H3M : Robot
    {
        [Parameter("EMA Period", DefaultValue = 200)]
        public int EmaPeriod { get; set; }

        [Parameter("ADX Period", DefaultValue = 14)]
        public int AdxPeriod { get; set; }

        [Parameter("ADX Threshold", DefaultValue = 25)]
        public double AdxThreshold { get; set; }

        [Parameter("Fractal Period", DefaultValue = 3)]
        public int FractalPeriod { get; set; }

        [Parameter("ATR MA Type", DefaultValue = MovingAverageType.Exponential)]
        public MovingAverageType AtrMaType { get; set; }

        [Parameter("Risk Percent", DefaultValue = 1.0)]
        public double RiskPercent { get; set; }

        [Parameter("Stop Loss Buffer Pips", DefaultValue = 5)]
        public double StopLossBufferPips { get; set; }

        [Parameter("Min RR", DefaultValue = 1.3)]
        public double MinRR { get; set; }

        [Parameter("Max RR", DefaultValue = 3.0)]
        public double MaxRR { get; set; }

        private ExponentialMovingAverage _ema;
        private AverageDirectionalMovementIndexRating _adxr;
        private Fractals _fractals;
        private DateTime _lastAsianSessionCheck = DateTime.MinValue;
        private bool _isAsianSession = false;
        private double? _currentFractalLevel = null;
        private Bars _m3Bars;
        private bool _isWaitingForStructureBreak = false;
        private double _lastFractalSweepLevel;
       
        private Bars _h1Bars;
        private AverageTrueRange _atr;

        // --- Asia/Frankfurt session time helpers ---
        private static readonly int AsiaStartHour = 0; // 00:00 UTC+3
        private static readonly int AsiaEndHour = 9;   // 09:00 UTC+3
        private static readonly int FrankfurtStartHour = 9; // 09:00 UTC+3
        private static readonly int FrankfurtEndHour = 10;  // 10:00 UTC+3
        private static readonly int LondonStartHour = 10; // 10:00 UTC+3
        private static readonly int LondonEndHour = 15;   // 15:00 UTC+3

        private double? _asianFractalLevel = null;
        private DateTime? _asianFractalTime = null;
        private bool _fractalSwept = false;
        private bool _entryDone = false;

        protected override void OnStart()
        {
            _ema = Indicators.ExponentialMovingAverage(Bars.ClosePrices, EmaPeriod);
            _adxr = Indicators.AverageDirectionalMovementIndexRating(AdxPeriod);
            _fractals = Indicators.Fractals(FractalPeriod);
            _m3Bars = MarketData.GetBars(TimeFrame.Minute3);
            _h1Bars = MarketData.GetBars(TimeFrame.Hour);
            _atr = Indicators.AverageTrueRange(14, AtrMaType);
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
            
            return Math.Floor(riskAmount / stopLossAmount);
        }

        private (double? takeProfitPrice, double rr) CalculateTakeProfit(TradeType tradeType, double entryPrice, double stopLossPrice)
        {
            var currentPrice = tradeType == TradeType.Buy ? Symbol.Ask : Symbol.Bid;
            var stopLossDistance = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;

            // Find nearest fractal level
            var nearestFractal = FindNearestFractalLevel(tradeType, currentPrice);
            if (nearestFractal == null) return (null, 0);

            var takeProfitDistance = Math.Abs(nearestFractal.Value - entryPrice) / Symbol.PipSize;
            var rr = takeProfitDistance / stopLossDistance;

            // If RR is less than minimum, try to find next fractal
            if (rr < MinRR)
            {
                var nextFractal = FindNextFractalLevel(tradeType, currentPrice, nearestFractal.Value);
                if (nextFractal != null)
                {
                    takeProfitDistance = Math.Abs(nextFractal.Value - entryPrice) / Symbol.PipSize;
                    rr = takeProfitDistance / stopLossDistance;

                    // If RR is still less than minimum or more than maximum, don't enter
                    if (rr < MinRR || rr > MaxRR)
                        return (null, rr);

                    return (nextFractal.Value, rr);
                }
            }

            // If RR is more than maximum, don't enter
            if (rr > MaxRR)
                return (null, rr);

            return (nearestFractal.Value, rr);
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
            if (Positions.Find("H3M") != null) return;

            var entryPrice = tradeType == TradeType.Buy ? Symbol.Ask : Symbol.Bid;
            var stopLossPrice = CalculateStopLoss(tradeType, _lastFractalSweepLevel);
            var atrValue = _atr.Result.Last(0);
            var stopLossPips = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;

            var (takeProfitPrice, rr) = CalculateTakeProfit(tradeType, entryPrice, stopLossPrice);
            
            if (takeProfitPrice == null)
            {
                Print($"Skipping entry - RR {rr:F2} is outside allowed range ({MinRR}-{MaxRR})");
                return;
            }

            var positionSize = CalculatePositionSize(stopLossPips);
            if (positionSize == 0) return;

            var result = ExecuteMarketOrder(tradeType, SymbolName, positionSize, "H3M", stopLossPrice, takeProfitPrice);
            
            if (result.IsSuccessful)
            {
                Print($"Entered {tradeType} position at {result.Position.EntryPrice}");
                Print($"Stop Loss: {stopLossPrice} (behind swept fractal at {_lastFractalSweepLevel})");
                Print($"Take Profit: {takeProfitPrice}");
                Print($"Risk/Reward: {rr:F2}");
            }
            else
            {
                Print($"Failed to enter position: {result.Error}");
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
                _isWaitingForStructureBreak = false;
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

        private bool IsPriceNearFractal()
        {
            if (_currentFractalLevel == null) return false;

            var currentPrice = Bars.ClosePrices.Last(0);
            var distance = Math.Abs(currentPrice - _currentFractalLevel.Value);
            var atr = _atr.Result.Last(0);
            
            return distance <= atr * 0.5;
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

        private bool IsStructureBreak()
        {
            if (!_isWaitingForStructureBreak) return false;

            var trendContext = DetermineTrendContext();
            var lastCandle = _m3Bars.Last(1);
            var currentCandle = _m3Bars.Last(0);

            if (trendContext == TrendContext.Bearish)
            {
                // For bearish trend, check if current candle closed below the fractal sweep level
                return currentCandle.Close < _lastFractalSweepLevel && 
                       currentCandle.Close < currentCandle.Open; // Confirmed by body
            }
            else if (trendContext == TrendContext.Bullish)
            {
                // For bullish trend, check if current candle closed above the fractal sweep level
                return currentCandle.Close > _lastFractalSweepLevel && 
                       currentCandle.Close > currentCandle.Open; // Confirmed by body
            }

            return false;
        }

        private TrendContext DetermineTrendContext()
        {
            var currentPrice = Bars.ClosePrices.Last(0);
            var currentEma = _ema.Result.Last(0);
            var currentAdxr = _adxr.ADXR.Last(0);

            if (currentAdxr < AdxThreshold)
                return TrendContext.Neutral;

            return currentPrice > currentEma ? TrendContext.Bullish : TrendContext.Bearish;
        }

        protected override void OnTick()
        {
            // 1. Найти ближайший фрактал в азиатскую сессию (раз в день)
            if ((_asianFractalLevel == null || Server.Time.Date != _asianFractalTime?.Date) && !_fractalSwept)
            {
                FindAsianSessionFractal();
                _fractalSwept = false;
                _entryDone = false;
            }
            // 2. Проверить свип фрактала во Франкфурт/Лондон
            CheckFractalSweep();
            // 3. После свипа ждать слома на 3м
            if (Is3mStructureBreak() && !_entryDone)
            {
                Print("Слом структуры на 3м после свипа! Вход в позицию.");
                var trendContext = DetermineTrendContext();
                if (trendContext == TrendContext.Bullish)
                    EnterPosition(TradeType.Buy);
                else if (trendContext == TrendContext.Bearish)
                    EnterPosition(TradeType.Sell);
                _entryDone = true;
                // После попытки входа не реагировать на этот фрактал до следующего дня
                _asianFractalLevel = null;
                _asianFractalTime = null;
            }
        }

        protected override void OnStop()
        {
            // Handle cBot stop here
        }

        // --- Find the nearest fractal formed in Asia session ---
        private void FindAsianSessionFractal()
        {
            _asianFractalLevel = null;
            _asianFractalTime = null;
            double? bestLevel = null;
            DateTime? bestTime = null;
            double minDistance = double.MaxValue;
            var h1Bars = _h1Bars;
            var currentPrice = Bars.ClosePrices.Last(0);
            var today = Server.Time.Date;
            for (int i = 2; i < h1Bars.Count - 2; i++)
            {
                var barTime = h1Bars.OpenTimes[i];
                if (barTime.Date != today)
                    continue;
                if (!IsInAsiaSession(barTime))
                    continue;
                if (DetermineTrendContext() == TrendContext.Bullish && !double.IsNaN(_fractals.DownFractal[i]))
                {
                    var level = _fractals.DownFractal[i];
                    var distance = Math.Abs(currentPrice - level);
                    if (level < currentPrice && distance < minDistance)
                    {
                        minDistance = distance;
                        bestLevel = level;
                        bestTime = barTime;
                    }
                }
                if (DetermineTrendContext() == TrendContext.Bearish && !double.IsNaN(_fractals.UpFractal[i]))
                {
                    var level = _fractals.UpFractal[i];
                    var distance = Math.Abs(currentPrice - level);
                    if (level > currentPrice && distance < minDistance)
                    {
                        minDistance = distance;
                        bestLevel = level;
                        bestTime = barTime;
                    }
                }
            }
            _asianFractalLevel = bestLevel;
            _asianFractalTime = bestTime;
            if (_asianFractalLevel != null)
                Print($"Выбран ближайший азиатский фрактал: {_asianFractalLevel} (время: {_asianFractalTime})");
        }

        // --- Detect sweep of Asian fractal in Frankfurt/London session ---
        private void CheckFractalSweep()
        {
            if (_asianFractalLevel == null || _fractalSwept) return;
            var now = Server.Time;
            if (!IsInFrankfurtSession(now) && !IsInLondonSession(now)) return;
            var price = Bars.ClosePrices.Last(0);
            if (DetermineTrendContext() == TrendContext.Bullish && price < _asianFractalLevel)
            {
                _fractalSwept = true;
                _lastFractalSweepLevel = _asianFractalLevel.Value;
                Print($"Свип часовой азиатской ликвидности: {_asianFractalLevel} (время: {_asianFractalTime}) во Франкфурт/Лондон");
            }
            if (DetermineTrendContext() == TrendContext.Bearish && price > _asianFractalLevel)
            {
                _fractalSwept = true;
                _lastFractalSweepLevel = _asianFractalLevel.Value;
                Print($"Свип часовой азиатской ликвидности: {_asianFractalLevel} (время: {_asianFractalTime}) во Франкфурт/Лондон");
            }
        }

        // --- Detect 3m structure break after sweep ---
        private bool Is3mStructureBreak()
        {
            if (!_fractalSwept || _entryDone) return false;
            var m3 = _m3Bars;
            var last = m3.Last(0);
            var prev = m3.Last(1);
            if (DetermineTrendContext() == TrendContext.Bullish)
            {
                // Слом: закрытие выше хая, который снял ликвидность
                return last.Close > prev.High && last.Close > last.Open;
            }
            if (DetermineTrendContext() == TrendContext.Bearish)
            {
                // Слом: закрытие ниже лоя, который снял ликвидность
                return last.Close < prev.Low && last.Close < last.Open;
            }
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