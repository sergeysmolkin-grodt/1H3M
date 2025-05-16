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
            if (stopLossDistance == 0) 
            {
                DebugLog("[DEBUG] CalculateTakeProfit: Расстояние до стоп-лосса равно 0. Невозможно рассчитать RR.");
                return (null, 0);
            }

            // 1. Ищем ближайший H1 фрактал для TP
            double? firstTpCandidate = FindNearestH1FractalForTP(tradeType, entryPrice);
            DebugLog($"[DEBUG] CalculateTakeProfit: Вход={entryPrice:F5}, SL={stopLossPrice:F5}, SL_dist={stopLossDistance:F5}");

            if (firstTpCandidate.HasValue)
            {
                DebugLog($"[DEBUG] CalculateTakeProfit: Первый кандидат на TP (ближайший H1 фрактал): {firstTpCandidate.Value:F5}");
                double rr1 = Math.Abs(firstTpCandidate.Value - entryPrice) / stopLossDistance;
                DebugLog($"[DEBUG] CalculateTakeProfit: RR для первого TP ({firstTpCandidate.Value:F5}) = {rr1:F2}");

                if (rr1 >= MinRR && rr1 <= MaxRR)
                {
                    DebugLog($"[DEBUG] CalculateTakeProfit: Первый TP ({firstTpCandidate.Value:F5}) подходит по RR ({rr1:F2}). Используем его.");
                    return (firstTpCandidate.Value, rr1);
                }
                else if (rr1 < MinRR)
                {
                    DebugLog($"[DEBUG] CalculateTakeProfit: RR для первого TP ({firstTpCandidate.Value:F5}) слишком низкий ({rr1:F2} < {MinRR:F2}). Ищем следующий H1 фрактал.");
                    double? secondTpCandidate = TryFindNextH1Fractal(tradeType, entryPrice, firstTpCandidate.Value);
                    if (secondTpCandidate.HasValue)
                    {
                        DebugLog($"[DEBUG] CalculateTakeProfit: Второй кандидат на TP (следующий H1 фрактал): {secondTpCandidate.Value:F5}");
                        double rr2 = Math.Abs(secondTpCandidate.Value - entryPrice) / stopLossDistance;
                        DebugLog($"[DEBUG] CalculateTakeProfit: RR для второго TP ({secondTpCandidate.Value:F5}) = {rr2:F2}");

                        if (rr2 >= MinRR && rr2 <= MaxRR)
                        {
                            DebugLog($"[DEBUG] CalculateTakeProfit: Второй TP ({secondTpCandidate.Value:F5}) подходит по RR ({rr2:F2}). Используем его.");
                            return (secondTpCandidate.Value, rr2);
                        }
                        else
                        {
                            DebugLog($"[DEBUG] CalculateTakeProfit: RR для второго TP ({secondTpCandidate.Value:F5}) не подходит ({rr2:F2}). Диапазон RR: {MinRR:F2}-{MaxRR:F2}. Вход отменен.");
                            return (null, rr2); // Возвращаем RR, даже если TP не подходит, для логов
                        }
                    }
                    else
                    {
                        DebugLog($"[DEBUG] Следующий H1 фрактал не найден. Первый TP ({firstTpCandidate.Value:F5}) имел RR={rr1:F2}. Вход отменен.");
                        return (null, rr1); // Возвращаем RR от первого TP
                    }
                }
                else // rr1 > MaxRR
                {
                    DebugLog($"[DEBUG] RR для первого TP ({firstTpCandidate.Value:F5}) слишком высокий ({rr1:F2} > {MaxRR:F2}). Вход отменен.");
                    return (null, rr1);
                }
            }
            else
            {
                DebugLog($"[DEBUG] Ближайший H1 фрактал для TP не найден. Используем альтернативный ключевой уровень.");
                // Если фракталы не найдены, пробуем использовать FindKeyLevelForTP
                // ВАЖНО: Логика FindKeyLevelForTP может не соответствовать стратегии, основанной строго на фракталах.
                // Раскомментируйте и протестируйте, если это приемлемо.
                /*
                double? keyLevelTp = FindKeyLevelForTP(tradeType, entryPrice);
                if (keyLevelTp.HasValue)
                {
                    DebugLog($"[DEBUG] CalculateTakeProfit: Альтернативный TP (ключевой уровень): {keyLevelTp.Value:F5}");
                    double rrKey = Math.Abs(keyLevelTp.Value - entryPrice) / stopLossDistance;
                    DebugLog($"[DEBUG] CalculateTakeProfit: RR для ключевого уровня ({keyLevelTp.Value:F5}) = {rrKey:F2}");
                    if (rrKey >= MinRR && rrKey <= MaxRR)
                    {
                        DebugLog($"[DEBUG] CalculateTakeProfit: Ключевой уровень TP ({keyLevelTp.Value:F5}) подходит по RR ({rrKey:F2}). Используем его.");
                        return (keyLevelTp.Value, rrKey);
                    }
                    else
                    {
                        DebugLog($"[DEBUG] RR для ключевого уровня ({keyLevelTp.Value:F5}) не подходит ({rrKey:F2}). Вход отменен.");
                        return (null, rrKey);
                    }
                }
                else
                {
                    DebugLog("[DEBUG] Ключевой уровень для TP также не найден. Вход отменен.");
                    return (null, 0);
                }
                */
                DebugLog("[DEBUG] H1 фракталы и ключевые уровни не найдены. Вход отменен.");
                return (null, 0); // Если фракталы не найдены, нет TP
            }
        }

        private double CalculateStopLoss(TradeType tradeType, double asianFractalLevelToPlaceSLBehind)
        {
            if (tradeType == TradeType.Buy)
            {
                // For buy orders, place stop loss below the swept fractal with smaller buffer
                return asianFractalLevelToPlaceSLBehind - (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
            else
            {
                // For sell orders, place stop loss above the swept fractal
                return asianFractalLevelToPlaceSLBehind + (0.3 * Symbol.PipSize); // Уменьшаем буфер до 0.3 пипса
            }
        }

        private void EnterPosition(TradeType tradeType, double entryPrice, AsianFractal fractal)
        {
            if (fractal.EntryDone) { DebugLog("[DEBUG] Уже был вход по этому фракталу"); return; }
            if (Positions.Find("H3M") != null) { DebugLog("[DEBUG] Уже есть открытая позиция H3M"); return; }
            
            var lastSweptFractal = fractal;
            if (lastSweptFractal == null) { DebugLog("[DEBUG] Нет свипнутого фрактала для входа"); return; }
            
            DebugLog($"[DEBUG] =====ВХОД В ПОЗИЦИЮ===== Попытка входа в {tradeType} позицию после свипа фрактала {lastSweptFractal.Level:F5} ({lastSweptFractal.Time})");
            DebugLog($"[DEBUG] Азиатский фрактал: {lastSweptFractal.Level:F5}, Экстремум M3 бара свипа: {lastSweptFractal.SweepExtreme.Value:F5}, Цена входа (BOS): {entryPrice:F5}");
            
            // Стоп-лосс размещается за экстремумом M3 бара, который совершил свип азиатского фрактала
            var stopLossPrice = CalculateStopLoss(tradeType, lastSweptFractal.SweepExtreme.Value); 
            
            var (takeProfitPrice, rr) = CalculateTakeProfit(tradeType, entryPrice, stopLossPrice);
            
            // --- Логирование для 14.05.2025 ---
            if (Server.Time.Date == new DateTime(2025, 5, 14) && 
                Math.Abs(fractal.Level - 1.11832) < Symbol.PipSize * 0.5 && 
                Math.Abs(entryPrice - 1.11913) < Symbol.PipSize * 2) // Допуск для цены входа побольше
            {
                DebugLog($"[USER_TARGET_LOG 14.05.2025] EnterPosition для азиатского фрактала ~1.11832, вход ~1.11913:");
                DebugLog($"    > Бот: SL={stopLossPrice:F5}, TP={takeProfitPrice?.ToString("F5") ?? "N/A"}, RR={rr:F2}");
                DebugLog($"    > Ожидание: SL ~1.11891, TP ~1.12360 (предполагая положительное значение).");
                if (takeProfitPrice.HasValue) {
                    if (rr < MinRR) DebugLog($"    > Бот (RR Check): RR ({rr:F2}) < MinRR ({MinRR:F2}). Вход НЕВОЗМОЖЕН.");
                    else if (rr > MaxRR) DebugLog($"    > Бот (RR Check): RR ({rr:F2}) > MaxRR ({MaxRR:F2}). Вход НЕВОЗМОЖЕН.");
                    else DebugLog($"    > Бот (RR Check): RR ({rr:F2}) в диапазоне [{MinRR:F2} - {MaxRR:F2}]. Вход ВОЗМОЖЕН.");
                } else {
                    DebugLog($"    > Бот (RR Check): Тейк-профит не рассчитан. Вход НЕВОЗМОЖЕН.");
                }
            }
            // --- Конец логирования для 14.05.2025 ---

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

            // Specific logging logic for 14.05.2025
            if (time.Date == new DateTime(2025, 5, 14))
            {
                // Always log messages containing specific markers for 14.05.2025 debugging
                if (message.Contains("[DEBUG 14.05.2025]") || message.Contains("[USER_TARGET_LOG 14.05.2025]"))
                {
                    Print(message);
                    return; // Message logged, exit
                }

                // For other messages on this date, restrict to a key event window
                // Window: 08:55 to 09:30 UTC+3 for 14.05.2025
                var startEventWindow = new DateTime(time.Year, time.Month, time.Day, 9, 18, 0); 
                var endEventWindow = new DateTime(time.Year, time.Month, time.Day, 9, 18, 0);   
                if (time >= startEventWindow && time <= endEventWindow)
                {
                    Print(message);
                }
            }
            // On other dates, DebugLog will do nothing by default, reducing log volume.
            // If general logging for other dates is needed in the future,
            // an 'else' block could be added here with different conditions.
        }

        protected override void OnTick()
        {
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
            
            // --- Лог ожидаемого сценария от пользователя для 14.05.2025 ---
            if (Server.Time.Date == new DateTime(2025, 5, 14) && Server.Time.Hour == 8 && Server.Time.Minute == 50) // Выводим один раз в начале дня
            {
                Print("[USER_EXPECTED_SCENARIO_14.05.2025] Ожидаемая сделка:");
                Print("    Фрактал 1 (Азия H1): ~1.11822 (бот использует 1.11822 или близкий)");
                Print("    Свип фрактала 1: в ~09:03 UTC+3 (M3 Low < уровня фрактала)");
                Print("    Слом структуры (BOS): M3 свеча закрывается выше High M3-бара свипа в ~09:15 UTC+3");
                Print("    Вход Long: ~1.11913");
                Print("    Stop Loss: ~1.11806");
                Print("    Take Profit: ~1.12360");
            }
            // --- Конец лога ожидаемого сценария ---

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
                DebugLog($"[DEBUG 14.05.2025] FindAsianSessionFractals: After sorting for {trendContext}. Found {_asianFractals.Count} fractals. CurrentPrice for diff: {currentPrice:F5}");
                foreach (var f in _asianFractals)
                {
                    DebugLog($"    - Level: {f.Level:F5}, Time: {f.Time}, Diff Pips: {Math.Abs(f.Level - currentPrice) / Symbol.PipSize:F1}");
                }
            }
        }

        private void CheckFractalsSweep(TrendContext trendContext)
        {
            if (_asianFractals.Count == 0)
            {
                //DebugLog("[DEBUG] Нет азиатских фракталов для проверки свипа.");
                return;
            }

            bool anyFractalSweptThisTick = false;

            foreach (var fractal in _asianFractals.Where(f => !f.EntryDone)) // Проверяем только те, по которым не было входа
            {
                // Проверка мгновенного свипа текущей ценой
                if (trendContext == TrendContext.Bullish && Symbol.Bid < fractal.Level)
                {
                    fractal.IsSwept = true;
                    fractal.SweepLevel = Symbol.Bid;
                    fractal.SweepExtreme = Symbol.Bid; 
                    fractal.SweepBarIndex = _m3Bars.Count -1; 
                    fractal.LastBosCheckBarIndex = fractal.SweepBarIndex.Value; // Инициализация
                    anyFractalSweptThisTick = true;
                    DebugLog($"[DEBUG] Мгновенный СВИП фрактала {fractal.Level:F5} в {fractal.Time} текущей ценой {Symbol.Bid:F5}. SweepExtreme установлен {fractal.SweepExtreme:F5}");
                }
                else if (trendContext == TrendContext.Bearish && Symbol.Ask > fractal.Level)
                {
                    fractal.IsSwept = true;
                    fractal.SweepLevel = Symbol.Ask;
                    fractal.SweepExtreme = Symbol.Ask; 
                    fractal.SweepBarIndex = _m3Bars.Count -1; 
                    fractal.LastBosCheckBarIndex = fractal.SweepBarIndex.Value; // Инициализация
                    anyFractalSweptThisTick = true;
                    DebugLog($"[DEBUG] Мгновенный СВИП фрактала {fractal.Level:F5} в {fractal.Time} текущей ценой {Symbol.Ask:F5}. SweepExtreme установлен {fractal.SweepExtreme:F5}");
                }

                // Если уже свипнут (мгновенно или на предыдущем тике), но еще не было входа
                if (fractal.IsSwept && !fractal.EntryDone)
                {
                    // Проверяем BOS
                    var bosResult = Is3mStructureBreak(fractal, trendContext);
                    if (bosResult.IsBreak)
                    {
                        DebugLog($"[DEBUG] СЛОМ СТРУКТУРЫ подтвержден для фрактала {fractal.Level:F5} (свипнут на {fractal.SweepExtreme:F5}). Время слома: {bosResult.BreakTime}, Цена входа: {bosResult.EntryPrice:F5}");
                        EnterPosition(trendContext == TrendContext.Bullish ? TradeType.Buy : TradeType.Sell, bosResult.EntryPrice, fractal);
                        // fractal.EntryDone = true; // Помечается внутри EnterPosition
                        if (_lastTradeDate == Server.Time.Date) // Если сделка была сегодня
                        {
                             DebugLog($"[DEBUG] Сделка по фракталу {fractal.Level:F5} совершена. Поиск других сделок сегодня прекращен.");
                             return; // Выходим из CheckFractalsSweep и OnTick, если сделка была сегодня
                        }
                    }
                    else
                    {
                        //DebugLog($"[DEBUG] Для свипнутого фрактала {fractal.Level:F5} (экстремум {fractal.SweepExtreme:F5}) слом структуры НЕ найден.");
                    }
                    anyFractalSweptThisTick = true; // Фрактал уже был свипнут, продолжаем его "вести"
                    continue; // Переходим к следующему фракталу в _asianFractals
                }

                // Исторический свип (если не было мгновенного на этом тике и фрактал еще не был помечен как свипнутый ранее)
                if (!fractal.IsSwept) // Эта проверка важна, чтобы не перезаписывать SweepExtreme от мгновенного свипа
                {
                    for (int i = 0; i < Math.Min(300, _m3Bars.Count); i++) // Проверяем последние N M3 баров
                    {
                        var barTime = _m3Bars.OpenTimes.Last(i);
                        var m3Low = _m3Bars.LowPrices.Last(i);
                        var m3High = _m3Bars.HighPrices.Last(i);
                        int barM3Index = _m3Bars.Count - 1 - i;

                        bool isBarInAllowedSession = IsLondonOrFrankfurtSession(barTime);
                        bool isBarOnFractalDate = barTime.Date == fractal.Time.Date; // Свип должен быть в день формирования фрактала

                        // --- Логирование для отладки сессий и дат при историческом свипе ---
                        if (Server.Time.Date == new DateTime(2025, 5, 14) && Server.Time.Hour == 9 && Server.Time.Minute >= 0 && Server.Time.Minute <= 5 && fractal.Time.Date == new DateTime(2025,5,14))
                        {
                            //DebugLog($"[HIST_SWEEP_SESS_CHECK 14.05 09:00-09:05] M3BarTime: {barTime}, InAllowedSess: {isBarInAllowedSession}, BarOnFractalDate: {isBarOnFractalDate} (FractalDate: {fractal.Time.Date})");
                        }
                        // --- Конец логирования для отладки ---

                        if (!(isBarInAllowedSession && isBarOnFractalDate)) // Пропускаем бары не в сессии или не в день фрактала
                        {
                            //if (i < 5 && Server.Time.Date == new DateTime(2025,5,14)) DebugLog($"[DEBUG] Ист. свип: Бар {barTime} пропущен. Sess: {isBarInAllowedSession}, DateOk: {isBarOnFractalDate}");
                            continue;
                        }
                        
                        // Логирование M3 баров для свипа фрактала 1.11822 в районе 09:00-09:05 14.05.2025
                        bool shouldLogDetail = Server.Time.Date == new DateTime(2025, 5, 14) &&
                                            fractal.Level == 1.11822 && // Конкретный фрактал
                                            Server.Time.Hour == 9 && (Server.Time.Minute >= 0 && Server.Time.Minute <= 10); // Время сервера (тика)

                        if (shouldLogDetail && i < 5) // Логируем только 5 последних M3 баров для краткости
                        {
                            Print($"[DETAIL_SWEEP_LOG {barTime:dd.MM.yyyy HH:mm:ss}] M3 Bar [{barM3Index}] O:{_m3Bars.OpenPrices.Last(i):F5} H:{m3High:F5} L:{m3Low:F5} C:{_m3Bars.ClosePrices.Last(i):F5}. Fractal Level: {fractal.Level:F5}. InFrankfurt: {IsInFrankfurtSession(barTime)}, InLondon: {IsInLondonSession(barTime)}");
                        }

                        if (trendContext == TrendContext.Bullish && m3Low < fractal.Level)
                        {
                            if (shouldLogDetail) Print($"[SWEEP_CHECK_VALUES ServerT:{Server.Time:HH:mm:ss} M3BarOpenT:{barTime:HH:mm:ss}] m3Low={m3Low:F5}, fractalLevel={fractal.Level:F5}, ConditionMet?=True");
                            
                            fractal.IsSwept = true;
                            fractal.SweepLevel = m3Low;
                            fractal.SweepExtreme = m3Low; 
                            fractal.SweepBarIndex = barM3Index;
                            fractal.LastBosCheckBarIndex = fractal.SweepBarIndex.Value; // Инициализация
                            anyFractalSweptThisTick = true;
                            DebugLog($"[DEBUG] Исторический СВИП фрактала {fractal.Level:F5} (время {fractal.Time}) M3 баром {barTime} (Low={m3Low:F5}). SweepExtreme установлен {fractal.SweepExtreme:F5}");
                            break; 
                        }
                        else if (trendContext == TrendContext.Bearish && m3High > fractal.Level)
                        {
                            if (shouldLogDetail) Print($"[SWEEP_CHECK_VALUES ServerT:{Server.Time:HH:mm:ss} M3BarOpenT:{barTime:HH:mm:ss}] m3High={m3High:F5}, fractalLevel={fractal.Level:F5}, ConditionMet?=True");

                            fractal.IsSwept = true;
                            fractal.SweepLevel = m3High;
                            fractal.SweepExtreme = m3High; 
                            fractal.SweepBarIndex = barM3Index;
                            fractal.LastBosCheckBarIndex = fractal.SweepBarIndex.Value; // Инициализация
                            anyFractalSweptThisTick = true;
                            DebugLog($"[DEBUG] Исторический СВИП фрактала {fractal.Level:F5} (время {fractal.Time}) M3 баром {barTime} (High={m3High:F5}). SweepExtreme установлен {fractal.SweepExtreme:F5}");
                            break; 
                        }
                        else
                        {
                             if (shouldLogDetail && i < 2) Print($"[SWEEP_CHECK_VALUES ServerT:{Server.Time:HH:mm:ss} M3BarOpenT:{barTime:HH:mm:ss}] m3LowOrHigh Relevant, fractalLevel={fractal.Level:F5}, ConditionMet?=False");
                        }
                    }
                }
                
                // Если после всех проверок (мгновенный И исторический) для текущего фрактала IsSwept все еще true,
                // и по нему не было входа, проверяем BOS.
                // Это дублирует проверку BOS выше, но нужно если исторический свип только что произошел.
                if (fractal.IsSwept && !fractal.EntryDone)
                {
                    var bosResult = Is3mStructureBreak(fractal, trendContext);
                    if (bosResult.IsBreak)
                    {
                        DebugLog($"[DEBUG] СЛОМ СТРУКТУРЫ (после ист.свипа) подтвержден для фрактала {fractal.Level:F5} (свипнут на {fractal.SweepExtreme:F5}). Время слома: {bosResult.BreakTime}, Цена входа: {bosResult.EntryPrice:F5}");
                        EnterPosition(trendContext == TrendContext.Bullish ? TradeType.Buy : TradeType.Sell, bosResult.EntryPrice, fractal);
                        // fractal.EntryDone = true; // Помечается внутри EnterPosition
                         if (_lastTradeDate == Server.Time.Date)
                        {
                             DebugLog($"[DEBUG] Сделка по фракталу {fractal.Level:F5} совершена. Поиск других сделок сегодня прекращен.");
                             return; // Выходим из CheckFractalsSweep и OnTick
                        }
                    }
                    else
                    {
                        //DebugLog($"[DEBUG] Для свипнутого фрактала {fractal.Level:F5} (экстремум {fractal.SweepExtreme:F5}) слом структуры НЕ найден (после ист. свипа).");
                    }
                    anyFractalSweptThisTick = true; // Подтверждаем, что есть активный свипнутый фрактал
                }
            } // конец foreach (var fractal in _asianFractals.Where(f => !f.EntryDone))

            if (!anyFractalSweptThisTick)
            {
                //DebugLog("[DEBUG] Нет свипнутых фракталов, ожидание свипа");
            }
            //else
            //{
            //    DebugLog("[DEBUG] Статус фракталов после проверки свипа:");
            //    foreach (var f in _asianFractals.Where(fr => fr.IsSwept && !fr.EntryDone).Take(MaxTrackedFractals)) // Показываем только активные для отслеживания
            //    {
            //        DebugLog($"[DEBUG] Фрактал: {f.Level:F5}, время: {f.Time}, свипнут: {f.IsSwept}, свип-уровень: {f.SweepLevel:F5}, экстремум: {f.SweepExtreme:F5}, sweepBarIndex: {f.SweepBarIndex}");
            //    }
            //}
        }

        private class StructureBreakResult
        {
            public bool IsBreak { get; set; }
            public double EntryPrice { get; set; }
            public DateTime BreakTime { get; set; } // Добавим время слома
        }

        private StructureBreakResult Is3mStructureBreak(AsianFractal fractal, TrendContext trendContext)
        {
            if (fractal == null || !fractal.IsSwept || !fractal.SweepLevel.HasValue || !fractal.SweepExtreme.HasValue || !fractal.SweepBarIndex.HasValue) 
            { 
                DebugLog("[DEBUG] Is3mStructureBreak: Некорректные данные по фракталу для проверки слома."); 
                return new StructureBreakResult { IsBreak = false }; 
            }

            var m3 = _m3Bars;
            // Ensure SweepBarIndex is valid before accessing m3.HighPrices or m3.LowPrices
            if (fractal.SweepBarIndex.Value < 0 || fractal.SweepBarIndex.Value >= m3.Count)
            {
                DebugLog($"[DEBUG] Is3mStructureBreak: Некорректный SweepBarIndex ({fractal.SweepBarIndex.Value}) для фрактала {fractal.Level:F5}.");
                return new StructureBreakResult { IsBreak = false };
            }

            // Определяем начальный бар для этого анализа.
            // Мы хотим начать проверку с бара, следующего за последним проверенным,
            // или с бара, следующего за баром свипа, если это первая проверка BOS после свипа.
            int startBarToAnalyze = fractal.LastBosCheckBarIndex + 1;
            // Убедимся, что не начинаем раньше или на самом баре свипа. Минимум это SweepBarIndex + 1.
            if (startBarToAnalyze <= fractal.SweepBarIndex.Value)
            {
                startBarToAnalyze = fractal.SweepBarIndex.Value + 1;
            }

            if (m3.Count <= startBarToAnalyze) // Нет новых баров для проверки с последнего раза или после свипа
            {
                return new StructureBreakResult { IsBreak = false };
            }
            
            // DebugLog($"[DEBUG] Is3mStructureBreak: Проверка слома для фрактала {fractal.Level:F5} ({fractal.Time}), свип экстремум SL: {fractal.SweepExtreme.Value:F5}, тренд: {trendContext}");
            // sweepExtremeLevel (который fractal.SweepExtreme.Value) используется для SL.
            // Для BOS мы используем High/Low бара свипа.

            // Итерируем M3 бары НАЧИНАЯ СО СЛЕДУЮЩЕГО после бара, который сделал свип
            for (int barIndexOffset = startBarToAnalyze; barIndexOffset < m3.Count; barIndexOffset++)
            {
                var barTime = m3.OpenTimes[barIndexOffset];
                var close = m3.ClosePrices[barIndexOffset];
                var high = m3.HighPrices[barIndexOffset];
                var low = m3.LowPrices[barIndexOffset];
                // var sweepExtremeLevel = fractal.SweepExtreme.Value; // Это для SL

                // Обновляем LastBosCheckBarIndex на текущий обрабатываемый бар *перед* любыми continue или return.
                // Это гарантирует, что этот бар не будет повторно обработан для этого фрактала в последующих вызовах.
                fractal.LastBosCheckBarIndex = barIndexOffset;

                // Проверяем, что бар находится в нужной сессии (Франкфурт или Лондон)
                if (!(IsInFrankfurtSession(barTime) || IsInLondonSession(barTime)))
                {
                    // DebugLog($"[DEBUG] Is3mStructureBreak: Бар {barIndexOffset} ({barTime:HH:mm}) вне сессии Франкфурта/Лондона. Пропуск.");
                    continue; 
                }
                
                bool currentBarConfirmsBOS = false;
                double levelToBreak = 0; // Инициализируем, чтобы компилятор не ругался

                if (trendContext == TrendContext.Bullish)
                {
                    levelToBreak = m3.HighPrices[fractal.SweepBarIndex.Value]; // High бара свипа
                    DebugLog($"[DEBUG] Is3mStructureBreak (Bullish): Анализ M3 бара [{barIndexOffset}] Close:{close:F5} Time:{barTime:HH:mm} vs SweepBarHigh:{levelToBreak:F5} (Fractal: {fractal.Level:F5}, SweepExtremeForSL: {fractal.SweepExtreme.Value:F5})");
                    if (close > levelToBreak)
                    {
                        currentBarConfirmsBOS = true;
                    }
                }
                else if (trendContext == TrendContext.Bearish)
                {
                    levelToBreak = m3.LowPrices[fractal.SweepBarIndex.Value]; // Low бара свипа
                    DebugLog($"[DEBUG] Is3mStructureBreak (Bearish): Анализ M3 бара [{barIndexOffset}] Close:{close:F5} Time:{barTime:HH:mm} vs SweepBarLow:{levelToBreak:F5} (Fractal: {fractal.Level:F5}, SweepExtremeForSL: {fractal.SweepExtreme.Value:F5})");
                    if (close < levelToBreak)
                    {
                        currentBarConfirmsBOS = true;
                    }
                }

                if (currentBarConfirmsBOS)
                {
                    double bosDistancePips = 0;
                    if (trendContext == TrendContext.Bullish) {
                        bosDistancePips = (close - levelToBreak) / Symbol.PipSize;
                    } else { // Bearish
                        bosDistancePips = (levelToBreak - close) / Symbol.PipSize;
                    }

                    if (bosDistancePips <= MaxBOSDistancePips)
                    {
                        if (trendContext == TrendContext.Bullish)
                        {
                            DebugLog($"[DEBUG] Is3mStructureBreak: СЛОМ СТРУКТУРЫ ВВЕРХ (Bullish) ПОДТВЕРЖДЕН И В ДОПУСКЕ. M3 бар [{barIndexOffset}] закрылся ({close:F5}) > High бара свипа ({levelToBreak:F5}) в {barTime:HH:mm}. Дистанция: {bosDistancePips:F1} пипсов (Макс: {MaxBOSDistancePips:F1}).");
                        }
                        else // Bearish
                        {
                            DebugLog($"[DEBUG] Is3mStructureBreak: СЛОМ СТРУКТУРЫ ВНИЗ (Bearish) ПОДТВЕРЖДЕН И В ДОПУСКЕ. M3 бар [{barIndexOffset}] закрылся ({close:F5}) < Low бара свипа ({levelToBreak:F5}) в {barTime:HH:mm}. Дистанция: {bosDistancePips:F1} пипсов (Макс: {MaxBOSDistancePips:F1}).");
                        }
                        
                        // --- Логирование для 14.05.2025 ---
                        if (Server.Time.Date == new DateTime(2025, 5, 14) && Math.Abs(fractal.Level - 1.11832) < Symbol.PipSize * 0.5)
                        {
                            DebugLog($"[USER_TARGET_LOG 14.05.2025] Is3mStructureBreak (тренд {trendContext}) для азиатского фрактала ~1.11832:");
                            DebugLog($"    > Бот: СЛОМ СТРУКТУРЫ! Время: {barTime:HH:mm:ss}, Цена закрытия (вход): {close:F5}, Дистанция от уровня BOS: {bosDistancePips:F1} пипсов.");
                            DebugLog($"    > Ожидание: Слом в ~09:18 UTC+3, Цена входа ~1.11913.");
                        }
                        // --- Конец логирования для 14.05.2025 ---
                        return new StructureBreakResult { IsBreak = true, EntryPrice = close, BreakTime = barTime };
                    }
                    else
                    {
                        DebugLog($"[DEBUG] Is3mStructureBreak: Слом структуры на M3 баре [{barIndexOffset}] ({barTime:HH:mm}) ПРОПУЩЕН. Закрытие {close:F5} слишком далеко ({bosDistancePips:F1} пипсов) от уровня слома {levelToBreak:F5}. Макс. допустимо: {MaxBOSDistancePips:F1} пипсов. Ищем дальше...");
                        // Не возвращаем, цикл продолжится для следующего бара barIndexOffset
                    }
                }
            }
            
            // Логируем это сообщение "не найдено", только если мы действительно проитерировали какие-то новые бары в этом вызове.
            if (startBarToAnalyze < m3.Count) // подразумевает, что цикл выполнялся или хотя бы пытался выполниться
            {
                 DebugLog($"[DEBUG] Is3mStructureBreak: Слом структуры не обнаружен (или все допустимые были слишком далеко) в новых барах [{startBarToAnalyze} до {m3.Count -1}] для фрактала {fractal.Level:F5}.");
            }
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