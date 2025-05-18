import pandas as pd
import mplfinance as mpf
import argparse
import matplotlib.pyplot as plt

def plot_custom_chart(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Ошибка: CSV файл не найден по пути: {csv_file_path}")
        return
    except Exception as e:
        print(f"Ошибка при чтении CSV файла: {e}")
        return

    # Преобразование Timestamp в datetime объекты и установка как индекса
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index(pd.DatetimeIndex(df['Timestamp']))

    # Данные для основного графика H1 свечей
    h1_bars_df = df[df['EventType'] == 'H1_BAR'].copy() # Используем .copy() для избежания SettingWithCopyWarning
    if h1_bars_df.empty:
        print("Нет данных H1_BAR для отображения.")
        return
        
    h1_bars_df.rename(columns={
        'H1_Open': 'Open',
        'H1_High': 'High',
        'H1_Low': 'Low',
        'H1_Close': 'Close'
    }, inplace=True)
    
    # Убедимся, что колонки имеют числовой тип
    ohlc_cols = ['Open', 'High', 'Low', 'Close']
    for col in ohlc_cols:
        h1_bars_df[col] = pd.to_numeric(h1_bars_df[col], errors='coerce')
    
    h1_bars_df = h1_bars_df[ohlc_cols].dropna()


    # Подготовка дополнительных графических элементов (addplot)
    ap_elements = []
    additional_plots_data = {} # Данные для addplot

    # Маркеры для событий
    event_markers = {
        'ASIAN_FRACTAL_Bullish': [],
        'ASIAN_FRACTAL_Bearish': [],
        'SWEEP_BullishSweep': [],
        'SWEEP_BearishSweep': [],
        'BOS_CONFIRMED_Buy': [],
        'BOS_CONFIRMED_Sell': [],
        'TRADE_ENTRY_Buy_Entry': [],
        'TRADE_ENTRY_Sell_Entry': [],
        'TRADE_ENTRY_Buy_SL': [],
        'TRADE_ENTRY_Sell_SL': [],
        'TRADE_ENTRY_Buy_TP': [],
        'TRADE_ENTRY_Sell_TP': []
    }
    
    # Стили для Asian Fractals (горизонтальные линии)
    fractal_lines_bullish = []
    fractal_lines_bearish = []

    for index, row in df.iterrows():
        event_time = row['Timestamp']
        event_type = row['EventType']
        price1 = row['Price1']
        price2 = row['Price2']
        trade_type_event = row['TradeType'] # 'Bullish', 'Bearish', 'Buy', 'Sell'

        if pd.notna(price1):
            if event_type == 'ASIAN_FRACTAL':
                color = 'blue' if trade_type_event == 'Bullish' else 'red'
                # Для горизонтальной линии фрактала нам нужна начальная и конечная точка по времени
                # Попробуем сделать ее видимой на протяжении нескольких H1 баров или фиксированной длины
                line_start_time = event_time
                line_end_time = event_time + pd.Timedelta(hours=3) # Длина линии, например, 3 часа
                
                # Создаем DataFrame для линии фрактала
                fractal_df = pd.DataFrame(
                    data=[price1, price1], 
                    index=[line_start_time, line_end_time], 
                    columns=['value']
                )
                if trade_type_event == 'Bullish':
                    fractal_lines_bullish.append(fractal_df)
                else: # Bearish
                    fractal_lines_bearish.append(fractal_df)

            elif event_type == 'SWEEP':
                marker_color = 'orange'
                marker_style = 'o'
                key = f"{event_type}_{trade_type_event}"
                if key in event_markers:
                     event_markers[key].append(price1)
                else: # Если нет разделения по trade_type_event для SWEEP
                     event_markers.setdefault('SWEEP_Generic', []).append(price1)
                     
            elif event_type == 'BOS_CONFIRMED':
                marker_color = 'purple'
                marker_style = '^' if trade_type_event == 'Buy' else 'v'
                key = f"{event_type}_{trade_type_event}"
                event_markers[key].append(price1)
                
            elif event_type == 'TRADE_ENTRY':
                entry_price = price1
                sl_price = price2 # SL сохранен в Price2
                # TP нужно будет извлечь из Notes, если он там есть и нужен для отображения
                # notes: $"TP: {tpResult.takeProfitPrice?.ToString(CultureInfo.InvariantCulture) ?? "N/A"}, RR: {tpResult.rr.ToString("F2", CultureInfo.InvariantCulture)}, Label: {label}"
                
                tp_price = None
                try:
                    notes_parts = row['Notes'].split(';')
                    for part in notes_parts:
                        if 'TP:' in part:
                            tp_str = part.split(':')[1].strip()
                            if tp_str != 'N/A':
                                tp_price = float(tp_str)
                            break
                except:
                    pass # Ошибка парсинга TP, оставляем None

                if trade_type_event == 'Buy':
                    event_markers['TRADE_ENTRY_Buy_Entry'].append(entry_price)
                    if pd.notna(sl_price): event_markers['TRADE_ENTRY_Buy_SL'].append(sl_price)
                    if pd.notna(tp_price): event_markers['TRADE_ENTRY_Buy_TP'].append(tp_price)
                elif trade_type_event == 'Sell':
                    event_markers['TRADE_ENTRY_Sell_Entry'].append(entry_price)
                    if pd.notna(sl_price): event_markers['TRADE_ENTRY_Sell_SL'].append(sl_price)
                    if pd.notna(tp_price): event_markers['TRADE_ENTRY_Sell_TP'].append(tp_price)
        
    # Подготовка данных для mpf.make_addplot
    # Азиатские фракталы (линии)
    if fractal_lines_bullish:
        # Объединяем все бычьи фрактальные линии в один DataFrame, если их несколько
        # Для mplfinance лучше передавать один DataFrame на addplot для линий
        # или строить их отдельно через matplotlib Axes.
        # Пока что, mplfinance не очень хорошо рисует много отдельных горизонтальных линий через addplot.
        # Альтернатива - рисовать их как scatter точки, но это не линии.
        # Попробуем собрать их в один series с NaN, где линии прерываются. Это сложно.
        # Простой вариант: нарисуем их как scatter маркеры на уровне фрактала в момент его появления.
        for f_df in fractal_lines_bullish:
             ap_elements.append(mpf.make_addplot(f_df['value'], type='line', color='blue', width=0.7, alpha=0.7))
    if fractal_lines_bearish:
        for f_df in fractal_lines_bearish:
             ap_elements.append(mpf.make_addplot(f_df['value'], type='line', color='red', width=0.7, alpha=0.7))


    # События как scatter маркеры
    # Убедимся, что все списки в event_markers имеют одинаковую длину с h1_bars_df.index
    # или создадим Series с NaN, где нет событий.
    
    plot_styles = {
        'ASIAN_FRACTAL_Bullish': {'type': 'scatter', 'color': 'blue', 'marker': '_', 'markersize': 100}, # Отобразим как маркеры пока
        'ASIAN_FRACTAL_Bearish': {'type': 'scatter', 'color': 'red', 'marker': '_', 'markersize': 100}, # Отобразим как маркеры пока
        'SWEEP_BullishSweep': {'type':'scatter', 'color':'yellow', 'marker':'o', 'markersize':50},
        'SWEEP_BearishSweep': {'type':'scatter', 'color':'gold', 'marker':'o', 'markersize':50},
        'SWEEP_Generic': {'type':'scatter', 'color':'orange', 'marker':'o', 'markersize':50},
        'BOS_CONFIRMED_Buy': {'type':'scatter', 'color':'lime', 'marker':'^', 'markersize':100},
        'BOS_CONFIRMED_Sell': {'type':'scatter', 'color':'magenta', 'marker':'v', 'markersize':100},
        'TRADE_ENTRY_Buy_Entry': {'type':'scatter', 'color':'green', 'marker':'^', 'markersize':200},
        'TRADE_ENTRY_Sell_Entry': {'type':'scatter', 'color':'maroon', 'marker':'v', 'markersize':200},
        'TRADE_ENTRY_Buy_SL': {'type':'scatter', 'color':'gray', 'marker':'_', 'markersize':150},
        'TRADE_ENTRY_Sell_SL': {'type':'scatter', 'color':'gray', 'marker':'_', 'markersize':150},
        'TRADE_ENTRY_Buy_TP': {'type':'scatter', 'color':'darkcyan', 'marker':'_', 'markersize':150},
        'TRADE_ENTRY_Sell_TP': {'type':'scatter', 'color':'darkcyan', 'marker':'_', 'markersize':150},
    }

    for key, prices_list in event_markers.items():
        if prices_list: # Если есть события этого типа
            style = plot_styles.get(key)
            if style:
                # Создаем Series для addplot, совмещенный с индексом h1_bars_df
                event_series = pd.Series(index=h1_bars_df.index, dtype=float)
                # Находим ближайшие H1 бары для времен событий и ставим маркеры там
                # Это упрощение, т.к. события могут быть между H1 барами.
                for price_val in prices_list: # предполагается, что prices_list это список цен, а не (время, цена)
                                            # это неверно, нужно время события
                    # Это место нужно переделать, так как event_markers хранит только цены.
                    # Нам нужен исходный DataFrame df, чтобы сопоставить цены с временами событий.
                    # Пройдемся по df еще раз для сбора данных для scatter
                    pass # Заглушка, нужно переделать логику сбора данных для scatter

    # Переделка сбора данных для scatter
    scatter_data = {}
    for key in plot_styles.keys():
        scatter_data[key] = pd.Series(index=h1_bars_df.index, dtype=float)

    for index, row in df.iterrows():
        event_time = row['Timestamp']
        event_type = row['EventType']
        price1 = row['Price1']
        price2 = row['Price2']
        trade_type_event = row['TradeType']

        # Найдем ближайший индекс в h1_bars_df для event_time
        # Это нужно, чтобы маркеры отображались на графике H1
        # Если h1_bars_df пустой, то h1_bars_df.index.get_indexer вернет ошибку или пустой массив
        if h1_bars_df.empty:
            continue
        
        # Ищем ближайший существующий H1 бар по времени, не позднее времени события
        target_h1_idx = h1_bars_df.index.get_indexer([event_time], method='ffill')[0]
        # Если событие произошло до первого H1 бара, get_indexer вернет -1
        if target_h1_idx == -1: 
            # Попробуем взять следующий бар, если есть (method='bfill')
            target_h1_idx = h1_bars_df.index.get_indexer([event_time], method='bfill')[0]
            if target_h1_idx == -1: # Если все еще не нашли, пропускаем
                 continue
        
        plot_idx_time = h1_bars_df.index[target_h1_idx]


        if pd.notna(price1):
            if event_type == 'ASIAN_FRACTAL':
                key = f"{event_type}_{trade_type_event}"
                if key in scatter_data: scatter_data[key].loc[plot_idx_time] = price1
            elif event_type == 'SWEEP':
                key_specific = f"{event_type}_{trade_type_event}"
                key_generic = 'SWEEP_Generic'
                if key_specific in scatter_data:
                    scatter_data[key_specific].loc[plot_idx_time] = price1
                elif key_generic in scatter_data : # Fallback
                     scatter_data[key_generic].loc[plot_idx_time] = price1
            elif event_type == 'BOS_CONFIRMED':
                key = f"{event_type}_{trade_type_event}"
                if key in scatter_data: scatter_data[key].loc[plot_idx_time] = price1
            elif event_type == 'TRADE_ENTRY':
                entry_price = price1
                sl_price = price2
                tp_price = None
                try:
                    notes_parts = row['Notes'].split(';')
                    for part in notes_parts:
                        if 'TP:' in part:
                            tp_str = part.split(':')[1].strip()
                            if tp_str != 'N/A': tp_price = float(tp_str)
                            break
                except: pass

                if trade_type_event == 'Buy':
                    if 'TRADE_ENTRY_Buy_Entry' in scatter_data: scatter_data['TRADE_ENTRY_Buy_Entry'].loc[plot_idx_time] = entry_price
                    if pd.notna(sl_price) and 'TRADE_ENTRY_Buy_SL' in scatter_data: scatter_data['TRADE_ENTRY_Buy_SL'].loc[plot_idx_time] = sl_price
                    if pd.notna(tp_price) and 'TRADE_ENTRY_Buy_TP' in scatter_data: scatter_data['TRADE_ENTRY_Buy_TP'].loc[plot_idx_time] = tp_price
                elif trade_type_event == 'Sell':
                    if 'TRADE_ENTRY_Sell_Entry' in scatter_data: scatter_data['TRADE_ENTRY_Sell_Entry'].loc[plot_idx_time] = entry_price
                    if pd.notna(sl_price) and 'TRADE_ENTRY_Sell_SL' in scatter_data: scatter_data['TRADE_ENTRY_Sell_SL'].loc[plot_idx_time] = sl_price
                    if pd.notna(tp_price) and 'TRADE_ENTRY_Sell_TP' in scatter_data: scatter_data['TRADE_ENTRY_Sell_TP'].loc[plot_idx_time] = tp_price
    
    for key, series_to_plot in scatter_data.items():
        if not series_to_plot.dropna().empty: # Если есть что рисовать
            style = plot_styles.get(key)
            if style:
                 ap_elements.append(mpf.make_addplot(series_to_plot, **style))


    # Построение графика
    try:
        mpf.plot(h1_bars_df, 
                 type='candle', 
                 style='yahoo', 
                 title=f'H3M Custom Chart: {csv_file_path.split("_")[-2]}', # Используем символ из имени файла
                 ylabel='Price',
                 addplot=ap_elements if ap_elements else None, # Передаем None если список пуст
                 volume=False, # Пока без объема
                 figratio=(16,9),
                 panel_ratios=(1, 0.3) if any(ap.get('panel',0) != 0 for ap in ap_elements if isinstance(ap,dict)) else (1,), # для разделения панелей, если нужно
                 figsize=(15, 7)) # Размер графика
    except Exception as e:
        print(f"Ошибка при построении графика: {e}")
        print("Возможно, в данных H1 отсутствуют необходимые колонки 'Open', 'High', 'Low', 'Close' или они содержат NaN после конвертации.")
        print("Содержимое h1_bars_df.head():")
        print(h1_bars_df.head())
        print("Содержимое h1_bars_df.info():")
        h1_bars_df.info()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Построение графика H1 свечей с событиями из CSV файла от H3M cBot.')
    parser.add_argument('csv_file', type=str, help='Путь к CSV файлу с данными.')
    
    args = parser.parse_args()
    plot_custom_chart(args.csv_file) 