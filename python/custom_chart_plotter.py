import pandas as pd
import argparse
import json
import os

def generate_lightweight_chart_html(csv_file_path, output_html_path="lightweight_chart.html"):
    try:
        df = pd.read_csv(csv_file_path, delimiter=';')
    except FileNotFoundError:
        print(f"Ошибка: CSV файл не найден по пути: {csv_file_path}")
        return
    except Exception as e:
        print(f"Ошибка при чтении CSV файла: {e}")
        return

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    h1_bars_df = df[df['EventType'] == 'H1_BAR'].copy()
    if h1_bars_df.empty:
        print("Нет данных H1_BAR для отображения.")
        return
        
    candlestick_data = []
    for _, row in h1_bars_df.iterrows():
        try:
            # Убедимся, что все значения OHLC являются числами
            o = float(row['H1_Open'])
            h = float(row['H1_High'])
            l = float(row['H1_Low'])
            c = float(row['H1_Close'])
            time_val = int(row['Timestamp'].timestamp()) 
            
            candlestick_data.append({
                "time": time_val,
                "open": o,
                "high": h,
                "low": l,
                "close": c
            })
        except ValueError as ve:
            print(f"Предупреждение: Пропуск строки из-за ошибки конвертации данных в число: {row}, ошибка: {ve}")
            continue # Пропускаем эту строку, если данные не корректны
        except Exception as ex:
            print(f"Предупреждение: Пропуск строки из-за неизвестной ошибки: {row}, ошибка: {ex}")
            continue

    if not candlestick_data:
        print("Нет корректных данных H1_BAR для отображения после фильтрации и конвертации.")
        return

    candlestick_data = [dict(t) for t in {tuple(d.items()) for d in candlestick_data}]
    candlestick_data.sort(key=lambda x: x['time'])

    print("Первые 5 элементов candlestick_data для проверки:")
    for i, item in enumerate(candlestick_data[:5]):
        print(f"  {i}: {item}")

    markers_data = []
    price_lines_data = []

    candlestick_json = json.dumps(candlestick_data)
    markers_json = json.dumps(markers_data)
    price_lines_json = json.dumps(price_lines_data)
    
    chart_title = f'H3M Custom Chart for {csv_file_path.split("_")[-2]} on {pd.to_datetime(candlestick_data[0]["time"], unit="s").date() if candlestick_data else "N/A"}'

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <title>{chart_title}</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body, html {{ margin: 0; padding: 0; height: 100%; }}
        #chartContainer {{ width: 100%; height: 100%; }}
    </style>
</head>
<body>
    <div id="chartContainer"></div>
    <script>
        const chartContainer = document.getElementById('chartContainer');
        const chart = LightweightCharts.createChart(chartContainer, {{
            width: chartContainer.clientWidth,
            height: chartContainer.clientHeight,
            layout: {{
                backgroundColor: '#ffffff',
                textColor: 'rgba(33, 56, 77, 1)',
            }},
            grid: {{
                vertLines: {{
                    color: 'rgba(197, 203, 206, 0.5)',
                }},
                horzLines: {{
                    color: 'rgba(197, 203, 206, 0.5)',
                }},
            }},
            crosshair: {{
                mode: LightweightCharts.CrosshairMode.Normal,
            }},
            priceScale: {{
                borderColor: 'rgba(197, 203, 206, 0.8)',
                precision: 5,
                minMove: 0.00001
            }},
            timeScale: {{
                borderColor: 'rgba(197, 203, 206, 0.8)',
                timeVisible: true,
                secondsVisible: false,
                // Попробуем более явное форматирование для шкалы времени
                tickMarkFormatter: (time, tickMarkType, locale) => {{
                    const d = new Date(time * 1000); 
                    // Для Daily/Weekly/Monthly можно показывать только дату
                    // Для интрадей можно показывать HH:mm
                    // LightweightCharts.TickMarkType.Year = 0, Month = 1, Day = 2, Time = 3, TimeWithSeconds = 4
                    if (tickMarkType === LightweightCharts.TickMarkType.Time) {{
                        return d.toLocaleTimeString(locale, {{ hour: '2-digit', minute: '2-digit' }});
                    }}
                    return d.toLocaleDateString(locale, {{ day: 'numeric', month: 'short' }});
                }},
            }},
        }});

        const candleSeries = chart.addCandlestickSeries({{
            upColor: 'rgba(34, 139, 34, 0.8)', 
            downColor: 'rgba(255, 69, 0, 0.8)', 
            borderDownColor: 'rgba(255, 69, 0, 1)',
            borderUpColor: 'rgba(34, 139, 34, 1)',
            wickDownColor: 'rgba(255, 69, 0, 1)',
            wickUpColor: 'rgba(34, 139, 34, 1)',
        }});

        const candlestickData = {candlestick_json};
        console.log("Candlestick data being passed to chart:", candlestickData.slice(0,5)); // Отладка в консоли браузера
        candleSeries.setData(candlestickData);
        
        // const markersData = {markers_json};
        // if (markersData.length > 0) {{
        //     candleSeries.setMarkers(markersData);
        // }}

        // const priceLinesData = {price_lines_json};
        // priceLinesData.forEach(line => {{
        //     candleSeries.createPriceLine({{
        //         price: line.price,
        //         color: line.color || '#000000',
        //         lineWidth: line.lineWidth || 1,
        //         lineStyle: line.lineStyle || LightweightCharts.LineStyle.Solid,
        //         axisLabelVisible: true,
        //         title: line.title || '',
        //     }});
        // }});

        window.addEventListener('resize', () => {{
            chart.applyOptions({{ width: chartContainer.clientWidth, height: chartContainer.clientHeight }});
        }});

        chart.timeScale().fitContent();

    </script>
</body>
</html>
    """

    try:
        with open(output_html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML график сохранен в: {os.path.abspath(output_html_path)}")
    except Exception as e:
        print(f"Ошибка при сохранении HTML файла: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Генерация интерактивного HTML графика H1 свечей с событиями из CSV файла от H3M cBot с использованием Lightweight Charts.')
    parser.add_argument('csv_file', type=str, help='Путь к CSV файлу с данными.')
    parser.add_argument('--output', type=str, default='lightweight_chart.html', help='Имя выходного HTML файла (по умолчанию: lightweight_chart.html)')
    
    args = parser.parse_args()
    generate_lightweight_chart_html(args.csv_file, args.output) 