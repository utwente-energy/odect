echo "Running FORECAST"
python3.10 get_tsoinfo.py
python3.10 get_historicweather.py
python3.10 get_weatherforecast.py
python3.10 main.py -d
python3.10 forecast_prices.py
python3.10 forecast_co2.py
echo "Finished FORECAST run"

