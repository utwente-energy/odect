# Copyright 2024 Gerwin Hoogsteen

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Running FORECAST"
cd /app/odect
/usr/local/bin/python3.11 get_tsoinfo.py
/usr/local/bin/python3.11 get_historicweather.py
/usr/local/bin/python3.11 get_weatherforecast.py
/usr/local/bin/python3.11 main.py -d
/usr/local/bin/python3.11 forecast_prices.py
/usr/local/bin/python3.11 forecast_co2.py
echo "Finished FORECAST run"

