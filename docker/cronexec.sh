echo "Running ODECT"
cd /app/odect
/usr/local/bin/python3.11 main.py -d --prune
/usr/local/bin/python3.11 main.py -d
echo "Finished ODECT run"
