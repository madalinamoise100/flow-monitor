import requests
import json

r = requests.get('http://localhost:1337/monitor/salesperson5/HedgeFund/2019-11-20')
print(r.text)
